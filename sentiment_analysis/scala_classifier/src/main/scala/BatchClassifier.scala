import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.net.{HttpURLConnection, URL}
import java.io.{OutputStreamWriter, BufferedWriter}
import java.nio.charset.StandardCharsets
import scala.io.Source

object BatchClassifier {

  // Serializable function to call ML endpoint
  def classifyText(text: String, mlEndpoint: String): (String, Double) = {
    if (text == null || text.trim.isEmpty) {
      return ("unknown", 0.0)
    }

    try {
      val labels = Seq(
        "The writer expresses optimism about artificial intelligence.",
        "The writer expresses pessimism about artificial intelligence."
      )

      // Build JSON payload
      val labelsJson = labels.map(l => "\"" + escapeJson(l) + "\"").mkString("[", ",", "]")
      val payload = s"""{"text":"${escapeJson(text)}","candidate_labels":$labelsJson}"""

      // Make HTTP request
      val url = new URL(mlEndpoint)
      val conn = url.openConnection().asInstanceOf[HttpURLConnection]
      conn.setRequestMethod("POST")
      conn.setRequestProperty("Content-Type", "application/json; charset=utf-8")
      conn.setDoOutput(true)
      conn.setConnectTimeout(30000)
      conn.setReadTimeout(30000)

      val writer = new BufferedWriter(new OutputStreamWriter(conn.getOutputStream, StandardCharsets.UTF_8))
      writer.write(payload)
      writer.flush()
      writer.close()

      val code = conn.getResponseCode
      if (code >= 200 && code < 300) {
        val responseStream = conn.getInputStream
        val response = Source.fromInputStream(responseStream, "utf-8").mkString

        // Parse response (assuming ujson is available)
        import ujson._
        val json = read(response)
        val scores = json("scores").arr.map(_.num)
        val respLabels = json("labels").arr.map(_.str)

        val (topScore, topIdx) = scores.zipWithIndex.maxBy(_._1)
        val topLabel = respLabels(topIdx)

        // Simplify label to "optimistic" or "pessimistic"
        val sentiment = if (topLabel.contains("optimism")) "optimistic" else "pessimistic"

        (sentiment, topScore)
      } else {
        println(s"ML API returned error code: $code")
        ("error", 0.0)
      }

    } catch {
      case e: Exception =>
        println(s"Error classifying text: ${e.getMessage}")
        ("error", 0.0)
    }
  }

  def escapeJson(s: String): String = {
    s.flatMap {
      case '"'  => "\\\""
      case '\\' => "\\\\"
      case '\b' => "\\b"
      case '\f' => "\\f"
      case '\n' => "\\n"
      case '\r' => "\\r"
      case '\t' => "\\t"
      case c if c < ' ' => f"\\u${c.toInt}%04x"
      case c => c.toString
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: BatchClassifier <input_path> <ml_endpoint>")
      println("Example: BatchClassifier /inputs/hackernews/raw/ http://ml-server:8000/classify")
      sys.exit(1)
    }

    val inputPath = args(0)
    val mlEndpoint = args(1)

    val spark = SparkSession.builder()
      .appName("HackerNews Batch Classifier")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    println(s"Reading data from: $inputPath")

    // Read JSON Lines files
    val rawData = spark.read.json(inputPath)

    // Filter to only stories with text
    val storiesWithText = rawData
      .filter($"type" === "story")
      .filter($"text".isNotNull && length($"text") > 100)
      .select(
        $"id",
        $"by".as("author"),
        $"time".cast("timestamp").as("date_published"),
        $"title",
        $"url",
        $"text",
        $"score",
        $"descendants",
        coalesce($"kids", array()).as("kids")
      )

    println(s"Found ${storiesWithText.count()} stories with text")

    // Define UDF for classification
    val classifyUDF = udf((text: String) => {
      val (sentiment, score) = classifyText(text, mlEndpoint)
      (sentiment, score)
    })

    // Apply classification in batches with caching
    println("Starting classification (this will take a while)...")

    val classified = storiesWithText
      .repartition(20) // Parallelize across cluster
      .withColumn("classification", classifyUDF($"text"))
      .withColumn("sentiment_label", $"classification._1")
      .withColumn("sentiment_score", $"classification._2")
      .drop("classification")

    // Cache to avoid recomputation
    classified.cache()

    val classifiedCount = classified.count()
    println(s"Classified {classifiedCount} articles")

    // Write to Hive
    println("Writing to Hive table...")
    classified.write
      .mode("overwrite")
      .format("orc")
      .saveAsTable("belincoln_hackernews_classified")

    println("Batch classification complete!")
    println(s"Results written to: belincoln_hackernews_classified")

    // Show sentiment distribution
    println("\nSentiment Distribution:")
    classified.groupBy("sentiment_label")
      .agg(
        count("*").as("count"),
        avg("sentiment_score").as("avg_confidence"),
        avg("score").as("avg_hn_score")
      )
      .show()

    spark.stop()
  }
}