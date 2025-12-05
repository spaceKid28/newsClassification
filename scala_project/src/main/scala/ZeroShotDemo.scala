// scala
import java.net.{HttpURLConnection, URL}
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets
import scala.io.Source
import java.io.{OutputStreamWriter, BufferedWriter}
import ujson._

object ZeroShotDemo {
  def main(args: Array[String]): Unit = {

    // Currently reading from local filesystem... I will want to
    // ( 1. make a pull from a Hbase or Hive table, 2. pull column named "URL" ,
    // 2. scrape article text (this is done in hackerNews.py), save article text as "val article"
    val textPath = "src/main/resources/newsArticle.txt"
    val article = new String(Files.readAllBytes(Paths.get(textPath)), StandardCharsets.UTF_8)

    // hopefully this can remain the same
    val labels = Seq(
      "The writer expresses optimism about artificial intelligence.",
      "The writer expresses pessimism about artificial intelligence."
    )

    val labelsJson = labels.map(l => "\"" + escapeJson(l) + "\"").mkString("[", ",", "]")
    // this is JSON object we use to make API call (app.py)
    val payload = s"""{"text":"${escapeJson(article)}","candidate_labels":$labelsJson}"""

    // I will need to change this line on the cluster (http://127...
    // this is the target endpoint for the POST request
    val url = new URL("http://127.0.0.1:8000/classify")
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("POST")
    conn.setRequestProperty("Content-Type", "application/json; charset=utf-8")
    conn.setDoOutput(true)

    val writer = new BufferedWriter(new OutputStreamWriter(conn.getOutputStream, StandardCharsets.UTF_8))
    writer.write(payload)
    writer.flush()
    writer.close()

    // get response for App (which should include the classification)
    val code = conn.getResponseCode
    val responseStream = if (code >= 200 && code < 300) conn.getInputStream else conn.getErrorStream
    val response = Source.fromInputStream(responseStream, "utf-8").mkString
    println(s"HTTP $code")
    println(response)
    // parse response using ujson
    val json = read(response)

    // get scores (Seq[Double])
    val scores = json("scores").arr.map(_.num)

    // rename response labels to avoid shadowing the earlier `labels`
    val respLabels = json("labels").arr.map(_.str)

    println(s"Scores: ${scores.mkString(", ")}")

    // find top prediction (highest score)
    val (topScore, topIdx) = scores.zipWithIndex.maxBy(_._1)
    val topLabel = respLabels(topIdx)
    println(s"Top prediction: $topLabel (score = $topScore)")
  }

  private def escapeJson(s: String): String = {
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
}
