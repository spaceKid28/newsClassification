import sttp.client3._
import sttp.client3.circe._
import io.circe.{Decoder, HCursor}

case class ZeroShotRequest(
  inputs: String,
  parameters: Map[String, Any]
)

case class ZeroShotResponse(
  sequence: String,
  labels: List[String],
  scores: List[Double]
)

implicit val decodeResponse: Decoder[ZeroShotResponse] =
  new Decoder[ZeroShotResponse] {
    final def apply(c: HCursor): Decoder.Result[ZeroShotResponse] =
      for {
        seq <- c.downField("sequence").as[String]
        labels <- c.downField("labels").as[List[String]]
        scores <- c.downField("scores").as[List[Double]]
      } yield ZeroShotResponse(seq, labels, scores)
  }

object ZeroShotExample {
  def main(args: Array[String]): Unit = {

    val backend = HttpURLConnectionBackend()
    val hfToken = sys.env("HF_API_TOKEN")

    val articleText =
      """Experts warn that new AI regulations could slow innovation and harm startups."""

    val labels = List(
      "The writer expresses optimism about artificial intelligence.",
      "The writer expresses pessimism about artificial intelligence."
    )

    val requestJson = ZeroShotRequest(
      inputs = articleText,
      parameters = Map(
        "candidate_labels" -> labels,
        "hypothesis_template" -> "{}"
      )
    )

    val response = basicRequest
      .post(uri"https://api-inference.huggingface.co/models/MoritzLaurer/deberta-v3-small-mnli")
      .header("Authorization", s"Bearer $hfToken")
      .body(requestJson)
      .response(asJson[ZeroShotResponse])
      .send(backend)

    println(response.body)
  }
}
