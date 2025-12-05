import ai.djl.Model
import ai.djl.huggingface.translator.HuggingFaceTranslatorFactory
import ai.djl.inference.Predictor
import ai.djl.ndarray.NDManager
import scala.jdk.CollectionConverters._

object Translator {

  private val modelName = "Helsinki-NLP/opus-mt-ar-en"

  private val translator = HuggingFaceTranslatorFactory.builder()
    .optBatchSize(1)
    .optSourceLanguage("ar")
    .optTargetLanguage("en")
    .build()

  private val model = Model.newInstance("translator")
  model.load(modelName, Map("device" -> "cpu").asJava)

  private val predictor = model.newPredictor(translator)

  def translate(text: String): String = {
    predictor.predict(text)
  }
}

object Main extends App {
  val result = Translator.translate("البيتكوين ترتفع بقوة اليوم.")
  println(s"Translated: $result")
}
