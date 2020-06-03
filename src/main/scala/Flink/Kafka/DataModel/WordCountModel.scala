package Flink.Kafka.DataModel

import play.api.libs.json.JsValue


object WordCountModel{
  def readElement(jsonElement: JsValue): WordCount = {
    val word = (jsonElement \ "word").get.toString()
    val count = (jsonElement \ "count").get.toString().toInt
    WordCount(word,count)
  }

  case class WordCount(word: String, count: Int) {
    override def toString = {
      "word         : " + this.word + "\n" +
        "count            : " + this.count + "\n"
    }

  }

}



