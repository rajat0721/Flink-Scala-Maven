package Flink.Kafka.Util

import java.util.Properties

import scala.io.Source

object FileUtility extends Serializable {

  def getProperty(property: String): String = {
    var properties: Properties = new Properties()
    val url = getClass.getResource("/application.properties")

    if (url != null) {
      val source = Source.fromURL(url)
      properties.load(source.bufferedReader())
    }
    properties.getProperty(property)
  }

  def getProperty: Properties = {
    var properties: Properties = null
    val url = getClass.getResource("/application.properties")

    if (url != null) {
      val source = Source.fromURL(url)
      properties = new Properties()
      properties.load(source.bufferedReader())
    }
    properties
  }

}

