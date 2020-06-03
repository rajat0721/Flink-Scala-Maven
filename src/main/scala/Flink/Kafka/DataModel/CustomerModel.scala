package Flink.Kafka.DataModel

import play.api.libs.json.JsValue

object CustomerModel {

  def readElement(jsonElement: JsValue): Customer = {
    val id = (jsonElement \ "id").get.toString().toInt
    val name = (jsonElement \ "name").get.toString()
    Customer(id,name)
  }

  override def toString: String = super.toString

  case class Customer(id: Int, name: String){
    override def toString = {
      "id         : " + this.id + "\n" +
        "name            : " + this.name + "\n"
    }

  }
}

