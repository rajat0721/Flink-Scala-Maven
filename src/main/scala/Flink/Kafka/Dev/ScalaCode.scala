package Flink.Kafka.Dev

import Flink.Kafka.DataModel.CustomerModel.Customer

import scala.util.parsing.json.JSONObject
//import com.fasterxml.jackson.module.scala.DefaultScalaModule

object ScalaCode {

  def main(args: Array[String]): Unit = {
    //println(FileUtility.getProperty("bootstrap.servers"))
    val column = Column("id", 0, "INT", 11)
    val customer = Customer(12,"hi")
    println(ccToMap(column))
    println(ccToMap(customer))
    val map1 = ccToMap(customer)
    println(JSONObject(map1).toString())

    JSONObject(map1).toString()

    val l=""
    var output = l
    try {
      output = ""
    }catch{
      case x : Exception =>
      {
        println("exception"+l)
        output = l+"|"
      }
    }
    return output
  }

  def ccToMap(cc: AnyRef) =
    (Map[String, Any]() /: cc.getClass.getDeclaredFields) {
      (a, f) =>
        f.setAccessible(true)
        a + (f.getName -> f.get(cc))
    }

  // Usage

  case class Column(name: String,
                    typeCode: Int,
                    typeName: String,
                    size: Int = 0)

}
