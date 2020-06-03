package Flink.Kafka.Dev

import java.util.Properties

import Flink.Kafka.DataModel.CustomerModel
import Flink.Kafka.DataModel.CustomerModel.Customer
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import play.api.libs.json.Json

import scala.util.Try

object KafkaJsonConsumer {

  implicit val formats = org.json4s.DefaultFormats
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.0.114:9092")
    properties.setProperty("group.id", "test-grp")
    //env.enableCheckpointing(5000) //5000 milliSec
    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // configure watermark interval
    env.getConfig.setAutoWatermarkInterval(1000L)

    val consumer = new FlinkKafkaConsumer011[String]("customer", new SimpleStringSchema(), properties)
    consumer.setStartFromEarliest()

    val stream1 = env.addSource(consumer).rebalance
    //stream1.print("String")


    val kafkaConsumer = new FlinkKafkaConsumer011[ObjectNode](
      "customer",
      new JSONKeyValueDeserializationSchema(false),
      properties
    )
    kafkaConsumer.setStartFromEarliest()

    //val messageStream:DataStream[ObjectNode]= env.addSource(kafkaConsumer)
    //messageStream.print("json")
    /*messageStream.map(json => println(json.get("id").asText()+""+json.get("name").asText()))
    messageStream.filter(node => {
      if(node.get("id").asText().equals("3"))
        println("yes")
      node.get("id").asText().equals("3")
    })
*/

/*    env.addSource(new FlinkKafkaConsumer("new", new SimpleStringSchema(), properties))
      .flatMap(raw => Try(JsonMethods.parse(raw).toOption)
        .map(_.extract[Customer])*/


 /*   val jsonStream = messageStream.map(new MapFunction[ObjectNode,Object] {
      override def map(value: ObjectNode): AnyRef ={
        value.get("id").asText()
      }
    })*/

    //jsonStream.print()

/*    stream1.map(new MapFunction<ObjectNode, Object>() {
      @Override
      public Object map(ObjectNode value) throws Exception {
        value.get("field").as(...)
      }
    })*/

    val stream2:DataStream[Customer]= stream1.map( str => {
      Try(CustomerModel.readElement(Json.parse(str))).getOrElse(Customer(0,Try(CustomerModel.readElement(Json.parse(str))).toString))
    })

    stream2.print("stream2")

    val stream3 = stream2.filter(json => {
      if(json.id.equals(3)) println("yes")
      json.name.equals("inValid")
    })

    stream3.print("stream3")
    //val stream3 = stream2.map(data => println(data.id.toInt+100+"   "+data.name))

    //stream1.print()
    //stream2.uid()
    //stream1.print("YO OUT")

    env.execute("This is Flink.Kafka+Flink")

  }
}
