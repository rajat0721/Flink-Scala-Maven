package Flink.Kafka.Dev

import java.util.Properties

import Flink.Kafka.DataModel.CustomerModel
import Flink.Kafka.DataModel.CustomerModel.Customer
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import play.api.libs.json.Json

import scala.util.Try
import scala.util.parsing.json.JSONObject

object KafkaJsonConsumer2 {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.0.114:9092")
    properties.setProperty("group.id", "test-grp")
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(5000)
    env.getConfig.setAutoWatermarkInterval(1000L)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

   /* val fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val fsTableEnv = StreamTableEnvironment.create(env,fsSettings)
*/
   // val bsEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val bsTableEnv = StreamTableEnvironment.create(env, bsSettings)

    val consumer = new FlinkKafkaConsumer011[String]("customer", new SimpleStringSchema(), properties)
    consumer.setStartFromEarliest()
    val producer = new FlinkKafkaProducer011[String]("customer-out",new SimpleStringSchema(), properties)

    //Flink.Kafka JSON Consumer
    val stream1:DataStream[Customer] = env.addSource(consumer).rebalance
      .map(str => {
        Try(CustomerModel.readElement(Json.parse(str))).getOrElse(Customer(0, Try(CustomerModel.readElement(Json.parse(str))).toString))
      })

    bsTableEnv.registerDataStream("myTable", stream1)
    val table1: Table = bsTableEnv.fromDataStream(stream1)


    //Filtering
    val stream3 = stream1.filter(json => {
      !json.name.startsWith("Fail")
    })
    //stream3.print("stream3")

    //kafka JSON Producer
    val stream4 = stream3.map(customer => JSONObject(ScalaCode.ccToMap(customer)).toString().replace("\\\"",""))
    stream4.print("output-kafka")

    stream4.addSink(producer)

    bsTableEnv.execute("This is Flink.Kafka+Flink")

  }
}
