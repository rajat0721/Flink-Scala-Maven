package Flink.Kafka.TableApi

import org.apache.flink.streaming.util.serialization.SerializationSchema

class KafkaOutputSchema extends SerializationSchema[(Boolean, (String, Double))] {

  override def serialize(t: (Boolean, (String, Double))): Array[Byte] = {
    val status:Boolean = t._1
    val agntnum:String = t._2._1
    val sumIPIFPS:Double = t._2._2
    val outputStr = "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"status\"},{\"type\":\"double\",\"optional\":false,\"field\":\"sumIPIFPS\"},{\"type\":\"string\",\"optional\":false,\"field\":\"agntnum\"}],\"optional\":false,\"name\":\"com.flink.message\"},\"payload\":{\"status\":\""+status+"\",\"sumIPIFPS\":"+sumIPIFPS+",\"agntnum\":\""+agntnum+"\"}}"
    //print(outputStr)
    outputStr.getBytes()
  }
}