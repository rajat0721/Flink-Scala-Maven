/*
package lumiq

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.KafkaSink

object KafkaProducer {

  def main(args: Array[String]){
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // parse user parameters
    val parameterTool = ParameterTool.fromArgs(args)

    // add a simple source which is writing some strings
    val messageStream = env.addSource(new WriteIntoKafka.SimpleStringGenerator)

    // write stream to Flink.Kafka
    messageStream.addSink(new KafkaSink[String](parameterTool.getRequired("bootstrap.servers"), parameterTool.getRequired("topic"), new WriteIntoKafka.SimpleStringSchema))
  }

}
*/
