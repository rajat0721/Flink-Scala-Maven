package Flink.Kafka.Dev

import Flink.Kafka.DataModel.WordCountModel.WordCount
import Flink.Kafka.DataModel._
import org.apache.flink.api.scala._
//import org.apache.flink.contrib.streaming.state.RocksDBStateBackend


object WordCountWithDataModel {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    //val tEnv = BatchTableEnvironment.create(env)

    // configure watermark interval
    env.getConfig.setAutoWatermarkInterval(1000L)


    val text = env.fromElements("rajat rajat rajat good to see you in action flink")


    val wcDataModel: DataSet[WordCount] = text.flatMap {
      _.toLowerCase.split("\\W+")}
      .map(WordCountModel.WordCount(_,1))
        .groupBy("word")
        .sum("count")
    wcDataModel.print()
  }

}
