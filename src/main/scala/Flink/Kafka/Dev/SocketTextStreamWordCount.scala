package Flink.Kafka.Dev

import org.apache.flink.streaming.api.scala._

object SocketTextStreamWordCount {

  def main(args: Array[String]) {
    /*if (args.length != 2) {
      System.err.println("USAGE:\nSocketTextStreamWordCount <hostname> <port>")
      return
    }*/

    val hostName = "localhost"
    val port = 9000

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //Create streams for names and ages by mapping the inputs to the corresponding objects
    val text = env.socketTextStream(hostName, port)
    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .keyBy(0)
      .sum(1)

    counts print

    env.execute("Scala SocketTextStreamWordCount Example")
  }

}

