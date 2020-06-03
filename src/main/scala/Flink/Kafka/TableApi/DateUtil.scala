
package Flink.Kafka.TableApi

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import org.apache.flink.table.functions.ScalarFunction

class DateUtil extends ScalarFunction {

  def to_datestr(datime: String): String = {
    val timePattern = DateTimeFormatter.ofPattern("dd-MM-yy HH:mm:ss.SSSSSSSSS a")
    val date = LocalDateTime.parse(datime, timePattern)
    date.toString
  }
}

