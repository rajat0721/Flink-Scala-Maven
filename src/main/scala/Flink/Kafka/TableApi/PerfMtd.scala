/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package Flink.Kafka.TableApi

import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
//import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors._

object PerfMtd {
  def main(args: Array[String]) {

    // Creating Stream Execution Environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // Setting RocksDB backend for check-pointing. TODO : HDFS/DFS for prod environment
    //env.setStateBackend(new RocksDBStateBackend("file:///home/flink-1.9.0/checkpoints"))
    env.enableCheckpointing(150000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    //Creating StreamTableEnvironment
    val stenv = StreamTableEnvironment.create(env)

    // Flink.Kafka Server details
    val kafkaBootStrapServer = "localhost:9092"

    // Sink Flink.Kafka Topic for kafka connect
    val sinkTopic = "perfmtd_sink"

    // Connect to ACYRPF topic and register as table
    stenv
      .connect(new Kafka()
        .version("universal")
        .topic("ACYRPF")
        .property("bootstrap.servers",kafkaBootStrapServer)
        .startFromEarliest()
      )
      .withFormat(new Json()
        .failOnMissingField(false)
        .deriveSchema()
      )
      .withSchema(new Schema()
        .field("acctyear",Types.STRING())
        .field("acctmnth",Types.STRING())
        .field("frdate",Types.STRING())
        .field("todate",Types.STRING())
        .field("usrprf",Types.STRING())
        .field("jobnm",Types.STRING())
        .field("datime",Types.STRING())
        .field("id",Types.STRING())
        .field("op_type",Types.STRING())
      )
      .inAppendMode()
      .registerTableSource("ACYRPF_SRC")

    // Handle Updates/Deletes
    val acyrpf = stenv.sqlQuery("""SELECT a.* from ACYRPF_SRC a INNER JOIN  (select id,max(datime) as datime from ACYRPF_SRC group by id) b ON a.id = b.id AND a.datime = b.datime where op_type<>'D'""")

    // Register table as ACYRPF
    stenv.registerTable("ACYRPF",acyrpf)


    val acyrpf_single = stenv.sqlQuery("""SELECT frdate,todate,'test' as joincolumn FROM ACYRPF WHERE acctyear = '2019' AND acctmnth = '8'""")

    stenv.registerTable("ACYRPF_1",acyrpf_single)

    // Connect to CHDRPF topic and register as table
    stenv
      .connect(new Kafka()
        .version("universal")
        .topic("CHDRPF")
        .property("bootstrap.servers",kafkaBootStrapServer)
        .startFromEarliest()
      )
      .withFormat(new Json()
        .failOnMissingField(false)
        .deriveSchema()
      )
      .withSchema(new Schema()
        .field("chdrnum",Types.STRING())
        .field("servunit",Types.STRING())
        .field("cnttype",Types.STRING())
        .field("tranno",Types.STRING())
        .field("validflag",Types.STRING())
        .field("currfrom",Types.STRING())
        .field("currto",Types.STRING())
        .field("statcode",Types.STRING())
        .field("statdate",Types.STRING())
        .field("stattran",Types.STRING())
        .field("occdate",Types.STRING())
        .field("ccdate",Types.STRING())
        .field("reptype",Types.STRING())
        .field("repnum",Types.STRING())
        .field("cownnum",Types.STRING())
        .field("jownnum",Types.STRING())
        .field("payrnum",Types.STRING())
        .field("despnum",Types.STRING())
        .field("asgnnum",Types.STRING())
        .field("cntbranch",Types.STRING())
        .field("agntnum",Types.STRING())
        .field("billfreq",Types.STRING())
        .field("billchnl",Types.STRING())
        .field("collchnl",Types.STRING())
        .field("btdate",Types.STRING())
        .field("ptdate",Types.STRING())
        .field("sinstfrom",Types.STRING())
        .field("sinstto",Types.STRING())
        .field("sinstamt01",Types.STRING())
        .field("sinstamt06",Types.STRING())
        .field("instfrom",Types.STRING())
        .field("instto",Types.STRING())
        .field("insttot01",Types.STRING())
        .field("insttot06",Types.STRING())
        .field("instpast01",Types.STRING())
        .field("instpast06",Types.STRING())
        .field("outstamt",Types.STRING())
        .field("bankkey",Types.STRING())
        .field("bankacckey",Types.STRING())
        .field("billsupr",Types.STRING())
        .field("billspfrom",Types.STRING())
        .field("billspto",Types.STRING())
        .field("isam01",Types.STRING())
        .field("isam06",Types.STRING())
        .field("pstcde",Types.STRING())
        .field("pstrsn",Types.STRING())
        .field("psttrn",Types.STRING())
        .field("pstdat",Types.STRING())
        .field("bankcode",Types.STRING())
        .field("datime",Types.STRING())
        .field("id",Types.STRING())
        .field("chdrcoy",Types.STRING())
        .field("op_type",Types.STRING())
      )
      .inAppendMode()
      .registerTableSource("CHDRPF_SRC")


    // Query to handle inserts /updates /deletes
    val chdrpf = stenv.sqlQuery("""SELECT a.* from CHDRPF_SRC a INNER JOIN  (select id,max(datime) as datime from CHDRPF_SRC group by id) b ON a.id = b.id AND a.datime = b.datime where op_type<>'D'""")

    // Register table as CHDRPF
    stenv.registerTable("CHDRPF",chdrpf)


    // Connect to COVTPF Topic and register as table
    stenv
      .connect(new Kafka()
        .version("universal")
        .topic("COVTPF")
        .property("bootstrap.servers", kafkaBootStrapServer)
        .startFromEarliest()
      )
      .withFormat(new Json()
        .failOnMissingField(false)
        .deriveSchema()
      )
      .withSchema(new Schema()
        .field("chdrcoy",Types.STRING())
        .field("chdrnum",Types.STRING())
        .field("life",Types.STRING())
        .field("coverage",Types.STRING())
        .field("rider",Types.STRING())
        .field("termid",Types.STRING())
        .field("transaction_date",Types.STRING())
        .field("transaction_time",Types.STRING())
        .field("user_id",Types.STRING())
        .field("crtable",Types.STRING())
        .field("risk_cess_date",Types.STRING())
        .field("prem_cess_date",Types.STRING())
        .field("risk_cess_age",Types.STRING())
        .field("prem_cess_age",Types.STRING())
        .field("risk_cess_term",Types.STRING())
        .field("prem_cess_term",Types.STRING())
        .field("sumins",Types.STRING())
        .field("liencd",Types.STRING())
        .field("mortcls",Types.STRING())
        .field("jlife",Types.STRING())
        .field("reserve_units_ind",Types.STRING())
        .field("reserve_units_date",Types.STRING())
        .field("polinc",Types.STRING())
        .field("numapp",Types.STRING())
        .field("effdate",Types.STRING())
        .field("sex01",Types.STRING())
        .field("sex02",Types.STRING())
        .field("anb_at_ccd01",Types.STRING())
        .field("anb_at_ccd02",Types.STRING())
        .field("billfreq",Types.STRING())
        .field("billchnl",Types.STRING())
        .field("seqnbr",Types.STRING())
        .field("singp",Types.INT())
        .field("instprem",Types.INT())
        .field("plan_suffix",Types.STRING())
        .field("payrseqno",Types.STRING())
        .field("cntcurr",Types.STRING())
        .field("ben_cess_age",Types.STRING())
        .field("ben_cess_term",Types.STRING())
        .field("ben_cess_date",Types.STRING())
        .field("bappmeth",Types.STRING())
        .field("zbinstprem",Types.STRING())
        .field("zlinstprem",Types.STRING())
        .field("zdivopt",Types.STRING())
        .field("paycoy",Types.STRING())
        .field("payclt",Types.STRING())
        .field("paymth",Types.STRING())
        .field("bankkey",Types.STRING())
        .field("bankacckey",Types.STRING())
        .field("paycurr",Types.STRING())
        .field("facthous",Types.STRING())
        .field("user_profile",Types.STRING())
        .field("job_name",Types.STRING())
        .field("datime",Types.STRING())
        .field("id",Types.STRING())
        .field("op_type",Types.STRING())
      )
      .inAppendMode()
      .registerTableSource("COVTPF_SRC")


    // Query to handle Inserts/Updates/Deletes
    val covtpf = stenv.sqlQuery("""SELECT a.* from COVTPF_SRC a INNER JOIN  (select id,max(datime) as datime from COVTPF_SRC group by id) b ON a.id = b.id AND a.datime = b.datime where op_type<>'D'""")

    // Register table as COVTPF
    stenv.registerTable("COVTPF",covtpf)


    // Connect to HPADPF Topic and register as table
    stenv
      .connect(new Kafka()
        .version("universal")
        .topic("HPADPF")
        .property("bootstrap.servers",kafkaBootStrapServer)
        .startFromEarliest()
      )
      .withFormat(new Json()
        .failOnMissingField(false)
        .deriveSchema()
      )
      .withSchema(new Schema()
        .field("chdrcoy",Types.STRING())
        .field("chdrnum",Types.STRING())
        .field("validflag",Types.STRING())
        .field("hpropdte",Types.STRING())
        .field("hprrcvdt",Types.STRING())
        .field("hissdte",Types.STRING())
        .field("huwdcdte",Types.STRING())
        .field("hoissdte",Types.STRING())
        .field("zoissbrn",Types.STRING())
        .field("zdoctor",Types.STRING())
        .field("znfopt",Types.STRING())
        .field("prospnum",Types.STRING())
        .field("usrprf",Types.STRING())
        .field("jobnm",Types.STRING())
        .field("datime",Types.STRING())
        .field("id",Types.STRING())
        .field("op_type",Types.STRING())
      )
      .inAppendMode()
      .registerTableSource("HPADPF_SRC")

    // Query to handle Inserts/ Updates /Deletes
    val hpadpf = stenv.sqlQuery("""SELECT a.* from HPADPF_SRC a INNER JOIN  (select id,max(datime) as datime from HPADPF_SRC group by id) b ON a.id = b.id AND a.datime = b.datime where op_type<>'D'""")

    // Register table as HPADPF
    stenv.registerTable("HPADPF",hpadpf)

    //Connect to PCDDPF topic and register as tables
    stenv
      .connect(new Kafka()
        .version("universal")
        .topic("PCDDPF")
        .property("bootstrap.servers",kafkaBootStrapServer)
        .startFromEarliest()
      )
      .withFormat(new Json()
        .failOnMissingField(false)
        .deriveSchema()
      )
      .withSchema(new Schema()
        .field("chdrcoy",Types.STRING())
        .field("chdrnum",Types.STRING())
        .field("agntnum",Types.STRING())
        .field("splitc",Types.STRING())
        .field("splitb",Types.INT())
        .field("validflag",Types.STRING())
        .field("tranno",Types.STRING())
        .field("currfrom",Types.STRING())
        .field("currto",Types.STRING())
        .field("user_id",Types.STRING())
        .field("termid",Types.STRING())
        .field("trtm",Types.STRING())
        .field("trdt",Types.STRING())
        .field("usrprf",Types.STRING())
        .field("jobnm",Types.STRING())
        .field("datime",Types.STRING())
        .field("zsplfyp",Types.STRING())
        .field("id",Types.STRING())
        .field("op_type",Types.STRING())
      )
      .inAppendMode()
      .registerTableSource("PCDDPF_SRC")

    // Query to handle updates / Inserts / Deletes
    val pcddpf = stenv.sqlQuery("""SELECT a.*,'test' as joincolumn from PCDDPF_SRC a INNER JOIN  (select id,max(datime) as datime from PCDDPF_SRC group by id) b ON a.id = b.id AND a.datime = b.datime where op_type<>'D'""")

    //Register table as PCDDPF
    stenv.registerTable("PCDDPF",pcddpf)


    // Run main query of perfMTD  to get sumIPIFPS
    val result1 = stenv.sqlQuery("""select proposal.agntnum, CAST( sum(proposal.ip) as Double ) as sumIPIFPS from ( select c.agntnum, b.chdrnum, c.splitb, sum (b.singp) * ( cast( c.splitb / 100 as decimal (3, 2) ) ) * (10 / 100), sum (b.instprem) * ( cast( c.splitb / 100 as decimal (3, 2) ) ) as ip from CHDRPF a inner join COVTPF b on a.chdrnum = b.chdrnum inner join PCDDPF c on a.chdrnum = c.chdrnum inner join HPADPF d on a.chdrnum = d.chdrnum inner join ACYRPF_1 e on c.joincolumn = e.joincolumn where a.chdrcoy = '2' and a.chdrcoy = b.chdrcoy and a.statcode = 'PS' and a.validflag in ('1', '3') and d.hpropdte >= e.frdate and d.hpropdte <= e.todate group by c.agntnum, b.chdrnum, c.splitb ) proposal group by proposal.agntnum""")


    // Create a custom kafka consumer.
    val myProducer = new FlinkKafkaProducer011[(Boolean, (String, Double))](
      kafkaBootStrapServer,
      sinkTopic,
      new KafkaOutputSchema())

    // Convert to retract Stream
    val stream1: DataStream[(Boolean,(String,Double))] = stenv.toRetractStream[(String,Double)](result1)

    stream1.addSink(myProducer)

    // execute program
    stenv.execute("Flink Streaming Scala")
  }
}
