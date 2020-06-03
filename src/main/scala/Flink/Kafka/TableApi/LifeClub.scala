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

object LifeClub {
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
    val sinkTopic = "perfmtd2_sink"


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


    val acyrpf_single = stenv.sqlQuery("""SELECT frdate,todate,'test' as joincolumn FROM ACYRPF WHERE acctyear = '2019' AND acctmnth = '7'""")

    stenv.registerTable("ACYRPF_1",acyrpf_single)

    // Connect to ZMPRPF topic and register as table
    stenv
      .connect(new Kafka()
        .version("universal")
        .topic("ZMPRPF")
        .property("bootstrap.servers",kafkaBootStrapServer)
        .startFromEarliest()
      )
      .withFormat(new Json()
        .failOnMissingField(false)
        .deriveSchema()
      )
      .withSchema(new Schema()
        .field("chdrnum",Types.STRING())
        .field("agentnum",Types.STRING())
        .field("initsc",Types.STRING())
        .field("datime",Types.STRING())
        .field("id",Types.STRING())
        .field("op_type",Types.STRING())
      )
      .inAppendMode()
      .registerTableSource("ZMPRPF_SRC")

    // Handle Updates/Deletes
    val zmprpf = stenv.sqlQuery("""SELECT a.* from ZMPRPF_SRC a INNER JOIN  (select id,max(datime) as datime from ZMPRPF_SRC group by id) b ON a.id = b.id AND a.datime = b.datime where op_type<>'D'""")

    // Register table as ZMPRPF
    stenv.registerTable("ZMPRPF",zmprpf)


    // Connect to HPADPF topic and register as table
    stenv
      .connect(new Kafka()
        .version("universal")
        .topic("HPADPF")
        .property("bootstrap.servers","localhost:9092")
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



    // Query to handle inserts /updates /deletes
    val hpadpf = stenv.sqlQuery("""SELECT a.*,'test' as joincolumn from HPADPF_SRC a INNER JOIN  (select id,max(datime) as datime from HPADPF_SRC group by id) b ON a.id = b.id AND a.datime = b.datime where op_type<>'D'""")

    // Register table as HPADPF
    stenv.registerTable("HPADPF",hpadpf)


    // Connect to MAPRPF Topic and register as table
    stenv
      .connect(new Kafka()
        .version("universal")
        .topic("MAPRPF")
        .property("bootstrap.servers","localhost:9092")
        .startFromEarliest()
      )
      .withFormat(new Json()
        .failOnMissingField(false)
        .deriveSchema()
      )
      .withSchema(new Schema()
        .field("agntnum",Types.STRING())
        .field("acctyr",Types.STRING())
        .field("mnth",Types.STRING())
        .field("cnttype",Types.STRING())
        .field("effdate",Types.STRING())
        .field("mlperpp01",Types.STRING())
        .field("mlperpp02",Types.STRING())
        .field("mlperpp03",Types.STRING())
        .field("mlperpp04",Types.STRING())
        .field("mlperpp05",Types.STRING())
        .field("mlperpp06",Types.STRING())
        .field("mlperpp07",Types.STRING())
        .field("mlperpp08",Types.STRING())
        .field("mlperpp09",Types.STRING())
        .field("mlperpp10",Types.STRING())
        .field("mlperpc01",Types.STRING())
        .field("mlperpc02",Types.STRING())
        .field("mlperpc03",Types.STRING())
        .field("mlperpc04",Types.STRING())
        .field("mlperpc05",Types.STRING())
        .field("mlperpc06",Types.STRING())
        .field("mlperpc07",Types.STRING())
        .field("mlperpc08",Types.STRING())
        .field("mlperpc09",Types.STRING())
        .field("mlperpc10",Types.STRING())
        .field("mldirpp01",Types.STRING())
        .field("mldirpp02",Types.STRING())
        .field("mldirpp03",Types.STRING())
        .field("mldirpp04",Types.STRING())
        .field("mldirpp05",Types.STRING())
        .field("mldirpp06",Types.STRING())
        .field("mldirpp07",Types.STRING())
        .field("mldirpp08",Types.STRING())
        .field("mldirpp09",Types.STRING())
        .field("mldirpp10",Types.STRING())
        .field("mldirpc01",Types.STRING())
        .field("mldirpc02",Types.STRING())
        .field("mldirpc03",Types.STRING())
        .field("mldirpc04",Types.STRING())
        .field("mldirpc05",Types.STRING())
        .field("mldirpc06",Types.STRING())
        .field("mldirpc07",Types.STRING())
        .field("mldirpc08",Types.STRING())
        .field("mldirpc09",Types.STRING())
        .field("mldirpc10",Types.STRING())
        .field("mlgrppp01",Types.STRING())
        .field("mlgrppp02",Types.STRING())
        .field("mlgrppp03",Types.STRING())
        .field("mlgrppp04",Types.STRING())
        .field("mlgrppp05",Types.STRING())
        .field("mlgrppp06",Types.STRING())
        .field("mlgrppp07",Types.STRING())
        .field("mlgrppp08",Types.STRING())
        .field("mlgrppp09",Types.STRING())
        .field("mlgrppp10",Types.STRING())
        .field("mlgrppc01",Types.STRING())
        .field("mlgrppc02",Types.STRING())
        .field("mlgrppc03",Types.STRING())
        .field("mlgrppc04",Types.STRING())
        .field("mlgrppc05",Types.STRING())
        .field("mlgrppc06",Types.STRING())
        .field("mlgrppc07",Types.STRING())
        .field("mlgrppc08",Types.STRING())
        .field("mlgrppc09",Types.STRING())
        .field("mlgrppc10",Types.STRING())
        .field("cntcount",Types.STRING())
        .field("sumins",Types.STRING())
        .field("zapi",Types.STRING())
        .field("zcficnt",Types.STRING())
        .field("zprpcnt",Types.STRING())
        .field("zntucnt",Types.STRING())
        .field("zaficnt",Types.STRING())
        .field("aracde",Types.STRING())
        .field("zsalesbr",Types.STRING())
        .field("tsalesunt",Types.STRING())
        .field("gacode",Types.STRING())
        .field("ztrnall",Types.STRING())
        .field("salezone",Types.STRING())
        .field("chdrnum",Types.STRING())
        .field("usrprf",Types.STRING())
        .field("jobnm",Types.STRING())
        .field("datime",Types.STRING())
        .field("id",Types.STRING())
        .field("op_type",Types.STRING())
      )
      .inAppendMode()
      .registerTableSource("MAPRPF_SRC")


    // Query to handle Inserts/Updates/Deletes
    val maprpf = stenv.sqlQuery("""SELECT a.* from MAPRPF_SRC a INNER JOIN  (select id,max(datime) as datime from MAPRPF_SRC group by id) b ON a.id = b.id AND a.datime = b.datime where op_type<>'D'""")

    // Register table as MAPRPF
    stenv.registerTable("MAPRPF",maprpf)


    // Connect to COVRPF Topic and register as table
    stenv
      .connect(new Kafka()
        .version("universal")
        .topic("COVRPF")
        .property("bootstrap.servers","localhost:9092")
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
        .field("jlife",Types.STRING())
        .field("coverage",Types.STRING())
        .field("rider",Types.STRING())
        .field("plnsfx",Types.STRING())
        .field("validflag",Types.STRING())
        .field("tranno",Types.STRING())
        .field("currfrom",Types.STRING())
        .field("currto",Types.STRING())
        .field("statcode",Types.STRING())
        .field("pstatcode",Types.STRING())
        .field("statreasn",Types.STRING())
        .field("crrcd",Types.STRING())
        .field("anbccd",Types.STRING())
        .field("sex",Types.STRING())
        .field("reptcd01",Types.STRING())
        .field("reptcd02",Types.STRING())
        .field("reptcd03",Types.STRING())
        .field("reptcd04",Types.STRING())
        .field("reptcd05",Types.STRING())
        .field("reptcd06",Types.STRING())
        .field("crinst01",Types.STRING())
        .field("crinst02",Types.STRING())
        .field("crinst03",Types.STRING())
        .field("crinst04",Types.STRING())
        .field("crinst05",Types.STRING())
        .field("prmcur",Types.STRING())
        .field("termid",Types.STRING())
        .field("trdt",Types.STRING())
        .field("trtm",Types.STRING())
        .field("user_id",Types.STRING())
        .field("stfund",Types.STRING())
        .field("stsect",Types.STRING())
        .field("stssect",Types.STRING())
        .field("crtable",Types.STRING())
        .field("rcesdte",Types.STRING())
        .field("pcesdte",Types.STRING())
        .field("bcesdte",Types.STRING())
        .field("nxtdte",Types.STRING())
        .field("rcesage",Types.STRING())
        .field("pcesage",Types.STRING())
        .field("bcesage",Types.STRING())
        .field("rcestrm",Types.STRING())
        .field("pcestrm",Types.STRING())
        .field("bcestrm",Types.STRING())
        .field("sumins",Types.STRING())
        .field("sicurr",Types.STRING())
        .field("varsi",Types.STRING())
        .field("mortcls",Types.STRING())
        .field("liencd",Types.STRING())
        .field("ratcls",Types.STRING())
        .field("indxin",Types.STRING())
        .field("bnusin",Types.STRING())
        .field("dpcd",Types.STRING())
        .field("dpamt",Types.STRING())
        .field("dpind",Types.STRING())
        .field("tmben",Types.STRING())
        .field("emv01",Types.STRING())
        .field("emv02",Types.STRING())
        .field("emvdte01",Types.STRING())
        .field("emvdte02",Types.STRING())
        .field("emvint01",Types.STRING())
        .field("emvint02",Types.STRING())
        .field("campaign",Types.STRING())
        .field("stsmin",Types.STRING())
        .field("rtrnyrs",Types.STRING())
        .field("rsunin",Types.STRING())
        .field("rundte",Types.STRING())
        .field("chgopt",Types.STRING())
        .field("fundsp",Types.STRING())
        .field("pcamth",Types.STRING())
        .field("pcaday",Types.STRING())
        .field("pctmth",Types.STRING())
        .field("pctday",Types.STRING())
        .field("rcamth",Types.STRING())
        .field("rcaday",Types.STRING())
        .field("rctmth",Types.STRING())
        .field("rctday",Types.STRING())
        .field("jllsid",Types.STRING())
        .field("instprem",Types.STRING())
        .field("singp",Types.STRING())
        .field("rrtdat",Types.STRING())
        .field("rrtfrm",Types.STRING())
        .field("bbldat",Types.STRING())
        .field("cbanpr",Types.STRING())
        .field("cbcvin",Types.STRING())
        .field("cbrvpr",Types.STRING())
        .field("cbunst",Types.STRING())
        .field("cpidte",Types.STRING())
        .field("icandt",Types.STRING())
        .field("exaldt",Types.STRING())
        .field("iincdt",Types.STRING())
        .field("crdebt",Types.STRING())
        .field("payrseqno",Types.STRING())
        .field("bappmeth",Types.STRING())
        .field("zbinstprem",Types.STRING())
        .field("zlinstprem",Types.STRING())
        .field("usrprf",Types.STRING())
        .field("jobnm",Types.STRING())
        .field("datime",Types.STRING())
        .field("id",Types.STRING())
        .field("op_type",Types.STRING())
      )
      .inAppendMode()
      .registerTableSource("COVRPF_SRC")


    // Query to handle Inserts/ Updates /Deletes
    val covrpf = stenv.sqlQuery("""SELECT a.* from COVRPF_SRC a INNER JOIN  (select id,max(datime) as datime from COVRPF_SRC group by id) b ON a.id = b.id AND a.datime = b.datime where op_type<>'D'""")

    // Register table as COVRPF
    stenv.registerTable("COVRPF",covrpf)




    // Run main query of perfMTD2  to get IP12
    val result2 = stenv.sqlQuery("""select ip12.agntnum, sum(ip12.cntcount) from ( select cntip.agntnum, cntip.chdrnum, cntcnt.cntcount, cntip.initsc, ( cast( cntip.initsc / cntcnt.cntcount as decimal (15, 2) ) ) as ip from ( select d.agntnum, d.chdrnum, sum (d.cntcount) as cntcount from HPADPF c inner join MAPRPF d on c.chdrnum = d.chdrnum inner join ACYRPF_1 q on c.joincolumn = q.joincolumn where c.chdrcoy = '2' and c.validflag = '1' and c.hoissdte >= q.frdate and c.hoissdte <= q.todate group by d.agntnum, d.chdrnum ) as cntcnt inner join ( select cntctip.agntnum, cntctip.chdrnum, cntctip.initsc from ( select f.agntnum, f.chdrnum, sum (f.initsc) as initsc from HPADPF e inner join ZMPRPF f on e.chdrnum = f.chdrnum inner join ACYRPF_1 q on e.joincolumn = q.joincolumn where e.chdrcoy = '2' and e.validflag = '1' and e.hoissdte >= q.frdate and e.hoissdte <= q.todate group by f.agntnum, f.chdrnum ) as cntctip inner join ( select b.chdrnum, count (b.chdrnum) as cntrider from HPADPF a inner join COVRPF b on a.chdrnum = b.chdrnum inner join ACYRPF_1 q on a.joincolumn = q.joincolumn where a.chdrcoy = '2' and a.chdrcoy = b.chdrcoy and a.validflag = '1' and a.validflag = b.validflag and b.statcode = 'IF' and ( b.life <> '01' or b.coverage <> '01' or b.rider <> '00' ) and a.hoissdte >= q.frdate and a.hoissdte <= q.todate group by b.chdrnum ) as cntrd on cntctip.chdrnum = cntrd.chdrnum where cntrd.cntrider >= 4 ) as cntip on cntcnt.agntnum = cntip.agntnum and cntcnt.chdrnum = cntip.chdrnum where cntcnt.cntcount <> 0 ) as ip12 where ip12.ip >= 12000000 group by ip12.agntnum""")




    // Create a custom kafka consumer.
    val myProducer = new FlinkKafkaProducer011[(Boolean, (String, Double))](
      kafkaBootStrapServer,
      sinkTopic,
      new KafkaOutputSchema())

    // Convert to retract Stream
    val stream2: DataStream[(Boolean,(String,Double))] = stenv.toRetractStream[(String,Double)](result2)

    stream2.addSink(myProducer)

    // execute program
    stenv.execute("Flink Streaming Scala")
  }
}