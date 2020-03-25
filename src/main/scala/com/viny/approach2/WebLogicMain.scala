package com.viny.approach2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import java.math.BigDecimal

case class Info(ts:String,url:String)

/*
THIS IMPLEMENTATION IS NOT SCALABLE AND WILL FAIL WHEN RUN ON LARGE DATASETS.
 */

object WebLogicMain {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("weblog").master("local[*]").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    //Window Duartion
    val WINDOW_TIME = "15.0"

    val sch = StructType.fromDDL("""ts string,
                                          elb_name string,
                                          request_ip string,
                                          backend_ip string,
                                          request_processing_time double,
                                          backend_processing_time double,
                                          client_response_time double,
                                          elb_response_code string,
                                          backend_response_code string,
                                          received_bytes bigint,
                                          sent_bytes bigint,
                                          request string,
                                          user_agent string,
                                          ssl_cipher string,
                                          ssl_protocol string""")

    //Reading the initial data. Location is set to my personal home location
    val raw_data = spark.read.schema(sch).option("delimiter"," ").csv("/Users/vinyasshetty/paytm/WeblogChallenge/data/2015_07_22_mktplace_shop_web_log_sample.log.gz")

    val wndw = Window.partitionBy("clientip").orderBy($"ts")
    val r_wndw = Window.partitionBy("clientip").orderBy($"ts".desc)

    val std_data = raw_data.select(date_format($"ts".cast("timestamp"), "yyyy-MM-dd HH:mm:ss").cast("timestamp").as("ts"),
      split($"request_ip",":")(0).as("clientip"),split($"request"," ")(1).as("url"),
      $"elb_response_code")
      .withColumn("prevts", lag($"ts",1).over(wndw))
      .withColumn("prevdiff", when($"prevts".isNull,0).otherwise(($"ts".cast("long") - $"prevts".cast("long"))/60).cast("decimal(12,4)"))
      .select($"clientip", collect_list(struct($"ts".cast("string").as("ts"),$"url")).over(wndw).as("ds_s"),
        collect_list($"prevdiff").over(wndw).as("prevdiff_l"),row_number().over(r_wndw).as("rw"))
      .filter($"rw" <=> 1)



    def session_func(xs:Seq[Info], ys:Seq[BigDecimal]):Seq[(Info,Int)]={
      val res = ys.foldLeft((List[Int](),new BigDecimal("0.0"),0)){case ((l,p,q),i) => {
        if(i.doubleValue() == 0) (q::l,i.add(p),q)
        else if((i.add(p)).compareTo(new BigDecimal("15")) <=0 ) (q::l,i.add(p),q)
        else ((q+1)::l,new BigDecimal("0"),q+1)
      }}._1
      xs zip (res.reverse)
    }

    val u_func = udf(session_func _)
   // val u_func = spark.udf.register("session_func", session_func _)

    val processed_data = std_data.select($"clientip",explode(u_func($"ds_s",$"prevdiff_l")).as("new_col"))
                                 .select($"clientip",$"new_col"("_1")("ts").as("ts").cast("timestamp"),
                                         $"new_col"("_1")("url").as("url"),$"new_col"("_2").as("sessionid"))

    spark.sparkContext.setCheckpointDir("")
    //1) aggregrate all page hits by visitor/IP during a session.
    processed_data.groupBy("sessionid","clientip")
                  .agg(count($"*").as("cnt")).orderBy($"cnt".desc)
                  .show(5,false)

    //2) Determine the average session time
    processed_data.groupBy($"clientip",$"sessionid")
                  .agg(((max($"ts").cast("long") - min($"ts").cast("long"))/60).as("sessionduration"),count($"*"))
                  .select(avg($"sessionduration")).show()

    //3) Determine unique URL visits per session. Hence used countDistinct
    processed_data.groupBy($"sessionid",$"clientip")
                  .agg(min($"ts").as("session_starttime"),max($"ts").as("session_endtime"),
                       countDistinct($"url").as("distincturlpersession"))
                  .orderBy($"distincturlpersession".desc).show(false)

    //4) Find the most engaged users, ie the IPs with the longest session times
    processed_data.groupBy($"sessionid",$"clientip")
                  .agg(((max($"ts").cast("long")-min($"ts").cast("long"))/60).cast("decimal(12,4)").as("sessiontime"),
                         min($"ts").as("session_starttime"),max($"ts").as("session_endtime"))
                  .orderBy($"sessiontime".desc).show(false)

  }

}
