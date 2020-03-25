package com.viny.approach3

import java.util.Date

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._

object Test {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("weblog").master("local[*]").getOrCreate()
    import spark.implicits._
    val wndw = Window.partitionBy("clientip").orderBy($"ts")
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

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    val std_data = raw_data.select(
      $"ts".as("ts_string"),
      regexp_replace($"ts","T"," ").as("ts_str_new"),
      to_timestamp($"ts", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'").as("ts_timestamp"),
      $"ts".cast("timestamp").as("ts_dir_timestamp"),

      // date_format($"ts".cast("timestamp"), "yyyy-MM-dd HH:mm:ss").cast("timestamp").as("ts_new"),
      // split($"request_ip",":")(0).as("clientip"),split($"request"," ")(1).as("url"),
      $"elb_response_code")

    import java.time.Instant
    import java.time.format.DateTimeFormatter
    import java.time.temporal.TemporalAccessor
    val s = "2015-07-22T09:00:28.019143Z"
    val ta = DateTimeFormatter.ISO_INSTANT.parse(s)
    val i = Instant.from(ta)
    val d = Date.from(i)
    println(d)

    std_data.select($"ts_string",$"ts_str_new",$"ts_timestamp",$"ts_dir_timestamp").show(false)
    //std_data.explain(true)
  }

}
