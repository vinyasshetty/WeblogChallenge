package com.viny.approach1

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._

object WebLogMain {

  def standardize(spark:SparkSession ,raw_data:DataFrame):DataFrame ={
    /*
      Extracting the IP only from the request_ip column and also extracting only the URL from request column
     I am creating a fixed 15 minute window in this approach, in this approach the 15 minute window session
     start and end time is fixed for all users.
     This approach does NOT have user personalized 15 minute window session start and end time.
     */
    import spark.implicits._
    val std_data = raw_data.select($"ts".cast("timestamp"),
                                    split($"request_ip",":")(0).as("clientip"),
                                    split($"request"," ")(1).as("url"),
                                    $"elb_response_code",
                                    window($"ts".cast("timestamp"),"15 minute").as("wndw"))

    return std_data
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("weblog").master("local[*]").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

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

    //Reading the initial data
    val raw_data = spark.read.schema(sch).option("delimiter"," ").csv("/Users/vinyasshetty/paytm/WeblogChallenge/data/2015_07_22_mktplace_shop_web_log_sample.log.gz")

    //Just looked at the Total Record Count :1158500
    println(raw_data.count())

    //Standardize the data
    val std_data = standardize(spark, raw_data)

    //1) Aggregrate all page hits by visitor/IP during a session
    std_data.groupBy("wndw","clientip").agg(count($"*").as("cnt"))
            .orderBy($"cnt".desc).show(5,false)

    /*
    Here is where i am calculating the individual user session time in the given fixed(start/end time)
    15 minute window session .ie i group by window and clientip and take the
     */
    val process_data =  std_data.groupBy("wndw", "clientip")
                                .agg(((max($"ts").cast("long") - min($"ts").cast("long"))/60).as("sessiontime"))

    //2) Determine the average session time : 0.9737394513887734
    process_data.select(avg($"sessiontime")).show()

    // Average Session Time Per client, have also provided the number of sessions per client

    process_data.groupBy($"clientip")
                .agg(avg($"sessiontime").as("avgsessionperclient"), count($"*").as("numsessionperclient"))
                .orderBy($"avgsessionperclient".desc).show(false)


    //3) Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.
    //Grouping by window and clientip and then getting the distinct count of URL.
    std_data.groupBy("wndw", "clientip")
             .agg(countDistinct($"url").as("disturlcnt"))
             .orderBy($"disturlcnt".desc).show(false)


    //4) Find the most engaged users, ie the IPs with the longest session times
    process_data.orderBy($"sessiontime".desc).show(false)


  }

}
