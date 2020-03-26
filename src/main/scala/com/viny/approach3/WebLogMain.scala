package com.viny.approach3

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import java.math.BigDecimal


object WebLogMain {

  /*
    This function cleans up the raw_data.
    Also extracted IP and excluded the ports from request_ip column
    Extracted only the URL from request.
    Using lag functions populated a column :prevts for every client, will use this for generating sessionid later
    Also generated a new column called timediff which will have the time difference(in mins) for every client between their two
    consecutive occurences.Thier first appearance record will have timediff as 0 ie when prevts is null.
    Casted this to Decimal(18,6).
     */
  def standardize(spark:SparkSession, raw_data:DataFrame):DataFrame={
    import spark.implicits._

    //Creating a Window of clientip and sorted by timestamp(ts) column
    val wndw = Window.partitionBy("clientip").orderBy($"ts")

    //Without this ,spark converted the UTC time to my PST time ie it added 5 hrs to time present in the file
    //Also i used decimal(18,6) when i converted the timestamp to make sure it retained the microsecond information.
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    val std_data = raw_data.select(
                                  //date_format($"ts".cast("timestamp"), "yyyy-MM-dd HH:mm:ss").cast("timestamp").as("ts"),
                                 //unix_timestamp($"ts", "yyyy-MM-dd'T'HH:mm:ss").cast("timestamp").as("ts"),
                                  $"ts".cast("timestamp").as("ts"),
                                  split($"request_ip",":")(0).as("clientip"),
                                  split($"request"," ")(1).as("url"),
                                  $"elb_response_code")
                           .withColumn("prevts", lag($"ts",1).over(wndw))
                           .withColumn("timediff", when($"prevts".isNull,0).otherwise(($"ts".cast("decimal(18,6)") - $"prevts".cast("decimal(18,6)"))/60.0).cast("decimal(18,6)"))

    //Ideally its better to add a filter to remove any ts column with null values
    return std_data

  }


  def main(args: Array[String]): Unit = {
    //Running this on my local system
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

    //Just looked at the Total Record Count :1158500
    println(raw_data.count())


    //Cleans up the raw data and derives some new columns
    val std_data = standardize(spark, raw_data)


    //UDAF Function with window Size
    val session_create = new SessionUDAF(new BigDecimal(WINDOW_TIME))

    /*
    This code will use the above UDAF and for every visitor/clientip keeps him in a session until X mins(15 mins) after
    which a new session will be created for the same user.Same would happen for every visitor independently.
    Sessionid starts from 0 for every client.
    Have added a extra descending order on timediff to window below  as compared to window function used in standardize while
    calculating prev_ts(lag) just to make sure that UDAF calc and prev_ts/timediff have the same order even when we have
    two clients with same ts value.
     but even if i remove it and use like earlier window ie :
     val new_wndw = Window.partitionBy("clientip").orderBy($"ts"),
     i believe it should work fine  because i looked at the physical plan and it sorts only once and calculates lag and UDAF
     in the same sorted order. But have kept it for now(Downside, see two sorts in Physical Plan).
     */
     val new_wndw = Window.partitionBy("clientip").orderBy($"ts",$"timediff".desc)
    //val new_wndw = Window.partitionBy("clientip").orderBy($"ts")

    val sessioned_data = std_data.select($"*", session_create($"timediff").over(new_wndw).as("sessionid"))
    /*Schema of sessioned_data
    root
      |-- ts: timestamp (nullable = true)
      |-- clientip: string (nullable = true)
      |-- url: string (nullable = true)
      |-- elb_response_code: string (nullable = true)
      |-- prevts: timestamp (nullable = true)
      |-- timediff: decimal(18,6) (nullable = true)
      |-- sessionid: integer (nullable = true)
     */

    //sessioned_data.explain(true)

    //sessioned_data.count() , count still same as raw_data

    //1) aggregrate all page hits by visitor/IP during a session.
    //Sessions for a client are identified by the sessionid column, also include the timerange
    //Thought of excluding records where response code was 4xx or 5xx, but kept it for now.
    sessioned_data.groupBy($"sessionid", $"clientip")
                  .agg(count($"*").as("cnt"),
                       min($"ts").as("session_starttime"), max($"ts").as("session_endtime"))
                  .orderBy($"cnt".desc).show(false)


    //2) Determine the average session time
    /*
    For a client session , i calculate the session duration(mins), by subtracting the min and max ts in tat session.
    Then calculate the average across all sessions and all clients
    Avg Session Time : 1.49 mins for 15 min window
    Avg Session Time increases to 2.4992 when we use a 30 min window.So 30 mins looks like a better option.
     */
    sessioned_data.groupBy($"sessionid",$"clientip")
                  .agg(((max($"ts").cast("decimal(18,6)") - min($"ts").cast("decimal(18,6)"))/60.0).cast("decimal(18,6)").as("sessiontime"))
                  .select(avg($"sessiontime").as("avgsession(in mins)")).show()

    //Total Number of Sessions -- 113375
    println(sessioned_data.groupBy($"sessionid",$"clientip").agg(max(lit(1)).as("dummy")).count())

    //Total Sum of Session Time -- 168998.57 mins
    sessioned_data.groupBy($"sessionid",$"clientip")
      .agg(((max($"ts").cast("decimal(18,6)")-min($"ts").cast("decimal(18,6)"))/60).cast("decimal(18,6)").as("sessiontime"))
      .select(sum($"sessiontime")).show()

    //Here i am calcuating the avg session time and number of sessions per clientip
    sessioned_data.groupBy($"sessionid", $"clientip")
                  .agg(((max($"ts").cast("decimal(18,6)") - min($"ts").cast("decimal(18,6)"))/60.0).cast("decimal(18,6)").as("sessiontime"))
                  .groupBy($"clientip")
                  .agg(avg($"sessiontime").as("avgsessionperclient(in mins)"), count($"*").as("numsessionperclient"))
                  .orderBy($"avgsessionperclient(in mins)".desc).show()


    //3) Determine unique URL visits per session. Hence used countDistinct
    /*
    One point here is I thought of parsing the URL, so that i can use only parts of the URL to provide valuable info.
    But was not not sure what part of the url would provide actual business value.

    sessioned_data.filter(!($"backend_processing_time" <=> -1 or $"request_processing_time" <=> -1) , should have
    applied this filter before counting the unique URL since its better to exclude requests that did not go through?
     */
    sessioned_data.groupBy($"sessionid", $"clientip")
                 .agg(min($"ts").as("session_starttime"), max($"ts").as("session_endtime"),
                      countDistinct($"url").as("distincturlpersession"))
                 .orderBy($"distincturlpersession".desc).show(false)


    //4) Find the most engaged users, ie the IPs with the longest session times
    /*For a client session group,i calculate the session duration(mins), by subtracting the min and max ts in tat session.
    Then order by descending order.If we want to rank them then we use use Window function across whole dataset and ordered
    by sessiontime descending, but will not work on large datasets since data would end up in one partition due to
    absence of a partition key
     */
    sessioned_data.groupBy($"sessionid",$"clientip")
                  .agg(((max($"ts").cast("decimal(18,6)") - min($"ts").cast("decimal(18,6)"))/60.0).cast("decimal(18,6)").as("sessiontime"),
                        min($"ts").as("session_starttime"), max($"ts").as("session_endtime"))
                  .orderBy($"sessiontime".desc).show(false)
    /*
    I have used same groupBy repeatdly from Point 2 to 4 , this was just so that it was easier for me to explain my thoughts.
    Could have ideally reused them or looked for merging the queries into one.
    */

    /*
    Rangebin 0 - Number of session where session time is 0 mins ie basically user was seen only once in a session.We have such 22192 sessions
    Rangebin 1 - User spent between > 0 and <=2.0 mins . We have such 66045 sessions
    Rangebin 2 - User spent between > 2 and <=4.0 mins . We have such 15239 sessions
    Rangebin 3 - User spent between > 4 and <=6.0 mins . We have such 3387 sessions
    and so on ...
   */
    sessioned_data.groupBy($"sessionid",$"clientip")
      .agg(((max($"ts").cast("decimal(18,6)")-min($"ts").cast("decimal(18,6)"))/60.0).cast("decimal(18,6)").as("sessiontime"))
      .groupBy(ceil($"sessiontime"/2.0).as("rangebin")).count().orderBy($"rangebin").show(false)



  }

}
