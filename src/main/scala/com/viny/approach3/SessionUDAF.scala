package com.viny.approach3

import java.math.BigDecimal
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

class SessionUDAF(val windowTime:BigDecimal) extends UserDefinedAggregateFunction {
  def deterministic: Boolean = true
  def inputSchema: StructType = StructType(Array(StructField("timediff", DecimalType(18,6))))
  def dataType: DataType = IntegerType


  def bufferSchema = StructType(Array(
    StructField("id", IntegerType),
    StructField("smcnt", DecimalType(18,6))
  ))

  def initialize(buffer: MutableAggregationBuffer) = {
    /* buffer(0) is the SessionID
     As a future enhancement may be generate new random session id, rather keeping as simple incrementing number from 0.
     Having random session id , it will be easier to get the to total number of sessions in the data and it will not have to depend
     on grouping clientip with sessionid.
     */
    buffer(0) = 0
    buffer(1) = new BigDecimal("0.0") //Sumoftimediff, sets back to 0 when it gets > 15
  }

  /*for a given key/clientip ordered by ts, sum up the timediff and when it becomes greater the fixed window time,
    then increment the sessionid and set sum to 0 and continue the process again.
 */
  def update(buffer: MutableAggregationBuffer, input: Row) = {

    //Checks if timediff is greater than the window time
    if((buffer.getAs[BigDecimal](1).add(input.getAs[BigDecimal](0))).compareTo(windowTime) <= 0){
      buffer(1) = buffer.getAs[BigDecimal](1).add(input.getAs[BigDecimal](0))

    }
      //If timediff summ is greater than window time, create new session
    else if((buffer.getAs[BigDecimal](1).add(input.getAs[BigDecimal](0))).compareTo(windowTime) > 0){
      buffer(0) = buffer.getAs[Int](0) + 1 //Could have generated random sessionid, then a simple increment
      buffer(1) =  new BigDecimal("0.0")
    }
    else{
      throw new Exception("Error from SessionUDAF")
    }

  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {}
  def evaluate(buffer: Row): Int = buffer.getInt(0) // return SessionId
}
