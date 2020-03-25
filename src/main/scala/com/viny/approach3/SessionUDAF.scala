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
    buffer(0) = 0  // SessionID
    buffer(1) = new BigDecimal("0.0") //Sumoftimediff, sets back to 0 when it gets > 15
  }

  /*for a given key/clientip ordered by ts, sum up the timediff and when it becomes greater the fixed window time,
    then increment the sessionid and set sum to 0 and continue the process again.
 */
  def update(buffer: MutableAggregationBuffer, input: Row) = {

    if((buffer.getAs[BigDecimal](1).add(input.getAs[BigDecimal](0))).compareTo(windowTime) <= 0){
      buffer(1) = buffer.getAs[BigDecimal](1).add(input.getAs[BigDecimal](0))

    }
    else if((buffer.getAs[BigDecimal](1).add(input.getAs[BigDecimal](0))).compareTo(windowTime) > 0){
      buffer(0) = buffer.getAs[Int](0) + 1
      buffer(1) =  new BigDecimal("0.0")
    }
    else{
      throw new Exception("Error from SessionUDAF")
    }

  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {}
  def evaluate(buffer: Row): Int = buffer.getInt(0) // return SessionId
}
