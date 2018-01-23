package com.batian.bigdata.plant.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

import scala.util.Try

/**
  * Created by Ricky on 2018/1/22
  *
  * @author Tomcox
  */
object GroupContactUDAF extends UserDefinedAggregateFunction{
  override def inputSchema: StructType = {
    StructType(Array(
      StructField("v1", StringType)
    ))
  }

  override def bufferSchema: StructType = {
    StructType(Array(
      StructField("v2",StringType)
    ))
  }

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0,"")
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //获取输入数据 格式是："101：上海"
    val inputValue = input.getString(0)

    //获取缓冲区数据
    val bufferValue = buffer.getString(0)

    //合并数据
    val mergeValue = this.mergeValue(bufferValue,inputValue)

    //更新环形缓冲区
    buffer.update(0,mergeValue)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //获取环形缓冲区数据
    var bufferValue1 = buffer1.getString(0)
    val bufferValue2 = buffer2.getString(0)
    val ( tmpStr <- bufferValue2.split(",")) {
      //合并bufferValue1 的tmpStr的值
      bufferValue1 =this.mergeValue(bufferValue1,tmpStr)
    }

    //更新缓冲区
    buffer1.update(0,bufferValue1)
  }

  override def evaluate(buffer: Row): Any = buffer.getString(0)

  private def mergeValue(buffer: String, str: String,defaultValue:Int = 1): String ={
    if (str == null || str.isEmpty) {
      buffer
    }else if (buffer == null  || buffer.isEmpty) {
      str
    }else {
      val arr1 = str.split(":")
      val key = arr1(0) + "：" + arr1(1)
      if (buffer.contains(key)) {
        //需要累加数据值
        buffer
          .split(",")
          .map(v => {
            if (v.contains(key)) {
              //增加value值
              val value = v.split(":")(2).toInt + Try(arr1(2).toInt).getOrElse(defaultValue)
              s"${key}:${value}"
            } else {
              v
            }
          })
          .mkString(",")
      }else {
        //增加数据
        buffer + "," + str
      }
    }
    ""
  }
}
