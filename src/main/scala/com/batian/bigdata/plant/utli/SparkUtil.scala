package com.batian.bigdata.plant.utli

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf,SparkContext}
/**
  * Created by Ricky on 2018/1/19
  *
  * @author Tomcox
  */
object SparkConfUtil {
  /**
    *
    * @param conf
    * @return
    */
  def getSparkContext(conf: SparkConf = new SparkConf()) : SparkContext = SparkContext.getOrCreate(conf)

  /**
    * 根据APP名称是否是本地运行环境返回一个sparkConf对象
    */
  def gennerateSparkConf(appName:String,isLocal:Boolean = false):SparkConf = {
    val conf = if (isLocal) {
      //本地运行环境，设置master
      new SparkConf()
        .setAppName(appName)
        .setMaster("local[*]")
    } else {
      new SparkConf()
        .setAppName(appName)
    }
    //设置一些共同配置项
    conf.set("spark.sql.shuffle.partitions","10")
    conf.set("spark.streaming.blockInterval","1s")

    //rdd进行数据cache的时候，内存最多允许存储的大小（占executor的内存比例），默认0.6
    //如果内存不够，可能有部数据不会进行cache(CacheManager会对cache的RDD数据进行管理操作（删除不会用的RDD缓存））
    conf.set("spark.storage.memoryFraction","0.6")
    //RDD进行shuffle的时候，shuffle数据写内存的阈值（占executor的内存比例），默认0.2
    conf.set("spark.shuffle.memoryFraction","0.2")
    //TODO:如果发现GC频繁而且持续时间长，这两个参数适当调低


    //参数修改
    //如果修改的HADOOP配置项，必须在配置项Key加一个前缀"spark.hadoop"
    conf.set("spark.hadoop.mapreduce.input.fileinputformat.split.minszie","616448000")

    //返回创建的对象
    conf
  }

}

object  SparkContextUtil {

  /**
    * 根据给定的SparkConf创建给定的SparkContext对象
    * 如果JVM已经存在一个SparkContext对象，那么直接返回已有的数据，否则创建一个新的SparkContext对象并返回
    * @param conf
    * @return
    */

  def getSparkContext(conf: SparkConf = new SparkConf()): SparkContext = SparkContext.getOrCreate(conf)
}

object SQLContextUtil {
  //这里的"_"表示给定空值 @transient表示这个属性不做序列化操作
  @transient private var hiveContext:HiveContext = _
  @transient private var sqlContext: SQLContext = _

  def getInstance(
                 sc:SparkContext,
                 integratedHive : Boolean = true,
                 generateMockData:(SparkContext,SQLContext) => Unit = ( sc: SparkContext ,sQLContext:SQLContext) => {}
                 ):SQLContext = {
    //产生SQLContext对象
    val context = if (integratedHive) {
      if (hiveContext == null ) {
        synchronized{
          if (hiveContext == null ) {
            hiveContext = new HiveContext(sc)
            generateMockData(sc,hiveContext)
          }
        }
      }
      //返回创建好的HiveContext
      hiveContext

    }else {
      if (sqlContext == null)
        synchronized {
          if (sqlContext == null) {
            sqlContext = new SQLContext(sc)
            generateMockData(sc,sqlContext)
          }
        }
      //返回创建好的SQLContext对象
      sqlContext
    }
    context
  }
}