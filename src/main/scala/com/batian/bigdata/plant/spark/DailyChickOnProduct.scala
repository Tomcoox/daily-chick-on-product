package com.batian.bigdata.plant.spark

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.batian.bigdata.plant.conf.ConfigurationManager
import com.batian.bigdata.plant.constant.Constants
import com.batian.bigdata.plant.dao.factory.DAOFactory
import com.batian.bigdata.plant.util.ParamUtil
import com.batian.bigdata.plant.utli.{SQLContextUtil, SparkConfUtil}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by Ricky on 2018/1/19
  *
  * @author Tomcox
  */



object DailyChickOnProduct {


  //jdbc配置
  lazy val url = ConfigurationManager.getProperty(Constants.JDBC_URL)
  lazy val user = ConfigurationManager.getProperty(Constants.JDBC_USER)
  lazy val password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD)
  lazy val properties = {
    val props = new Properties()
    props.put("user",user)
    props.put("password",password)
    props
  }


  def main(args: Array[String]): Unit = {
    //1.get Arge
    //1.1 get input TaskID
    val taskID = ParamUtil.getTaskIdFromArgs(args)
    //1.2from RDBMS get TaskParam
    val task = if (taskID == null) {
      throw new IllegalStateException("参数异常，taskID不能为空")
    } else {
      //GET DAO object
      val taskDao = DAOFactory.getTaskDAO
      //获取Task
      taskDao.findByTaskId(taskID)
    }

    //1.3从task中获取任务的过滤参数（这里的Task一定不为空）
    if (task.getTaskId != taskID) {
      throw new IllegalArgumentException("参数异常，数据库中没有对应的TaskID的任务")
    }
    val taskParam = ParamUtil.getTaskParam(task)
    if (taskParam == null || taskParam.isEmpty) {
      throw new IllegalStateException("参数异常，任务过滤参数不能为空")
    }

    //2Build SparkContext
    //2.1 Get correlate variable(Application is only，so 'taskID'  need add to behind the task name;
    val appName = Constants.SPARK_APP_NAME_SESSION + taskID
    val isLocal = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)

    //2.2build conf
    val conf = SparkConfUtil.gennerateSparkConf(appName,isLocal)
    //2.3build SC
    val sc = SparkConfUtil.getSparkContext(conf)
    //2.4bulid SparkSQL object
    /**
      * 实际运行这过程中数据存在hive中，但是开发过程中，本地运行的时候，有时候连接Hive不太方便，所以需要使用模拟数据进行本地的测试运行
      */
    val sqlContext = SQLContextUtil.getInstance(
      sc,
      //因为用到ROW_NUMBER,所以这里必须换成true(hiveContext)
      integratedHive = true,
      generateMockData = (sc, sqlContext) => {
        if(isLocal) {
          //加载用户行为数据和user_info数据

        }
      }
    )

    //UDF Definition
    //Define is judge empty
    def isEmpty(str:String) = str == null || str.trim.isEmpty

    sqlContext.udf.register("isEmpty", (str: String) => isEmpty(str))
    sqlContext.udf.register("isNotEmpty",func = (str: String) => {
      //define is Not Empty
      !isEmpty(str)
    })


    //group_contact
    sqlContext.udf.register("concat_int_String",(id:Int,name:String) => {
      id + ":" + name
    })
    sqlContext.udf.register("fetch_product_type",(str:String) => {
      val obj = JSON.parseObject(str)
      println(obj)
      val t = obj.getString("product_type")
      t match {
        case "0" => "自营商品"
        case "1" => "第三方商品"
        case _ => "未知"
      }
    })
    sqlContext.udf.register("group_contact",GroupContactUDAF)

    //三、数据获取及过滤 =》存≤储在Hive中
    val actionDataFrame = this.getActionDataFrameByFilter(sqlContext,taskParam)

  }

  /**
    * 根据过滤参数获取过滤数据
    * @param sqlContext
    * @param taskParam
    * @return
    */
  def getActionDataFrameByFilter(sqlContext: SQLContext, taskParam: JSONObject):DataFrame= {
    //1.获取过滤的参数
    val startDate = ParamUtil.getParam(taskParam,Constants.PARAM_START_DATE)
    val endDate = ParamUtil.getParam(taskParam,Constants.PARAM_END_DATE)
    val sex = ParamUtil.getParam(taskParam,Constants.PARAM_SEX)

    //职业（可以支持多选，用"，"分割数据）
    val professionals = ParamUtil.getParam(taskParam,Constants.PARAM_PROFESSIONALS)

    //TODO:这里的professionals的格式为 aaa,bbb,ccc。但是hql中的语句需要将格式转换为'aaa','bbb','ccc'
    val professionalWhereStr = professionals.map(v => {
      val str:String = v.split(",").map(_.trim).filter(_.nonEmpty).map(t => s"'${t}'").mkString(",")
      s"AND ui.profeessional IN (${str})"
    }).getOrElse("")

    //2.根据过滤条件判断是否需要join其它表（因为有可能会有两张表的过滤条件）
    //TODO:表示性别和职业这两张表中有一个值非空，那么就需要join这两张表，反之不需要join
    val needJoinUserInfo = if(sex.isDefined || professionals.isDefined ) Some(true) else None

    //基于过滤条件参数构建hql语句
    val hql =
      s"""
         |SELECT
         |  uva.click_product_id,uva.city_id
         |FROM
         |  user_visit_action uva
         |  ${needJoinUserInfo.map(v => "JOIN user_info ui ON uva.user_id = ui.user_id").getOrElse("")}
         |WHERE isNotEmpty(uva.click_product_id)
         | ${startDate.map(v => s"AND uva.date >= '${v}' ").getOrElse("")}
         | ${endDate.map(v => s"AND uva.date <= '${v}' ").getOrElse("")}
         | ${sex.map(v => s"AND ui.sex = '${v}' ").getOrElse("")}
         | ${professionalWhereStr}
         | """.stripMargin

    //sql语句容易经常出问题，所以需要养成一个好习惯，在每个SQL语句后面对SQL语句打印一下，出现问题好定位
    println("===========================================")
    println(hql)
    println("===========================================")

    //4.执行hql语句得到DataFrame对象
    val df = sqlContext.sql(hql)

    //返回dataframe
    df

  }

}
