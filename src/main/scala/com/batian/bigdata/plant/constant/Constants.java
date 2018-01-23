package com.batian.bigdata.plant.constant;

/**
 * Created by Ricky on 2018/1/19
 *
 * @author Tomcox
 */
public interface Constants {

    /**
     * Spark Application Constants
     */
    String SPARK_APP_NAME = "DailyChickOnProduct";
    String SPARK_APP_NAME_SESSION = "UserVisitSessionAnalyzeSpark_";


    /**
     * Properties Configuration Constants
     */
    String JDBC_DRIVER = "jdbc.driver";
    String JDBC_DATASOURCE_SIZE = "jdbc.datasource.size";
    String JDBC_URL = "jdbc.url";
    String JDBC_USER = "jdbc.user";
    String JDBC_PASSWORD = "jdbc.password";

    String SPARK_LOCAL = "spark.local";

    /**
     * Task Constants
     */
    String PARAM_SAMPLE_TYPE = "sampleType";
    String PARAM_SESSION_RATIO = "sessionRatio";
    String PARAM_START_DATE = "startDate";
    String PARAM_END_DATE = "endDate";
    String PARAM_START_AGE = "startAge";
    String PARAM_END_AGE = "endAge";
    String PARAM_PROFESSIONALS = "professionals";
    String PARAM_CITIES = "cities";
    String PARAM_SEX = "sex";
    String PARAM_KEYWORDS = "keywords";
    String PARAM_CATEGORY_IDS = "categoryIds";
    String FIELD_VISIT_LENGTH = "visitLength";
    String FIELD_STEP_LENGTH = "stepLength";
    String FIELD_START_TIME = "startTime";


}
