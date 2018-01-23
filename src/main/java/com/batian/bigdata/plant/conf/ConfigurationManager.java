package com.batian.bigdata.plant.conf;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by Ricky on 2018/1/19
 *
 * @author Tomcox
 */
public class ConfigurationManager {

    //create properties
    private static Properties prop = new Properties();

    //static code block ,load configuration properties file
    static {
        try {
            //create InputStream
            InputStream in = ConfigurationManager.class
                    .getClassLoader().getResourceAsStream("usertrack.properties");
            //load properties
            prop.load(in);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * get value by key
     * @param key
     * @return
     */
    public static String getProperty(String key) { return prop.getProperty(key);}


    /**
     * get Integer By Key
     * @param key
     * @return value
     */
    public static Integer getInteger(String key) {
        String value = getProperty(key);
        try {
            return Integer.valueOf(value);
        }catch (Exception e ) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * get Boolean by key
     * @param key
     * @return
     */
    public static Boolean getBoolean(String key) {
        String value = getProperty(key);
        try {
            return Boolean.valueOf(value);
        }catch (Exception e ) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * get Long By KEY
     * @param key
     * @return
     */
    public static Long getLong(String key) {
        String value = getProperty(key);
        try {
            return Long.valueOf(value);
        }catch (Exception e ) {
            e.printStackTrace();
        }
        return 0L;
    }


}
