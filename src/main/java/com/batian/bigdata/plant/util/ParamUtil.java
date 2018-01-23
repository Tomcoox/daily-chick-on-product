package com.batian.bigdata.plant.util;

import com.alibaba.fastjson.JSONObject;
import com.batian.bigdata.plant.domain.Task;
import scala.Option;


/**
 * Created by Ricky on 2018/1/19
 *
 * @author Tomcox
 */
public class ParamUtil {

    /**
     * 从命令行参数中提取任务id
     * @param args 命令行参数
     * @return 任务id
     */

    public static Long getTaskIdFromArgs(String[] args) {
        try {
            if (args != null && args.length > 0) {
                return Long.valueOf(args[0]);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 依据TaskId 获取参数
     * @param task
     * @return
     */
    public static JSONObject getTaskParam(Task task) {
        JSONObject taskParam = JSONObject.parseObject(task.getTaskName());
        if (taskParam != null && !taskParam.isEmpty()) {
            return taskParam;
        }
        return null;
    }


    /**
     * 从JSON对象中提取参数
     * Option是scala的，表示在JAVA中调用scala的代码
     * @param jsonObject JSON对象
     * @param field
     * @return 参数
     */
    public static Option<String> getParam(JSONObject jsonObject, String field) {
        String value = jsonObject.getString(field);
        if (value == null || value.trim().isEmpty()) {
            return Option.apply(null);
        } else {
            return Option.apply(value);
        }
    }
}
