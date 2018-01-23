package com.batian.bigdata.plant.dao.impl;

import com.batian.bigdata.plant.dao.ITaskDao;
import com.batian.bigdata.plant.domain.Task;
import com.batian.bigdata.plant.jdbc.JDBCHelper;

import java.sql.ResultSet;

/**
 * Created by Ricky on 2018/1/19
 * 任务管理DAO实现类
 * @author Tomcox
 */
public class TaskDAOImpl implements ITaskDao {

    /**
     * 根据主键查询任务
     * @param taskid 主键
     * @return 任务
     */
    @Override
    public Task findByTaskId(long taskid) {
        final Task task = new Task()
                ;

        String sql = " select * from tb_task where task_id = ？";
        Object[] params = new Object[]{taskid};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeQuery(sql, params,
                //Callbase Function
                new JDBCHelper.QueryCallBack() {
                    @Override
                    public void process(ResultSet rs) throws Exception {
                        if (rs.next()) {
                            long taskId = rs.getLong(1);
                            String taskName = rs.getString(2);
                            String createName = rs.getString(3);
                            String startTime = rs.getString(4);
                            String finishTime = rs.getString(5);
                            String taskType = rs.getString(6);
                            String taskStatus = rs.getString(7);
                            String taskParam = rs.getString(8);

                            task.setTaskId(taskId);
                            task.setCreateTime(createName);
                            task.setTaskName(taskName);
                            task.setStartTime(startTime);
                            task.setFinishTime(finishTime);
                            task.setTaskType(taskType);
                            task.setTaskStatus(taskStatus);
                            task.setTaskParam(taskParam);
                        }
                    }
                });

        return task;
    }
}
