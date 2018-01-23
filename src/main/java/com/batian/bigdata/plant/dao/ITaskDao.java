package com.batian.bigdata.plant.dao;

import com.batian.bigdata.plant.domain.Task;

/**
 * Created by Ricky on 2018/1/19
 * 任务管理DAO接口
 * @author Tomcox
 */
public interface ITaskDao {
    /**
     * 根据主键查询任务
     */
    Task findByTaskId(long taskid);
}
