package com.batian.bigdata.plant.dao.factory;

import com.batian.bigdata.plant.dao.ITaskDao;
import com.batian.bigdata.plant.dao.impl.TaskDAOImpl;

/**
 * Created by Ricky on 2018/1/19
 * DAO Factory class
 * @author Tomcox
 */
public class DAOFactory {

    /**
     * 获取任务管理DAO
     * @return DAO
     */

    public static ITaskDao getTaskDAO() {
        return new TaskDAOImpl();
    }
}
