package com.batian.bigdata.plant.jdbc;

import com.batian.bigdata.plant.conf.ConfigurationManager;
import com.batian.bigdata.plant.constant.Constants;

import java.sql.*;
import java.util.LinkedList;

/**
 * JDBC辅助组件
 * @author Tomcox
 */
public class JDBCHelper {

    //1.在静态代码块中直接加载数据库的驱动

    static {
        try {
            String driver = ConfigurationManager.getProperty(Constants.JDBC_DRIVER);
            Class.forName(driver);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //2.实现JDBCHelper的单例化
    private static JDBCHelper instance = null;

    //获取单例
    public static JDBCHelper getInstance() {
        if (instance == null) {
            synchronized (JDBCHelper.class) {
                if (instance == null) {
                    instance = new JDBCHelper();
                }
            }
        }
        return instance;
    }

    //数据库连接池
    private LinkedList<Connection> datasource = new LinkedList<Connection>();

    /**
     3.实现单例的过程中，创建唯一的数据库连接池
     *
     */
    private JDBCHelper() {
        //首先需要获取数据库池的大小，就是说，数据库连接池中要放多少个数据库连接
        //可以通过配置文件中配置的方式来灵活设定
        int datasourceSize = ConfigurationManager.getInteger(Constants.JDBC_DATASOURCE_SIZE);

        //然后创建指定数量的数据库连接，并放入数据库连接池中
        for (int i = 0; i < datasourceSize; i++) {
            String url = ConfigurationManager.getProperty(Constants.JDBC_URL);
            String user = ConfigurationManager.getProperty(Constants.JDBC_USER);
            String password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);

            try {
                Connection conn = DriverManager.getConnection(url, user, password);
                datasource.push(conn);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }
    /**
     * 4.提供获取数据库连接的方式
     */
    public synchronized Connection getConnection() {
        while (datasource.size() == 0) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return datasource.poll();
    }

    /**
     * 关闭数据库连接
     */

    public synchronized void returnConnection(Connection conn) {
        if (conn != null) {
            try {
                if (conn.isClosed()) {
                    String url = ConfigurationManager.getProperty(Constants.JDBC_URL);
                    String user = ConfigurationManager.getProperty(Constants.JDBC_USER);
                    String password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
                    try {
                        Connection conn2 = DriverManager.getConnection(url, user, password);
                        datasource.push(conn2);
                    } catch (Exception e) {
                        // nothings
                    }
                }
            } catch (SQLException e) {
                //nothings
            }
            //正常情况直接添加
            datasource.push(conn);
        }
    }

    /**
     * 5.开发增删改查的方法
     */
    /**
     *     5.1执行增删改查SQL语句
     * @param sql
     * @param params
     * @return 影响的行数
     */
    public int executeUpdate(String sql, Object[] params) {
        int rtn = 0;
        Connection conn = null;
        PreparedStatement pstmt = null;

        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);

            for (int i = 0; i < params.length; i++) {
                pstmt.setObject(i + 1,params[i]);
            }
            rtn = pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            if (conn != null) {
                datasource.push(conn);
            }
        }
        return rtn;
    }

    /**
     * 执行查询SQL语句
     * @param sql
     * @param params
     * @param callBack
     */
    public void executeQuery(String sql, Object[] params, QueryCallBack callBack) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);
            for (int i = 0; i < params.length; i++) {
                pstmt.setObject(i + 1, params[i]);
            }

            rs = pstmt.executeQuery();

            callBack.process(rs);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if (conn != null)
                datasource.push(conn);
        }
    }

    /**
     * 静态内部类：查询回调接口
     */
    public static interface QueryCallBack {
        /**
         * 处理查询结果
         * @param rs
         * @throws Exception
         */
        void process(ResultSet rs) throws Exception;
    }
}
