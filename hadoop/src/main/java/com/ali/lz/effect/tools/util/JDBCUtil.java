package com.ali.lz.effect.tools.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * MySQL JDBC工具类
 * 
 * @author jiuling.ypf
 * 
 */
public class JDBCUtil {

    // 日志操作记录对象
    private static final Log LOG = LogFactory.getLog(JDBCUtil.class);

    // JDBC URL连接串（主库）
    private static String masterUrl = "jdbc:mysql://10.232.36.5:3306/pandora";

    // JDBC URL连接串（备库）
    private static String slaveUrl = "jdbc:mysql://10.232.36.5:3306/pandora";

    // JDBC Driver类名
    private static String className = "com.mysql.jdbc.Driver";

    // 数据库用户名
    private static String username = "root";

    // 数据库密码
    private static String password = "";

    /**
     * 静态加载
     */
    static {
        // 加载JDBC驱动
        try {
            try {
                Class.forName(className).newInstance();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        } catch (ClassNotFoundException e) {
            LOG.error("ClassNotFoundException: " + e);
            e.printStackTrace();
        }

        // 加载MySQL配置信息
        String[] dbServerIp = { "127.0.0.1", "127.0.0.1" };
        String[] dbServerIpItems = ConfigLoader.getMysqlServerIp().split(",");
        if (dbServerIpItems.length == 1) {
            dbServerIp[0] = dbServerIpItems[0];
            dbServerIp[1] = dbServerIpItems[0];
        } else {
            dbServerIp[0] = dbServerIpItems[0];
            dbServerIp[1] = dbServerIpItems[1];
        }
        int dbServerPort = ConfigLoader.getMysqlServerPort();
        String dbName = ConfigLoader.getMysqlDbName();
        String dbUser = ConfigLoader.getMysqlDbUser();
        String dbPass = ConfigLoader.getMysqlDbPass();
        masterUrl = "jdbc:mysql://" + dbServerIp[0] + ":" + dbServerPort + "/" + dbName;
        slaveUrl = "jdbc:mysql://" + dbServerIp[1] + ":" + dbServerPort + "/" + dbName;
        username = dbUser;
        password = dbPass;
        LOG.info("config mysql master url: " + masterUrl);
        LOG.info("config mysql slave  url: " + slaveUrl);
    }

    /**
     * 获取连接（默认与Master创建连接）
     * 
     * @return
     */
    public Connection getConnection() {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(masterUrl, username, password);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 获取连接
     * 
     * @param isMaster
     * @return
     */
    public Connection getConnection(boolean isMaster) {
        Connection conn = null;
        try {
            if (isMaster)
                conn = DriverManager.getConnection(masterUrl, username, password);
            else
                conn = DriverManager.getConnection(slaveUrl, username, password);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 关闭连接
     * 
     * @param conn
     */
    public void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 关闭PreparedStatement
     * 
     * @param pstm
     */
    public void closePreparedStatement(PreparedStatement pstm) {
        if (pstm != null) {
            try {
                pstm.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 关闭Statement
     * 
     * @param stm
     */
    public void closeStatement(Statement stm) {
        if (stm != null) {
            try {
                stm.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 关闭ResultSet
     * 
     * @param rs
     */
    public void closeResultSet(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}
