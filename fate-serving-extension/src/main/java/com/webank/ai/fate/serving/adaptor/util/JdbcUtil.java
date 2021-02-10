package com.webank.ai.fate.serving.adaptor.util;

import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class JdbcUtil {

    private static DataSource dataSource; // 用静态属性保存唯一的一个连接池对象.
    public static Connection getConnection() throws SQLException {
        if (dataSource == null) { // 只有第一次获取连接时才创建连接池对象
            try {
                Properties properties = new Properties();
                InputStream in = JdbcUtil.class.getClassLoader().getResourceAsStream("druid.properties");
                properties.load(in);
                in.close();
                // 直接通过工厂来自动读取配置文件中的配置信息, 并创建连接池对象.
                dataSource = DruidDataSourceFactory.createDataSource(properties);
            } catch (Exception e) {
                throw new SQLException(e); // 异常的转型, 使得之前的代码不需要再修改即可用这个新方法.
            }
        }
        Connection connection = dataSource.getConnection();
        return connection;
    }



    public static void close(Connection connection) {
        close(connection, null);
    }

    public static void close(Connection connection, Statement statement) {
        close(connection, statement, null);
    }

    public static void close(Connection connection, Statement statement, ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }

        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }

        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }

    }
}
