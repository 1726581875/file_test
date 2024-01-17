package com.moyu.xmz.jdbc;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * @author xiaomingzhang
 * @date 2023/9/18
 */
public class Driver implements java.sql.Driver {


    static {
        // 注册驱动
        try {
            DriverManager.registerDriver(new Driver());
        } catch (SQLException var1) {
            throw new RuntimeException("Can't register driver!");
        }
    }


    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        return new ConnectionImpl(url);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return false;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }
}
