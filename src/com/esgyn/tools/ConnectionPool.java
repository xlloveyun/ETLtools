/*package com.esgyn.tools;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;

import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggerFactory;

import com.esgyn.dataloader.impl.TargetImpl;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class ConnectionPool {
	private static Logger _LOG = Logger.getLogger(ConnectionPool.class); // 用Log4J来记录日志
    private static HikariDataSource pool;  //定义第一个DataSource,用的是com.zaxxer.hikari.HikariDataSource
    private static HikariDataSource pool1;//定义第二个DataSource,用的是com.zaxxer.hikari.HikariDataSource
   //在构造函数中对DataSource进行配置，读取的是.properties文件。
    private ConnectionPool() {
        HikariConfig config = new HikariConfig("hikari.properties");
        HikariConfig config1 = new HikariConfig("hikari1.properties");
        pool = new HikariDataSource(config);
        pool1 = new HikariDataSource(config1);
    }
   //得到第一个Instance.
    public synchronized static HikariDataSource getInstance() {
        if (pool == null ) {
            try {
                if (pool == null) {
                    new ConnectionPool();
                }
            } catch (Exception e) {
                _LOG.error(e.getMessage(), e);
            }
        }
        return pool;
    }
    //得到第二个Instance.
    public synchronized static HikariDataSource getInstance1() {
        if (pool1 == null ) {
            try {
                if (pool1 == null) {
                    new ConnectionPool();
                }
            } catch (Exception e) {
                _LOG.error(e.getMessage(), e);
            }
        }
        return pool1;
    }
}*/