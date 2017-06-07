package com.esgyn.dataloader.impl;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import com.esgyn.dataloader.ITarget;
import com.esgyn.tools.DBUtil;

public class TargetImpl implements ITarget {
	private BlockingQueue<StringBuilder> queue = null;
	private static Logger logger = Logger.getLogger(TargetImpl.class);

	public TargetImpl(BlockingQueue<StringBuilder> queue) {
		this.queue = queue;
	}
	public TargetImpl() {
	}

	@Override
	public void WriteTargetToFileFromDB() {
		try {
			logger.info(queue.take().toString());
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void WriteTargetToDBFromDB(List<Object> list) {
		// TODO Auto-generated method stub
		Connection insertConn=null;
		PreparedStatement insertPs = null;
		Connection selectConn=null;
		Statement selectPs = null;
		selectConn=(Connection) list.get(0);
		selectPs=(Statement) list.get(1);
		ResultSet rs = (ResultSet) list.get(2);
		try {
			Properties prop = DBUtil.readProperties();
			File file = new File(prop.getProperty("driver.path"));
			URLClassLoader loader;
			loader = new URLClassLoader(new URL[] { file.toURI().toURL() });
			Object clazz = loader.loadClass(prop.getProperty("jdbc.driver")).newInstance();
			Driver myDriver = (Driver) clazz;

			Properties Obj = new Properties();
			Obj.setProperty("user", prop.getProperty("insert.user"));
			Obj.setProperty("password", prop.getProperty("insert.pwd"));
			insertConn = myDriver.connect(prop.getProperty("jdbc.insert.url"), Obj);
			insertPs = insertConn.prepareStatement(prop.getProperty("insert.test.query"));
			int rowCount=0;
			Object colName=null;
			Object colVal=null;
			while (rs.next()) {
				rowCount++;
				for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
					colVal= rs.getObject(i);
					colName=rs.getMetaData().getColumnName(i);
					insertPs.setObject(i, colVal);
					System.out.println(colName + "=" + colVal + "; \n");
				}
				insertPs.addBatch();
				while ((rowCount%1000)==0) {
					insertPs.executeBatch();
					System.out.println("batch inserted lines: " + rowCount);
				}
			}
			if (rowCount!=0) {
				insertPs.executeBatch();
			}
			System.out.println("All the data have been inserted successfully!");
			rs.close();
			selectPs.close();
			selectConn.close();
			insertPs.close();
			insertConn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			if (rs!=null) {
				try {
					rs.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (selectPs!=null) {
				try {
					selectPs.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (selectConn!=null) {
				try {
					selectConn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (insertPs!=null) {
				try {
					insertPs.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (insertConn!=null) {
				try {
					insertConn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	public void WriteTargetToMQFromDB() {
		// TODO Auto-generated method stub

	}

	@Override
	public void WriteTargetToDBFromFile() {
		// TODO Auto-generated method stub

	}

	@Override
	public void WriteTargetToDBFromMQ() {
		// TODO Auto-generated method stub

	}
}
