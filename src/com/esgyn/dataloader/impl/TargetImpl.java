package com.esgyn.dataloader.impl;

import java.io.File;
import java.io.IOException;
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import com.esgyn.dataloader.ITarget;
import com.esgyn.tools.DBUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

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
			e.printStackTrace();
		}
	}

	@SuppressWarnings("resource")
	@Override
	public void WriteTargetToDBFromDB(List<Object> list,Properties prop) {
		// TODO Auto-generated method stub
		Connection insertConn=null;
		PreparedStatement insertPs = null;
		Connection selectConn=null;
		Statement selectPs = null;
		selectConn=(Connection) list.get(0);
		selectPs=(Statement) list.get(1);
		ResultSet rs = (ResultSet) list.get(2);
		try {
			if (prop==null) {
				
				prop = DBUtil.readProperties();
			}
			File file = new File(prop.getProperty("insert.driver.path"));
			URLClassLoader loader;
			loader = new URLClassLoader(new URL[] { file.toURI().toURL() });
			Object clazz = loader.loadClass(prop.getProperty("insert.jdbc.driver")).newInstance();
			Driver myDriver = (Driver) clazz;

			Properties Obj = new Properties();
			Obj.setProperty("user", prop.getProperty("insert.user"));
			Obj.setProperty("password", prop.getProperty("insert.pwd"));
			insertConn = myDriver.connect(prop.getProperty("jdbc.insert.url"), Obj);
			String columns = prop.getProperty("mapping");
			ObjectMapper mapper = new ObjectMapper();
			JsonNode node = mapper.readTree(columns);
			Iterator<String> it = node.fieldNames();
			List<String> selectColNames = new ArrayList<String>();
			String insertCols="(";
			String selectCols = "(";
			String insertQuery = "";
			while (it.hasNext()) {
				String col = it.next().toString();
				if (it.hasNext()) {
					insertCols += col+",";
				}else{
					insertCols += col + ")";
				}
				
				selectColNames.add(col);
				if (it.hasNext()) {
					selectCols+="?,";
				}else{
					selectCols+="?)";
				}
			}
			
			String insertTable = prop.getProperty("insert.table");
			insertQuery = "upsert using load into " + insertTable + insertCols + " values" + selectCols;
			System.out.println("insertQuery=" + insertQuery);
			insertPs = insertConn.prepareStatement(insertQuery);
			int rowCount=0;
			int batchSize=1000;
			while (rs.next()) {
				rowCount++;
				for (int i = 1; i <= selectColNames.size(); i++) {
					insertPs.setObject(i, rs.getObject(node.get(selectColNames.get(i-1)).toString().replaceAll("\"","")));
				}
				insertPs.addBatch();
				if (!prop.getProperty("batch.size").equalsIgnoreCase("")) {
					batchSize=Integer.valueOf(prop.getProperty("batch.size"));
				}
				if ((rowCount%batchSize)==0) {
					insertPs.executeBatch();
					System.out.println("batch inserted lines: " + rowCount);
				}
			}
			if ((rowCount%batchSize)!=0) {
				insertPs.executeBatch();
			}
			System.out.println("All the data have been inserted successfully!");
			rs.close();
			selectPs.close();
			selectConn.close();
			insertPs.close();
			insertConn.close();
		} catch (SQLException e) {
			logger.error(e);
		} catch (ClassNotFoundException e) {
			logger.error(e);
		} catch (InstantiationException e) {
			logger.error(e);
		} catch (IllegalAccessException e) {
			logger.error(e);
		} catch (MalformedURLException e) {
			logger.error(e);
		} catch (JsonProcessingException e) {
			logger.error(e);
		} catch (IOException e) {
			logger.error(e);
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
