package com.esgyn.dataloader.impl;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import com.esgyn.dataloader.ISource;
import com.esgyn.tools.DBUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SourceImpl implements ISource {
	private BlockingQueue<StringBuilder> queue = null;
	private static Logger logger = Logger.getLogger(SourceImpl.class);
	public SourceImpl(BlockingQueue<StringBuilder> queue) {
		this.queue = queue;
	}

	public SourceImpl() {
	}

	public static void main(String[] args) {
		BlockingQueue<StringBuilder> queue = new ArrayBlockingQueue<StringBuilder>(100);
		SourceImpl source = new SourceImpl(queue);
		/*source.readFromDBToDB();*/
		System.out.println("********************finished!****************************");
	}

	public void readFromDBToFile(Properties prop) {
		try {
			if (prop==null) {
				prop =DBUtil.readProperties();
			}
			Statement selectPs = null;
			ResultSet rs = null;
			Connection selectConn = null;
			String selectTable = prop.getProperty("select.table");
			String selectQuery = "select * from " + selectTable +"limit 10000";
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
					insertCols += node.get(col)+",";
				}else{
					insertCols += node.get(col) + ")";
				}
				
				selectColNames.add(col);
				if (it.hasNext()) {
					selectCols+="?,";
				}else{
					selectCols+="?)";
				}
			}
			StringBuilder sb = new StringBuilder();
			selectConn = getConection(prop);
			selectPs=selectConn.createStatement();
			rs = selectPs.executeQuery(selectQuery);
			while (rs.next()) {
				int colCount = rs.getMetaData().getColumnCount();
				String colName = "";
				for (int i = 1; i <= colCount; i++) {
					colName = rs.getMetaData().getColumnName(i);
					Object val = rs.getObject(i);
					sb.append(colName + "=" + val + "; ");
				}
				sb.append("\n");
			}
			System.out.println(sb);
			queue.put(sb);
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
	}
	public List<Object> readFromDBToDB(Properties prop){
		Statement selectPs = null;
		ResultSet rs = null;
		Connection selectConn = null;
		List<Object> list = new ArrayList<Object>();
		try {
			if (prop==null) {
				prop =DBUtil.readProperties();
			}
			String selectTable = prop.getProperty("select.table");
			String selectQuery = "select * from " + selectTable;
			selectConn = getConection(prop);
			selectPs = selectConn.createStatement();
			rs = selectPs.executeQuery(selectQuery);
		} catch (SQLException e) {
			logger.error(e);
			e.printStackTrace();
		}
		list.add(selectConn);
		list.add(selectPs);
		list.add(rs);
		return list;
	}
	@SuppressWarnings("resource")
	private Connection getConection(Properties prop) {
		Connection selectConn = null;
		if (prop==null) {
			prop = DBUtil.readProperties();
		}
		try {
			String selectUrl = prop.getProperty("jdbc.select.url");
			String selectUsr = prop.getProperty("select.user");
			String selectPwd = prop.getProperty("select.pwd");
			String driver = prop.getProperty("select.jdbc.driver");
			File file = new File(prop.getProperty("select.driver.path"));
			URLClassLoader loader;
			loader = new URLClassLoader(new URL[] { file.toURI().toURL() });
			Object clazz = loader.loadClass(driver).newInstance();
			Driver myDriver = (Driver) clazz;

			Properties Obj = new Properties();
			Obj.setProperty("user", selectUsr);
			Obj.setProperty("password", selectPwd);
			selectConn = myDriver.connect(selectUrl, Obj);
		} catch (Exception e) {
			logger.error(e);
			e.printStackTrace();
		}
		return selectConn;
	}

	public void readFromFile() {
		System.out.println("here we are reading from file.");
	}

	public void readFromMQ() {
		System.out.println("here we are reading from MQ.");
	}
}
