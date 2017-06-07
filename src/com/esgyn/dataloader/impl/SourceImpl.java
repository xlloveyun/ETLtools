package com.esgyn.dataloader.impl;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.esgyn.dataloader.ISource;

public class SourceImpl implements ISource {
	private BlockingQueue<StringBuilder> queue = null;

	public SourceImpl(BlockingQueue<StringBuilder> queue) {
		this.queue = queue;
	}

	public SourceImpl() {
	}

	public static void main(String[] args) {
		BlockingQueue<StringBuilder> queue = new ArrayBlockingQueue<StringBuilder>(100);
		SourceImpl source = new SourceImpl(queue);
		source.readFromDBToDB();
		System.out.println("********************finished!****************************");
	}

	public void readFromDBToFile() {
		/*
		 * try{ StringBuilder sb = new StringBuilder(); ResultSet rs =
		 * getResultSet(); while (rs.next()) { int colCount =
		 * rs.getMetaData().getColumnCount(); String colName = ""; for (int i =
		 * 1; i <= colCount; i++) { colName = rs.getMetaData().getColumnName(i);
		 * Object val = rs.getObject(i); sb.append(colName + "=" + val + "; ");
		 * } sb.append("\n"); } System.out.println(sb); queue.put(sb); } catch
		 * (SQLException e) { // TODO Auto-generated catch block
		 * e.printStackTrace(); } catch (InterruptedException e) { // TODO
		 * Auto-generated catch block e.printStackTrace(); }
		 */
	}

	@SuppressWarnings("resource")
	public List<Object> readFromDBToDB() {
		Connection selectConn = null;
		Statement selectPs = null;
		ResultSet rs = null;
		Properties prop = new Properties();
		List<Object> list = new ArrayList<Object>();
		try {
			prop.load(ClassLoader.getSystemResource("db.properties").openStream());
			String selectUrl = prop.getProperty("jdbc.select.url");
			String selectUsr = prop.getProperty("select.user");
			String selectPwd = prop.getProperty("select.pwd");
			String selectQuery = prop.getProperty("select.test.query");
			String driver = prop.getProperty("jdbc.driver");
			File file = new File(prop.getProperty("driver.path"));
			URLClassLoader loader;
			loader = new URLClassLoader(new URL[] { file.toURI().toURL() });
			Object clazz = loader.loadClass(driver).newInstance();
			Driver myDriver = (Driver) clazz;

			Properties Obj = new Properties();
			Obj.setProperty("user", selectUsr);
			Obj.setProperty("password", selectPwd);
			selectConn = myDriver.connect(selectUrl, Obj);
			selectPs = selectConn.createStatement();
			rs = selectPs.executeQuery(selectQuery);
			list.add(selectConn);
			list.add(selectPs);
			list.add(rs);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	public void readFromFile() {
		System.out.println("here we are reading from file.");
	}

	public void readFromMQ() {
		System.out.println("here we are reading from MQ.");
	}
}
