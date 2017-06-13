/*package com.esgyn.dataloader.impl;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import org.apache.log4j.Logger;

import com.esgyn.dataloader.ISource;
import com.esgyn.dataloader.ITarget;
import com.esgyn.tools.DBUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class PartitionDBSourceImpl implements ISource {
	private static Logger logger = Logger.getLogger(PartitionDBSourceImpl.class);
	private ITarget target=null;
	private Properties config=null;
	private ResultSet rs =null;
	private Connection selectConn =null;
	private Statement selectPs =null;

	public PartitionDBSourceImpl(Properties prop,ITarget target) {
		if (prop==null) {
			this.config=DBUtil.readProperties();
		}else{
			this.config=prop;
		}
		this.target = target;
		selectConn = getConection();
		try {
			selectPs = selectConn.createStatement();
		} catch (SQLException e) {
			logger.error(e);
		}
	}
	@Override
	public void read(){
		try {
			String selectTable = config.getProperty("select.table");
			String selectQuery = "select * from " + selectTable;
			rs = selectPs.executeQuery(selectQuery);
		} catch (SQLException e) {
			logger.error(e);
		}
	}
	@SuppressWarnings("resource")
	private Connection getConection() {
		Connection selectConn = null;
		try {
			String selectUrl = config.getProperty("jdbc.select.url");
			String selectUsr = config.getProperty("select.user");
			String selectPwd = config.getProperty("select.pwd");
			String driver = config.getProperty("select.jdbc.driver");
			File file = new File(config.getProperty("select.driver.path"));
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
		}
		return selectConn;
	}

	@Override
	public List<ColumnDesc> getColumns() {
		String columns = config.getProperty("mapping");
		ObjectMapper mapper = new ObjectMapper();
		JsonNode node;
		List<ColumnDesc> selectColNames = new ArrayList<ColumnDesc>();
		try {
			node = mapper.readTree(columns);
			Iterator<String> it = node.fieldNames();
			while (it.hasNext()) {
				String col = it.next().toString();
				ColumnDesc columDesc = new ColumnDesc(col); 
				selectColNames.add(columDesc);
			}
		} catch (JsonProcessingException e) {
			logger.error(e);
		} catch (IOException e) {
			logger.error(e);
		}
		return selectColNames;
	}

	@Override
	public void process() {
		this.read();
		List<ColumnDesc> selectCols = this.getColumns();
		int rowCount=0;
		int batchSize=0;
		try {
			while (rs.next()) {
				rowCount++;
				List<Object> cols = new ArrayList<Object>();
				for (int i = 0; i < selectCols.size(); i++) {
					cols.add(rs.getObject(selectCols.get(i).getColName()));
				}
				target.addLine(cols);
				if ((rowCount%batchSize)==0) {
					target.commit();
					logger.info("it has inserted " + rowCount + " lines data successfully!");
				}
			}
			if ((rowCount%batchSize)!=0) {
				target.commit();
				logger.info("it has inserted " + rowCount + " lines data successfully!");
			}
			rs.close();
			selectPs.close();
			selectConn.close();
		} catch (SQLException e) {
			logger.error(e);
		}finally{
			if (rs!=null) {
				try {
					rs.close();
				} catch (SQLException e) {
					logger.error(e);
				}
			}
			if (selectPs!=null) {
				try {
					selectPs.close();
				} catch (SQLException e) {
					logger.error(e);
				}
			}
			if (selectConn!=null) {
				try {
					selectConn.close();
				} catch (SQLException e) {
					logger.error(e);
				}
			}
		}
	}
}
*/