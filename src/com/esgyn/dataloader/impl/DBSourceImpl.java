package com.esgyn.dataloader.impl;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
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
import com.esgyn.tools.SQLTypeMap;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DBSourceImpl implements ISource {
	private static Logger logger = Logger.getLogger(DBSourceImpl.class);
	private Properties config=null;
	private Connection selectConn =null;
	private Statement selectPs =null;

	public DBSourceImpl(Properties prop) {
		if (prop==null) {
			this.config=DBUtil.readProperties();
		}else{
			this.config=prop;
		}
		selectConn = getConection();
		try {
			selectPs = selectConn.createStatement();
		} catch (SQLException e) {
			logger.error(e);
		}
	}
	@Override
	public ResultSet read(){
		ResultSet rs = null;
		try {
			String selectTable = config.getProperty("select.table");
			String selectQuery = "select * from " + selectTable;
			rs = selectPs.executeQuery(selectQuery);
		} catch (SQLException e) {
			logger.error(e);
		}
		return rs;
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
			DatabaseMetaData metaData = selectConn.getMetaData();
			node = mapper.readTree(columns);
			Iterator<String> it = node.fieldNames();
			while (it.hasNext()) {
				String col = it.next().toString();
				String tableStr=config.getProperty("select.table").toUpperCase();
				String tableName = tableStr.substring(tableStr.lastIndexOf(".") +1);
				String catalog = tableStr.substring(0,tableStr.indexOf("."));
				String schema = tableStr.substring(tableStr.indexOf(".")+1, tableStr.lastIndexOf("."));
				ResultSet metaRs = metaData.getColumns(catalog, schema, tableName, col);
				if( metaRs.next() ){
					int intType = metaRs.getInt( "DATA_TYPE" );
					String databaseType = SQLTypeMap.convert( intType );
					ColumnDesc columDesc = new ColumnDesc(col,databaseType); 
					selectColNames.add(columDesc);
				}
			}
		} catch (JsonProcessingException e) {
			logger.error(e);
		} catch (IOException e) {
			logger.error(e);
		} catch (SQLException e) {
			logger.error(e);
		}
		return selectColNames;
	}
	@Override
	public void closeConnection() {
		try {
			this.selectPs.close();
			this.selectConn.close();
		} catch (SQLException e) {
			logger.error(e);
		}
		
	}
}
