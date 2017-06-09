package com.esgyn.dataloader.impl;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.esgyn.dataloader.ColumnDesc;
import com.esgyn.dataloader.ITarget;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DBTargetImpl implements ITarget {
	private static Logger logger = Logger.getLogger(DBTargetImpl.class);
	private String insertQuery = "";
	private Properties prop=null;
	private Connection insertConn = null;
	private PreparedStatement insertPs = null;
	public DBTargetImpl(Properties prop) {
		this.prop=prop;
		try {
			this.getColumns();
			insertConn = this.getInsertConnection();
			insertPs = insertConn.prepareStatement(insertQuery);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	@Override
	public void addLine(List<Object> cols) {
		try {
			for (int i = 0; i < cols.size(); i++) {
				insertPs.setObject(i+1, cols.get(i));
			}
			insertPs.addBatch();
		} catch (SQLException e) {
			logger.error(e);
		}
	}

	@Override
	public Long commit() {
		try {
			insertPs.executeBatch();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	private Connection getInsertConnection(){
		Connection insertConn=null;
		File file = new File(prop.getProperty("insert.driver.path"));
		URLClassLoader loader;
		try {
			loader = new URLClassLoader(new URL[] { file.toURI().toURL() });
			Object clazz = loader.loadClass(prop.getProperty("insert.jdbc.driver")).newInstance();
			Driver myDriver = (Driver) clazz;
			Properties Obj = new Properties();
			Obj.setProperty("user", prop.getProperty("insert.user"));
			Obj.setProperty("password", prop.getProperty("insert.pwd"));
			insertConn = myDriver.connect(prop.getProperty("jdbc.insert.url"), Obj);
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return insertConn;
	}

	@Override
	public List<ColumnDesc> getColumns() {
		String columns = prop.getProperty("mapping");
		ObjectMapper mapper = new ObjectMapper();
		List<ColumnDesc> insertCols = new ArrayList<ColumnDesc>();
		JsonNode node;
		try {
			node = mapper.readTree(columns);
			Iterator<String> it = node.fieldNames();
			String insertColsStr="(";
			String selectColsStr = "(";
			while (it.hasNext()) {
				String col = it.next().toString();
				if (it.hasNext()) {
					insertColsStr += col+",";
				}else{
					insertColsStr += col + ")";
				}
				ColumnDesc colDesc = new ColumnDesc(col);
				insertCols.add(colDesc);
				if (it.hasNext()) {
					selectColsStr+="?,";
				}else{
					selectColsStr+="?)";
				}
			}
			String insertTable = prop.getProperty("insert.table");
			insertQuery = "upsert using load into " + insertTable + insertColsStr + " values" + selectColsStr;
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return insertCols;
	}
}
