package com.esgyn.dataloader.impl;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.esgyn.dataloader.ISource;
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
	private ResultSet rs =null;
	private ISource	source = null;
	private List<ColumnDesc> selectCols = null;
	public DBTargetImpl(Properties prop,ISource source) throws Exception {
		this.prop=prop;
		this.source=source;
		try {
			selectCols = source.getColumns();
			this.getColumns();
			if (!selectCols.isEmpty()) {
				insertConn = this.getInsertConnection();
				insertPs = insertConn.prepareStatement(insertQuery);
			}
		} catch (SQLException e) {
			logger.error(e);
			try {
				if (e.getMessage().contains(prop.getProperty("insert.table") + " does not exist or is inaccessible")) {
					String createQuery = this.getCreateQuery(selectCols);
					insertPs = insertConn.prepareStatement(createQuery);
					int isCreated = insertPs.executeUpdate();
					if (isCreated==0) {
						insertPs = insertConn.prepareStatement(insertQuery);
					}else{
						throw new Exception("table" + prop.getProperty("insert.table")+ "could not be auto created!");
					}
				}
			} catch (SQLException e1) {
				logger.error(e);
			}
		}
	}
	@Override
	public void process() {
		rs = source.read();
		int rowCount=0;
		int batchSize=1000;
		try {
			while (rs.next()) {
				rowCount++;
				List<Object> cols = new ArrayList<Object>();
				for (int i = 0; i < selectCols.size(); i++) {
					cols.add(rs.getObject(selectCols.get(i).getColName()));
				}
				this.addLine(cols);
				batchSize=Integer.parseInt(prop.getProperty("batch.size"));
				if ((rowCount%batchSize)==0) {
					this.commit();
					logger.info("it has inserted " + rowCount + " lines data successfully!");
				}
			}
			if ((rowCount%batchSize)!=0) {
				this.commit();
				logger.info("it has inserted " + rowCount + " lines data successfully!");
			}
		} catch (SQLException e) {
			logger.error(e);
		}finally{
			source.closeConnection();
			if (rs!=null) {
				try {
					rs.close();
				} catch (SQLException e) {
					logger.error(e);
				}
			}
			if (insertPs!=null) {
				try {
					insertPs.close();
				} catch (SQLException e) {
					logger.error(e);
				}
			}
			if (insertConn!=null) {
				try {
					insertConn.close();
				} catch (SQLException e) {
					logger.error(e);
				}
			}
		}
	}
	private String getCreateQuery(List<ColumnDesc> cols) {
		String createQuery = "create table " +  prop.getProperty("insert.table");
		String createColStr = "(";
		for (Iterator iterator = cols.iterator(); iterator.hasNext();) {
			ColumnDesc colum = (ColumnDesc) iterator.next();
			if (iterator.hasNext()) {
				createColStr+=colum.getColName() +" "+ colum.getDataType()+ ",";
			}else{
				createColStr+=colum.getColName() +" "+ colum.getDataType()+ ")";
			}
		}
		createQuery +=createColStr;
		return createQuery;
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
			insertPs.clearBatch();
		} catch (SQLException e) {
			logger.error(e);
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
			logger.error(e);
		} catch (InstantiationException e) {
			logger.error(e);
		} catch (IllegalAccessException e) {
			logger.error(e);
		} catch (ClassNotFoundException e) {
			logger.error(e);
		} catch (SQLException e) {
			logger.error(e);
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
				ColumnDesc colDesc = new ColumnDesc(col,"");
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
			logger.error(e);
		} catch (IOException e) {
			logger.error(e);
		}
		return insertCols;
	}
	public void closeConnection(){
		try {
			this.insertConn.close();
			this.insertPs.close();
		} catch (SQLException e) {
			logger.error(e);
		}finally{
			if (this.insertConn!=null) {
				try {
					this.insertConn.close();
				} catch (SQLException e) {
					logger.error(e);
				}
			}
			if (this.insertPs!=null) {
				try {
					this.insertPs.close();
				} catch (SQLException e) {
					logger.error(e);
				}
			}
		}
	}
}
