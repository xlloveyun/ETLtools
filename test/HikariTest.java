import java.beans.PropertyVetoException;
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

import org.apache.log4j.Logger;
import org.junit.Test;

import com.esgyn.tools.DBUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class HikariTest {
	private static Logger logger = Logger.getLogger(HikariTest.class);
	@Test
	public void testHikariTest(){
		/*TrafT4DataSource dataSource = new TrafT4DataSource();
		Properties prop = DBUtil.readProperties();
		Connection conn=null;
		Statement st = null;
		ResultSet rs = null;
		dataSource.setApplicationName("ETL TOOLS");
		dataSource.setUrl(prop.getProperty("jdbc.select.url"));
		dataSource.setMaxIdleTime("10000");
		dataSource.setMaxStatements("5");
		dataSource.setMaxPoolSize("1");
		dataSource.setUser("trafodion");
		dataSource.setPassword("traf123");
		dataSource.setMinPoolSize("1");
		dataSource.setInitialPoolSize("1");
		try {
			conn = dataSource.getConnection();
			st = conn.createStatement();
			rs = st.executeQuery("select * from demo");
			while (rs.next()) {
				System.out.println("id = " + rs.getString("ID") + "NAME=" + rs.getString("NAME"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		for (int i = 0; i < 4; i++) {
			try {
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}*/
	}
	@SuppressWarnings("resource")
	@Test
	public void testHIkari(){
		Properties prop = null;
		Connection selectConn = null;
		Statement selectSt = null;
		ResultSet rs =null;
		if (prop==null) {
			prop = DBUtil.readProperties();
		}
		try {
			String selectUrl = prop.getProperty("jdbc.select.url");
			String selectUsr = prop.getProperty("select.user");
			String selectPwd = prop.getProperty("select.pwd");
			String driverName = prop.getProperty("select.jdbc.driver");
			File file = new File(prop.getProperty("select.driver.path"));
			URLClassLoader loader;
			loader = new URLClassLoader(new URL[] { file.toURI().toURL() });
			Driver driver = (Driver) loader.loadClass(driverName).newInstance();
			DriverManager.registerDriver(driver);

			HikariConfig config = new HikariConfig();
			config.setJdbcUrl(selectUrl);
			config.setUsername(selectUsr);
			config.setPassword(selectPwd);
			config.addDataSourceProperty("cachePrepStmts", "true");
			config.addDataSourceProperty("prepStmtCacheSize", "5");
			config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
			config.addDataSourceProperty("maximumPoolSize", "2");

			HikariDataSource ds = new HikariDataSource(config);
			selectConn = ds.getConnection();
			selectSt= selectConn.createStatement();
			rs=selectSt.executeQuery("select * from trafodion.nari.E_MP_DAY_ENERGY_P limit 100");
			while ( rs.next())  
			{  
			int colCount = rs.getMetaData().getColumnCount();
			String colName="";
			for (int i = 0; i < colCount; i++) {
				colName = rs.getMetaData().getColumnName(i);
				Object val=rs.getObject(i);
				System.out.println(colName + "=" + val + "; ");
			}
			}
		} catch (Exception e) {
			logger.error(e);
			e.printStackTrace();
		}
	}
	@SuppressWarnings("resource")
	@Test
	public void testC3p0(){
		Properties prop = DBUtil.readProperties();
		Connection selectConn = null;
		Statement selectSt = null;
		ResultSet rs =null;
		try {
			String selectUrl = prop.getProperty("jdbc.select.url");
			String selectUsr = prop.getProperty("select.user");
			String selectPwd = prop.getProperty("select.pwd");
			String driverName = prop.getProperty("select.jdbc.driver");
			File file = new File(prop.getProperty("select.driver.path"));
			URLClassLoader loader;
			loader = new URLClassLoader(new URL[] { file.toURI().toURL() });
			Driver driver = (Driver) loader.loadClass(driverName).newInstance();
			DriverManager.registerDriver(driver);
			
			
			ComboPooledDataSource ds = new ComboPooledDataSource();
            ds.setDriverClass(driverName);
            ds.setJdbcUrl(selectUrl);
            ds.setUser(selectUsr);
            ds.setPassword(selectPwd);
            selectConn = ds.getConnection();
           /* Properties Obj = new Properties();
			Obj.setProperty("user", selectUsr);
			Obj.setProperty("password", selectPwd);
			selectConn = driver.connect(selectUrl, Obj);*/
            selectSt= selectConn.createStatement();
			rs=selectSt.executeQuery("select * from trafodion.nari.E_MP_DAY_ENERGY_P limit 10");
            while ( rs.next())  
			{  
			int colCount = rs.getMetaData().getColumnCount();
			String colName="";
			for (int i = 1; i <= colCount; i++) {
				colName = rs.getMetaData().getColumnName(i);
				Object val=rs.getObject(i);
				System.out.println(colName + "=" + val + "; ");
			}
			}
            rs.close();
            selectSt.close();
            selectConn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (PropertyVetoException e) {
            e.printStackTrace();
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
		}finally{
			if (rs!=null) {
				try {
					rs.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (selectSt!=null) {
				try {
					selectSt.close();
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
		}
	}
	@Test
	public void testJson(){
		Properties prop = DBUtil.readProperties();
		Connection selectConn = null;
		Connection insertConn = null;
		Statement selectSt = null;
		PreparedStatement insertPs = null;
	    ResultSet rs = null;
		try {
			Class.forName(prop.getProperty("select.jdbc.driver"));
			selectConn = DriverManager.getConnection(prop.getProperty("jdbc.select.url"),prop.getProperty("select.user"),prop.getProperty("select.pwd"));
			insertConn = DriverManager.getConnection(prop.getProperty("jdbc.insert.url"),prop.getProperty("insert.user"),prop.getProperty("insert.pwd"));
			selectSt = selectConn.createStatement();
			String selectTable = prop.getProperty("select.table");
			rs = selectSt.executeQuery("select * from " + selectTable);
			String columns = prop.getProperty("mapping");
			String insertTable = prop.getProperty("insert.table");
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
			System.out.println(selectCols);
			System.out.println(insertCols);
			insertQuery = "insert into " + insertTable + insertCols + " values" + selectCols;
			System.out.println(insertQuery);
			insertPs = insertConn.prepareStatement(insertQuery);
			while (rs.next()) {
				for (int i = 1; i <= selectColNames.size(); i++) {
					insertPs.setObject(i, rs.getObject(selectColNames.get(i-1)));
				}
			}
			insertPs.executeUpdate();
			rs.close();
			selectSt.close();
			insertPs.close();
			selectConn.close();
			insertConn.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
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
			if (selectSt!=null) {
				try {
					selectSt.close();
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
}
