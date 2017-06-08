import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.junit.Test;
import org.trafodion.jdbc.t4.TrafT4DataSource;

import com.esgyn.tools.DBUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class HikariTest {
	@Test
	public void testHikariTest(){
		TrafT4DataSource dataSource = new TrafT4DataSource();
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
		/*for (int i = 0; i < 4; i++) {
			try {
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}*/
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
