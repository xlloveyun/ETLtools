import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class EtlDemo {
	private static Connection selectConn=null;
	private static Connection insertConn=null;
	private static Statement selectPs=null;
	private static PreparedStatement insertPs=null;
	private static ResultSet rs=null;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		Class.forName("org.trafodion.jdbc.t4.T4Driver");
	}
	@AfterClass
	public static void setUpAfterClass() throws Exception {
		if (rs!=null) {
			rs.close();
		}
		if (selectPs!=null) {
			selectPs.close();
		}
		if (insertPs!=null) {
			insertPs.close();
		}
		if (selectConn!=null) {
			selectConn.close();
		}
		if (insertConn!=null) {
			insertConn.close();
		}
	}
	@Test
	public void testEsgynToFile(){
		long start = System.currentTimeMillis();
		BufferedWriter bw=null;
		try
		{
		bw = new BufferedWriter(new FileWriter("dump.txt"));
		Properties prop = new Properties();
		prop.load(ClassLoader.getSystemResource("db.properties").openStream());
		String selectUrl = prop.getProperty("jdbc.select.url");
		String selectUsr = prop.getProperty("select.user");
		String selectPwd = prop.getProperty("select.pwd");
		String selectQuery = prop.getProperty("select.query");	
		selectConn=DriverManager.getConnection(selectUrl, selectUsr, selectPwd);
		selectPs=selectConn.createStatement();
		rs = selectPs.executeQuery(selectQuery);
		while ( rs.next())  
		{  
		int colCount = rs.getMetaData().getColumnCount();
		String colName="";
		for (int i = 0; i < colCount; i++) {
			colName = rs.getMetaData().getColumnName(i);
			Object val=rs.getObject(i);
			System.out.println(colName + "=" + val + "; ");
		}
		/*String meterId = rs.getString("METER_ID");
		String dataDate = rs.getString("DATA_DATE");
		String dayEneId = rs.getString("DAY_ENE_ID");
		String meteringTime = rs.getString("METERING_TIME");
		String papE = rs.getString("PAP_E");
		String papE1 = rs.getString("PAP_E1");
		String papE2 = rs.getString("PAP_E2");
		String papE3 = rs.getString("PAP_E3");
		String papE4 = rs.getString("PAP_E4");*/
		/*bw.write("meterId = " + meterId + "; dataDate = " + dataDate + "; dayEneId = " + dayEneId + "; meteringTime = " + meteringTime + 
				"; papE = " + papE + "; papE1 = " + papE1 + "; papE2 = " + papE2 + "; papE3 = " + papE3 + "; papE4 = " + papE4 + "\n");
				*/
		}
		long elapsedTimeMillis = System.currentTimeMillis()-start;
		float elapsedTimeMin = elapsedTimeMillis/(60*1000F);
		float elapsedTimeSec = (elapsedTimeMillis%(60*1000F))/1000F;
		System.out.println("the total time cost: " + elapsedTimeMin + " minutes; " + elapsedTimeSec + " seconds");
		bw.write("the total time cost: " + elapsedTimeMin + " minutes; " + elapsedTimeSec + " seconds");
		bw.flush();
		bw.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	@Test
	public void testEsgynToEsgyn(){
		// Get current time
		long start = System.currentTimeMillis();
		try
		{
		Properties prop = new Properties();
		prop.load(ClassLoader.getSystemResource("db.properties").openStream());
		String selectUrl = prop.getProperty("jdbc.select.url");
		String selectUsr = prop.getProperty("select.user");
		String selectPwd = prop.getProperty("select.pwd");
		String insertUrl = prop.getProperty("jdbc.select.url");
		String insertUsr = prop.getProperty("select.user");
		String insertPwd = prop.getProperty("select.pwd");
		String selectQuery = prop.getProperty("select.query");	
		String insertQuery = prop.getProperty("insert.query");
		int batchSize = Integer.parseInt(prop.getProperty("batch.size"));
		selectConn=DriverManager.getConnection(selectUrl, selectUsr, selectPwd);
		insertConn=DriverManager.getConnection(insertUrl, insertUsr, insertPwd);
		selectPs=selectConn.createStatement();
		insertPs=insertConn.prepareStatement(insertQuery);
		rs = selectPs.executeQuery(selectQuery);
		int count=0;
		while ( rs.next())  
		{  
		String meterId = rs.getString("METER_ID");
		String dataDate = rs.getString("DATA_DATE");
		String dayEneId = rs.getString("DAY_ENE_ID");
		String meteringTime = rs.getString("METERING_TIME");
		String papE = rs.getString("PAP_E");
		String papE1 = rs.getString("PAP_E1");
		String papE2 = rs.getString("PAP_E2");
		String papE3 = rs.getString("PAP_E3");
		String papE4 = rs.getString("PAP_E4");
		insertPs.setString(1, meterId);       
		insertPs.setString(2, dataDate);
		insertPs.setString(3, dayEneId);          
		insertPs.setString(4, meteringTime);   
		insertPs.setString(5, papE);   
		insertPs.setString(6, papE1);   
		insertPs.setString(7, papE2);   
		insertPs.setString(8, papE3);   
		insertPs.setString(9, papE4);  
		insertPs.addBatch();
		count++;
		if ((count%batchSize)==0) {
			insertPs.executeBatch();
			System.out.println("batch inserted lines: " + count);
		}
		}
		insertPs.executeBatch();
		System.out.println("All the data have been inserted successfully!");
		rs.close();
		selectPs.close();
		insertPs.close();
		selectConn.close();
		insertConn.close();
		
		//time counter
		long elapsedTimeMillis = System.currentTimeMillis()-start;
		// Get elapsed time in seconds
		float elapsedTimeSec = elapsedTimeMillis/1000F;
		// Get elapsed time in minutes
		float elapsedTimeMin = elapsedTimeMillis/(60*1000F);
		System.out.println("the total time cost: " + elapsedTimeMin + "minutes; " + elapsedTimeSec + "seconds");
		}
		catch (SQLException | IOException e)
		{
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
			if (insertPs!=null) {
				try {
					insertPs.close();
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
