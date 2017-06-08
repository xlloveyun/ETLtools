package com.esgyn.tools;

import java.io.IOException;
import java.sql.Connection;
import java.util.Properties;

public class DBUtil {
	public static Properties readProperties(){
		Properties prop = new Properties();
		try {
			prop.load(ClassLoader.getSystemResource("file.properties").openStream());
		} catch (IOException e) {
			e.printStackTrace();
		}
		return prop;
	}
	public static Connection getConnection(){
		return null;
	}
}
