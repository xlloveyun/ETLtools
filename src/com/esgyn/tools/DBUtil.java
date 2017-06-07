package com.esgyn.tools;

import java.io.IOException;
import java.util.Properties;

public class DBUtil {
	public static Properties readProperties(){
		Properties prop = new Properties();
		try {
			prop.load(ClassLoader.getSystemResource("db.properties").openStream());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return prop;
	}
}
