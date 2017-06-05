package com.esgyn.source;

import java.sql.Connection;
import java.sql.ResultSet;

public interface ISource {

	Connection connection = null;
	
	void getConnection();
	
	public ResultSet getResultSet();
	
	
	
}
