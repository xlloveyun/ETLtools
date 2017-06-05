package com.esgyn.dataloader;

import java.sql.ResultSet;

public interface ISource{

	public ResultSet readFromDB();
	
	public void readFromFile();
	
	public void readFromMQ();
	
	
}
