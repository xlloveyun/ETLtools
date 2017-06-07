package com.esgyn.dataloader;

import java.sql.ResultSet;
import java.util.List;

public interface ISource{

	public void readFromDBToFile();
	
	public List<Object> readFromDBToDB();
	
	public void readFromFile();
	
	public void readFromMQ();
	
	
}
