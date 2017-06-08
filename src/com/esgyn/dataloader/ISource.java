package com.esgyn.dataloader;

import java.util.List;
import java.util.Properties;

public interface ISource{

	public void readFromDBToFile(Properties prop);
	
	public List<Object> readFromDBToDB(Properties prop);
	
	public void readFromFile();
	
	public void readFromMQ();
	
	
}
