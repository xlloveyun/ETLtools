package com.esgyn.dataloader.impl;

import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.esgyn.dataloader.IDataLoader;
import com.esgyn.dataloader.ISource;
import com.esgyn.dataloader.ITarget;

public class DataLoaderImpl implements IDataLoader {
	private Properties prop = null;
	private static Logger logger = Logger.getLogger(TargetImpl.class);
	
	public DataLoaderImpl(){
		
	}
	public DataLoaderImpl(Properties prop){
		this.prop= prop;
	}

	private String direction = "DB2DB";
	public static void main(String[] args){
		Properties prop = new Properties();
		DataLoaderImpl loader = new DataLoaderImpl(prop);
		loader.loadData();
	}
	public void loadData() {
		ISource source = new SourceImpl();
		List<Object> list = null;
		switch (direction) {
		case "DB2DB":
			list = source.readFromDBToDB(prop);
			ITarget target = new TargetImpl();
			target.WriteTargetToDBFromDB(list,prop);
			break;
		case "DB2File":
			break;
		case "File2File":
			break;
		}
	}

	@Override
	public void run() {
		long start = System.currentTimeMillis();
		this.loadData();
		java.text.DecimalFormat   df   =new   java.text.DecimalFormat("#"); 
		long elapsedTimeMillis = System.currentTimeMillis()-start;
		String elapsedTimeMin = df.format(elapsedTimeMillis/(60*1000F));
		float elapsedTimeSec = (elapsedTimeMillis%(60*1000F))/1000F;
		System.out.println("the total time cost: " + elapsedTimeMin + " minutes; " + elapsedTimeSec + " seconds");
	}
}
