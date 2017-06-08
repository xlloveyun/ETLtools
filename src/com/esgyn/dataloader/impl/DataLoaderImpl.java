package com.esgyn.dataloader.impl;

import java.util.List;
import java.util.Properties;

import com.esgyn.dataloader.IDataLoader;
import com.esgyn.dataloader.ISource;
import com.esgyn.dataloader.ITarget;

public class DataLoaderImpl implements IDataLoader {
	Properties prop = null;
	
	public DataLoaderImpl(){
		
	}
	public DataLoaderImpl(Properties prop){
		this.prop= prop;
		run();
	}

	private String direction = "DB2DB";
	public static void main(String[] args){
		DataLoaderImpl loader = new DataLoaderImpl();
		Properties prop = new Properties();
		loader.loadData(prop);
	}
	public void loadData(Properties prop) {
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
		// TODO Auto-generated method stub
//		Properties prop = null;
		this.loadData(prop);
	}
}
