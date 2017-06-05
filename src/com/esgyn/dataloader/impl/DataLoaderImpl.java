package com.esgyn.dataloader.impl;

import java.sql.ResultSet;

import com.esgyn.dataloader.IDataLoader;
import com.esgyn.dataloader.ISource;
import com.esgyn.dataloader.ITarget;

public class DataLoaderImpl implements IDataLoader{
	
	
	public void loadData(){
		ISource source = new SourceImpl();
		ResultSet rs = source.readFromDB();
		ITarget target = new TargetImpl();
		target.WriteTargetToDB(rs);
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		this.loadData();
	}

}
