package com.esgyn.dataloader;

import java.util.Properties;

public interface IDataLoader extends Runnable{
	public void loadData(Properties prop);
}
