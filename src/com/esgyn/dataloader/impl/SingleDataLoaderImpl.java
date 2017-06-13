package com.esgyn.dataloader.impl;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.esgyn.dataloader.IDataLoader;
import com.esgyn.dataloader.ISource;
import com.esgyn.dataloader.ITarget;

public class SingleDataLoaderImpl implements IDataLoader {
	private Properties prop = null;
	private static Logger logger = Logger.getLogger(DBTargetImpl.class);
	
	public SingleDataLoaderImpl(){
		
	}
	public SingleDataLoaderImpl(Properties prop){
		this.prop= prop;
	}
	public void loadData() throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException {
		String sourceImpl = prop.getProperty("sourceImpl","com.esgyn.dataloader.impl.DBSourceImpl");
		String tgtImpl = prop.getProperty("targetImpl","com.esgyn.dataloader.impl.DBTargetImpl");
		ISource source = (ISource) Class.forName(sourceImpl).getDeclaredConstructor(Properties.class).newInstance(prop);
		ITarget target = (ITarget) Class.forName(tgtImpl).getDeclaredConstructor(Properties.class,ISource.class).newInstance(prop,source);
		target.process();
	}

	@Override
	public void run() {
		long start = System.currentTimeMillis();
		try {
			this.loadData();
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException | ClassNotFoundException e) {
			logger.error(e);
		}
		java.text.DecimalFormat   df   =new   java.text.DecimalFormat("#"); 
		long elapsedTimeMillis = System.currentTimeMillis()-start;
		String elapsedTimeMin = df.format(elapsedTimeMillis/(60*1000F));
		float elapsedTimeSec = (elapsedTimeMillis%(60*1000F))/1000F;
		logger.error("the total time cost: " + elapsedTimeMin + " minutes; " + elapsedTimeSec + " seconds");
	}
}
