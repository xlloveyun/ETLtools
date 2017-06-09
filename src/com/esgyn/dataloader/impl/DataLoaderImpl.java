package com.esgyn.dataloader.impl;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.esgyn.dataloader.IDataLoader;
import com.esgyn.dataloader.ISource;
import com.esgyn.dataloader.ITarget;

public class DataLoaderImpl implements IDataLoader {
	private Properties prop = null;
	private static Logger logger = Logger.getLogger(DBTargetImpl.class);
	
	public DataLoaderImpl(){
		
	}
	public DataLoaderImpl(Properties prop){
		this.prop= prop;
	}
	public void loadData() {
		String sourceImpl = prop.getProperty("sourceImpl","com.esgyn.dataloader.impl.DBSourceImpl");
		String tgtImpl = prop.getProperty("targetImpl","com.esgyn.dataloader.impl.DBTargetImpl");
		try {
			ITarget target = (ITarget) Class.forName(tgtImpl).getDeclaredConstructor(Properties.class).newInstance(prop);
			ISource source = (ISource) Class.forName(sourceImpl).getDeclaredConstructor(Properties.class,ITarget.class).newInstance(prop,target);
			source.process();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
