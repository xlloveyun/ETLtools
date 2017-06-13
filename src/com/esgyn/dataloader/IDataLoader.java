package com.esgyn.dataloader;

import java.lang.reflect.InvocationTargetException;

public interface IDataLoader extends Runnable{
	public void loadData() throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException;
}
