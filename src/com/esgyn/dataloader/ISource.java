package com.esgyn.dataloader;

import java.util.List;
import java.util.Properties;

public interface ISource{
	public void read();
	public List<ColumnDesc> getColumns();
	public void process();
}
