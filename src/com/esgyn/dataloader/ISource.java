package com.esgyn.dataloader;

import java.sql.ResultSet;
import java.util.List;
import java.util.Properties;

import com.esgyn.dataloader.impl.ColumnDesc;

public interface ISource{
	public ResultSet read();
	public List<ColumnDesc> getColumns();
	public void closeConnection();
}
