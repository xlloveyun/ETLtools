package com.esgyn.dataloader;

import java.util.List;

import com.esgyn.dataloader.impl.ColumnDesc;

public interface ITarget{
	public Long commit();
	public List<ColumnDesc> getColumns();
	public void addLine(List<Object> cols);
	public void closeConnection();
	public void process();
}
