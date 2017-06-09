package com.esgyn.dataloader;

import java.util.List;

public interface ITarget{
	public Long commit();
	public List<ColumnDesc> getColumns();
	public void addLine(List<Object> cols);
}
