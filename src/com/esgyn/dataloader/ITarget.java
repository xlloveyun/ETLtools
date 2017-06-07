package com.esgyn.dataloader;

import java.sql.ResultSet;
import java.util.List;

public interface ITarget{
	public void WriteTargetToFileFromDB();
	public void WriteTargetToMQFromDB();
	public void WriteTargetToDBFromFile();
	public void WriteTargetToDBFromMQ();
	void WriteTargetToDBFromDB(List<Object> list);
}
