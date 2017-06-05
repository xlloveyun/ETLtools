package com.esgyn.dataloader;

import java.sql.ResultSet;

public interface ITarget {
	public void WriteTargetToDB(ResultSet rs);
	public void WriteTargetToFile();
}
