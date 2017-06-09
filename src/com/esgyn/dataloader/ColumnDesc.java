package com.esgyn.dataloader;

public class ColumnDesc {
	private String colName = null;
	public ColumnDesc(String colName){
		this.setColName(colName);
	}
	public String getColName() {
		return colName;
	}
	public void setColName(String colName) {
		this.colName = colName;
	}
}
