package com.esgyn.dataloader.impl;

public class ColumnDesc {
	private String colName = null;
	private String dataType = null;
	private String length = null;
	public ColumnDesc(String colName, String dataType){
		this.setColName(colName);
		this.dataType=dataType;
	}
	public String getColName() {
		return colName;
	}
	public void setColName(String colName) {
		this.colName = colName;
	}
	public String getDataType() {
		return dataType;
	}
	public void setDataType(String dataType) {
		this.dataType = dataType;
	}
	public String getLength() {
		return length;
	}
	public void setLength(String length) {
		this.length = length;
	}
}
