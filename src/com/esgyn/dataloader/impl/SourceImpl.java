package com.esgyn.dataloader.impl;

import java.sql.ResultSet;

import com.esgyn.dataloader.ISource;

public class SourceImpl implements ISource{
	private ResultSet rs=null;
	public ResultSet readFromDB(){
		System.out.println("here we are reading from db.");
		return null;
	}
	
	public void readFromFile(){
		System.out.println("here we are reading from file.");
	}
	
	public void readFromMQ(){
		System.out.println("here we are reading from MQ.");
	}
}
