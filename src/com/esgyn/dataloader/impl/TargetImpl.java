package com.esgyn.dataloader.impl;

import java.sql.ResultSet;

import com.esgyn.dataloader.ITarget;

public class TargetImpl implements ITarget{
	public void WriteTargetToFile(){
		System.out.println("here we are writing to file");
	}
	public void WriteTargetToMQ(){
		System.out.println("here we are writing to MQ");
	}
	@Override
	public void WriteTargetToDB(ResultSet rs) {
		// TODO Auto-generated method stub
		System.out.println("here we are writing to DB");
	}
}
