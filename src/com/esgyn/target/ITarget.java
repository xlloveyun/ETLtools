package com.esgyn.target;

import java.sql.Connection;

public interface ITarget {
	
	Connection connection = null;
	public void setTarget();
}
