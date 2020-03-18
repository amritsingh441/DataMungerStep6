package com.stackroute.datamunger.query.parser;

/* This class is used for storing name of field, aggregate function for 
 * each aggregate function
 * */
public class AggregateFunction {
	
	private	String field=null;
	private	String function = null; 


	public void setField(String field) {
		this.field = field;
	}

	public void setFunction(String function) {
		this.function = function;
	}

	public String getField() {
		return field;
	}

	public String getFunction() {
		return function;
	}

}
