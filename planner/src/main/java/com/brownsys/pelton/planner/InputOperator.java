package com.brownsys.pelton.planner;

import java.util.ArrayList;

public class InputOperator extends Operator{
	private String tableName;
	
	InputOperator(){
		this.name = "Input";
	}
	InputOperator(String tableName, ArrayList<String> outSchema){
		this.name = "Input";
		this.tableName = tableName;
		this.outSchema = outSchema;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
}
