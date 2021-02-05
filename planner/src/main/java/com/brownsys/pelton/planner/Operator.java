package com.brownsys.pelton.planner;

import java.util.ArrayList;

public class Operator {
	protected Integer id;
	protected String name;
	// This variable will contain the output schema.
	// It functions the same for all operators.
	// Some operators could possibly have the same input schema.
	// This should be inferred from the operator type and available operators 
	// as input schema will not be stored here.
	protected ArrayList<String> outSchema;
	
	Operator(){
		
	}
	
	Operator(Integer id, String type){
		this.id = id;
		this.name = type;
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String type) {
		this.name = type;
	}

	public ArrayList<String> getOutSchema() {
		return outSchema;
	}

	public void setOutSchema(ArrayList<String> outSchema) {
		this.outSchema = outSchema;
	}
	
}
