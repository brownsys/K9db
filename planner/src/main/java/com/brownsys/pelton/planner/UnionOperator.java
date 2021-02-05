package com.brownsys.pelton.planner;

import java.util.ArrayList;

public class UnionOperator extends Operator{
	UnionOperator(){
		this.name = "Union";
	}
	
	UnionOperator(ArrayList<String>  outSchema){
		this.name = "Union";
		this.outSchema = outSchema;
	}
}
