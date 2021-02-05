package com.brownsys.pelton.planner;

import java.util.ArrayList;

public class ProjectOperator extends Operator{
	private ArrayList<Integer> cids;
	
	ProjectOperator(){
		this.name = "Project";
	}
	
	ProjectOperator(ArrayList<Integer> cids, ArrayList<String> outSchema){
		this.name = "Project";
		this.cids = cids;
		this.outSchema = outSchema;
	}

	public ArrayList<Integer> getCids() {
		return cids;
	}

	public void setCids(ArrayList<Integer> cids) {
		this.cids = cids;
	}
}
