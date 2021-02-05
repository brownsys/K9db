package com.brownsys.pelton.planner;

import java.util.ArrayList;
public class AggregateOperator extends Operator {
	private ArrayList<Integer> groupCols;
	private ArrayList<String> aggFuncs;
	private ArrayList<Integer> aggCols;
	
	AggregateOperator(){
		this.name = "Aggregate";
	}
	
	AggregateOperator(ArrayList<Integer> groupCols, ArrayList<String>  aggFuncs, ArrayList<Integer> aggCols){
		this.name = "Aggregate";
		this.groupCols = groupCols;
		this.aggFuncs = aggFuncs;
		this.aggCols = aggCols;
	}

	public ArrayList<Integer> getGroupCols() {
		return groupCols;
	}

	public void setGroupCols(ArrayList<Integer> groupCols) {
		this.groupCols = groupCols;
	}

	public ArrayList<String> getAggFuncs() {
		return aggFuncs;
	}

	public void setAggFuncs(ArrayList<String> aggFuncs) {
		this.aggFuncs = aggFuncs;
	}

	public ArrayList<Integer> getAggCols() {
		return aggCols;
	}

	public void setAggCols(ArrayList<Integer> aggCols) {
		this.aggCols = aggCols;
	}
	
}
