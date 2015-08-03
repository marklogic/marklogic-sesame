package com.marklogic.sesame.functionaltests.util;

import java.util.Iterator;

import org.openrdf.model.Statement;

public class StatementIterator implements Iterator{
	
	private StatementList sL;
	private int size;
	private int index = 0;
	
	public StatementIterator(StatementList<Statement> sL){
		this.sL =sL;
		size =sL.size();
		
	}

	@Override
	public boolean hasNext() {
		if(sL.get(index) !=null)
			return true;
		return false;
	}

	@Override
	public Statement next() {
		Statement st = (Statement) sL.get(index);
		index ++;
		return st;
	}
	
}
