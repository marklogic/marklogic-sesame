package com.marklogic.sesame.functionaltests.util;

import java.util.Iterator;

import org.openrdf.model.Statement;

public class StatementIterable<V extends Statement> implements Iterable{ 

	private StatementIterator sI;
	
	@Override
	public Iterator iterator() {
		// TODO Auto-generated method stub
		return this.sI;
	}
	
	public StatementIterable(StatementIterator  sI){
		this.sI =sI;
	}
}
