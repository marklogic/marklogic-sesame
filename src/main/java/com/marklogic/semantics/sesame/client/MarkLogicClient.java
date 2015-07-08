package com.marklogic.semantics.sesame.client;

import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.semantics.SPARQLQueryDefinition;
import com.marklogic.client.semantics.SPARQLQueryManager;
import com.marklogic.client.semantics.SPARQLTuple;
import com.marklogic.client.semantics.SPARQLTupleResults;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.query.impl.TupleQueryResultImpl;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class MarkLogicClient {

	protected static final Charset UTF8 = Charset.forName("UTF-8");

	private MarkLogicClientImpl mcimpl;

	private ValueFactory valueFactory;

	public MarkLogicClient() {
		this.mcimpl = new MarkLogicClientImpl();
		valueFactory = new ValueFactoryImpl();
	}

	public ValueFactory getValueFactory() {
		return valueFactory;
	}

	public TupleQueryResult sendTupleQuery(String querystring){
		mcimpl.databaseClient = DatabaseClientFactory.newClient(
				"127.0.0.1", 8200, "admin", "admin", DatabaseClientFactory.Authentication.DIGEST);

		SPARQLQueryManager smgr = mcimpl.databaseClient.newSPARQLQueryManager();
		SPARQLQueryDefinition qdef = smgr.newQueryDefinition(querystring);
		SPARQLTupleResults results = smgr.executeSelect(qdef);

		List<String> bindingNames = new ArrayList<String>(3);
		for ( String bindingName: results.getBindingNames() ) {
			bindingNames.add(bindingName);
		}

		List<BindingSet> bindingSetList = new ArrayList<BindingSet>();
		ValueFactory f = new ValueFactoryImpl();

		for ( SPARQLTuple tuple : results ) {
			MapBindingSet mbs = new MapBindingSet(1);
			ValueFactory factory = ValueFactoryImpl.getInstance();

			URI s = factory.createURI(tuple.get("s").getValue());
			URI p = factory.createURI(tuple.get("p").getValue());
			Literal o = factory.createLiteral(tuple.get("o").getValue());

			mbs.addBinding("s",s);
			mbs.addBinding("p",p);
			mbs.addBinding("o",o);
			bindingSetList.add(mbs);
		}

		TupleQueryResult impl = new TupleQueryResultImpl(bindingNames,bindingSetList);
		return (TupleQueryResult) impl;
	}
}
