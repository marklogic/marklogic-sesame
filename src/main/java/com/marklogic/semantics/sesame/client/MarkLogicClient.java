package com.marklogic.semantics.sesame.client;

import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.semantics.*;
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

	private ValueFactory f;

	public MarkLogicClient() {
		this.mcimpl = new MarkLogicClientImpl();
		this.f = new ValueFactoryImpl();
	}

	public ValueFactory getValueFactory() {
		return f;
	}

	public TupleQueryResult sendTupleQuery(String querystring){

        // to be extruded
		mcimpl.databaseClient = DatabaseClientFactory.newClient(
				"127.0.0.1", 8200, "admin", "admin", DatabaseClientFactory.Authentication.DIGEST);

		SPARQLQueryManager smgr = mcimpl.databaseClient.newSPARQLQueryManager();
		SPARQLQueryDefinition qdef = smgr.newQueryDefinition(querystring);
		SPARQLTupleResults results = smgr.executeSelect(qdef);
        // to be extruded

		List<String> bindingNames = new ArrayList<String>();
        String bindingnames[]= results.getBindingNames();
		for ( String bindingName: bindingnames ) {
			bindingNames.add(bindingName);
		}

		List<BindingSet> bindingSetList = new ArrayList<BindingSet>();
        for ( SPARQLTuple tuple : results ) {
			MapBindingSet mbs = new MapBindingSet();
            for(String name : bindingNames){
                SPARQLBinding binding = tuple.get(name);
                String bindingtype = binding.getType().toString();
                if (bindingtype.equals("uri")) {
                    URI s = f.createURI(binding.getValue());
                    mbs.addBinding(name, s);
                } else if (bindingtype.equals("literal")) {
                    Literal o = f.createLiteral(tuple.get("o").getValue());
                    mbs.addBinding(name, o);
                } else {
                }
            }
			bindingSetList.add(mbs);
		}

		return (TupleQueryResult) new TupleQueryResultImpl(bindingNames,bindingSetList);
	}
}
