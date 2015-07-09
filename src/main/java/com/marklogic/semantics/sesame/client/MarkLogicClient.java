package com.marklogic.semantics.sesame.client;

import com.marklogic.client.semantics.SPARQLBinding;
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

	private ValueFactory f;

	public MarkLogicClient() {
		this.mcimpl = new MarkLogicClientImpl();
		this.f = new ValueFactoryImpl();
	}

	public ValueFactory getValueFactory() {
		return f;
	}

	public TupleQueryResult sendTupleQuery(String queryString){

        SPARQLTupleResults results = mcimpl.performSPARQLQuery(queryString);

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
