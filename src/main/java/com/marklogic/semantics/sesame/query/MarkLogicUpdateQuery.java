package com.marklogic.semantics.sesame.query;

import com.marklogic.semantics.sesame.client.MarkLogicClient;
import org.openrdf.model.Value;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.*;
import org.openrdf.query.impl.MapBindingSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author James Fuller
 */
public class MarkLogicUpdateQuery implements Update {

    protected final Logger logger = LoggerFactory.getLogger(MarkLogicUpdateQuery.class);

    private MarkLogicClient client;

	private QueryLanguage queryLanguage = QueryLanguage.SPARQL;

    private String queryString;

    private String baseURI;

    private MapBindingSet mapBindingSet;

    public MarkLogicUpdateQuery(MarkLogicClient client, MapBindingSet mapBindingSet, String baseUri, String queryString) {
        super();
        this.client = client;
        this.queryLanguage = QueryLanguage.SPARQL;
        this.queryString = queryString;
        this.mapBindingSet= mapBindingSet;
    }

    public String getBaseURI() {
        return baseURI;
    }
    public void setBaseURI(String baseURI) {
        this.baseURI = baseURI;
    }

    public QueryLanguage getQueryLanguage() {
        return queryLanguage;
    }
    public void setQueryLanguage(QueryLanguage queryLanguage){
        if (QueryLanguage.SPARQL.equals(queryLanguage))
            queryLanguage = QueryLanguage.SPARQL;
        throw new UnsupportedQueryLanguageException("Unsupported query language " + queryLanguage.getName());
    }
    public String getQueryString() {
        return queryString;
    }
    public void setQueryString(String queryString) {
        this.queryString = queryString;
    }

    public MarkLogicClient getClient() {
        return client;
    }

    //evaluate
    @Override
    public void execute(){
        MarkLogicClient mc = getClient();
        mc.sendUpdateQuery(getQueryString(), mapBindingSet);
    }

    // bindings
    public void setBinding(String name, String stringValue) {
        mapBindingSet.addBinding(name, ValueFactoryImpl.getInstance().createURI(stringValue));
    }
    public void setBinding(String name, Value value) {
        mapBindingSet.addBinding(name, value);
    }
    public void removeBinding(String name) {
        mapBindingSet.removeBinding(name);
    }
    public void clearBindings() {
        mapBindingSet.clear();
    }

    @Override
    public BindingSet getBindings() {
        return null;
    }

    public void setDataset(Dataset dataset) {
    }
    public Dataset getDataset() {
        return null;
    }

    @Override
    public void setIncludeInferred(boolean includeInferred) {

    }

    @Override
    public boolean getIncludeInferred() {
        return false;
    }

    public void setMaxExecutionTime(int maxExecTime) {
    }
    public int getMaxExecutionTime() {
        return 0;
    }


}
