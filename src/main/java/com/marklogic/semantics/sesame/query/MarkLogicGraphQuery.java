package com.marklogic.semantics.sesame.query;

import com.marklogic.semantics.sesame.client.MarkLogicClient;
import org.openrdf.model.Value;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.*;
import org.openrdf.query.impl.AbstractQuery;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *
 * @author James Fuller
 */
public class MarkLogicGraphQuery extends AbstractQuery implements GraphQuery {

    protected final Logger logger = LoggerFactory.getLogger(MarkLogicGraphQuery.class);

    private MarkLogicClient client;

	private QueryLanguage queryLanguage = QueryLanguage.SPARQL;

    private String queryString;

    private String baseURI;

    private MapBindingSet mapBindingSet;

    public MarkLogicGraphQuery(MarkLogicClient client, MapBindingSet mapBindingSet, String baseUri, String queryString) {
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
    public GraphQueryResult evaluate()
            throws QueryEvaluationException {
        MarkLogicClient mc = getClient();
        try {
            return mc.sendGraphQuery(getQueryString(),mapBindingSet);
        } catch (IOException e) {
            throw new QueryEvaluationException(e);
        }
    }
    @Override
    public void evaluate(RDFHandler resultHandler) throws QueryEvaluationException, RDFHandlerException {
        GraphQueryResult queryResult = evaluate();
        QueryResults.report(queryResult, resultHandler);
    }

    // bindings
    public void setBinding(String name, String stringValue) {
        mapBindingSet.addBinding(name, ValueFactoryImpl.getInstance().createURI(stringValue));
    }
    @Override
    public void setBinding(String name, Value value) {
        mapBindingSet.addBinding(name,value);
    }
    @Override
    public void removeBinding(String name) {
        mapBindingSet.removeBinding(name);
    }
    @Override
    public void clearBindings() {
        mapBindingSet.clear();
    }

    @Override
    public void setDataset(Dataset dataset) {
    }
    @Override
    public Dataset getDataset() {
        return null;
    }

    @Override
    public void setMaxExecutionTime(int maxExecTime) {
    }
    @Override
    public int getMaxExecutionTime() {
        return 0;
    }

}
