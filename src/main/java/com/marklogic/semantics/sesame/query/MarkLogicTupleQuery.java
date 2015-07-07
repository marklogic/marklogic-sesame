package com.marklogic.semantics.sesame.query;

import com.marklogic.semantics.sesame.client.MarkLogicClient;
import org.openrdf.model.Value;
import org.openrdf.query.*;
import org.openrdf.query.impl.AbstractQuery;

public class MarkLogicTupleQuery extends AbstractQuery implements TupleQuery {

    private MarkLogicClient client;
	private QueryLanguage queryLanguage = QueryLanguage.SPARQL;
    private String queryString;
    private String baseURI;

    public MarkLogicTupleQuery() {
        this(new MarkLogicClient(),"","");
    }

    public MarkLogicTupleQuery(MarkLogicClient client, String baseUri, String queryString) {
        this.client = client;
        this.queryLanguage = QueryLanguage.SPARQL;
        this.queryString = queryString;
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

    public String getQueryString() {
        return queryString;
    }
    public void setQueryString(String queryString) {
        this.queryString = queryString;
    }

    public MarkLogicClient getClient() {
        if (client == null) {
            client =  new MarkLogicClient();
        }
        return client;
    }

    @Override
    public TupleQueryResult evaluate()
            throws QueryEvaluationException {

        MarkLogicClient mc = getClient();
        return mc.sendTupleQuery(getQueryString());
    }

    @Override
    public void evaluate(TupleQueryResultHandler handler) throws QueryEvaluationException, TupleQueryResultHandlerException {

    }

    @Override
    public void setBinding(String name, Value value) {

    }

    @Override
    public void removeBinding(String name) {

    }

    @Override
    public void clearBindings() {

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
