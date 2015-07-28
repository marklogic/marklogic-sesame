/*
 * Copyright 2015 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * A library that enables access to a MarkLogic-backed triple-store via the
 * Sesame API.
 */
package com.marklogic.semantics.sesame.query;

import com.marklogic.semantics.sesame.client.MarkLogicClient;
import org.openrdf.model.Value;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.UnsupportedQueryLanguageException;
import org.openrdf.query.impl.AbstractQuery;
import org.openrdf.repository.sparql.query.SPARQLQueryBindingSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author James Fuller
 */
public class MarkLogicQuery extends AbstractQuery {

    protected final Logger logger = LoggerFactory.getLogger(MarkLogicQuery.class);

    private MarkLogicClient client;

    private QueryLanguage queryLanguage = QueryLanguage.SPARQL;

    private String queryString;

    private String baseURI;

    private SPARQLQueryBindingSet mapBindingSet;

    private boolean includeInferred;

    public MarkLogicQuery(MarkLogicClient client, SPARQLQueryBindingSet mapBindingSet, String baseUri, String queryString) {
        super();
        setQueryString(queryString);
        setClient(client);
        setBindingSet(mapBindingSet);
        setIncludeInferred(true); // is default set true
    }
    public void setClient(MarkLogicClient client) {
        this.client=client;
    }
    public MarkLogicClient getClient() {
        return client;
    }

    // base uri
    public String getBaseURI() {
        return baseURI;
    }
    public void setBaseURI(String baseURI) {
        this.baseURI = baseURI;
    }

    // query language
    public QueryLanguage getQueryLanguage() {
        return queryLanguage;
    }
    public void setQueryLanguage(QueryLanguage queryLanguage){
        if (QueryLanguage.SPARQL.equals(queryLanguage))
            queryLanguage = QueryLanguage.SPARQL;
        throw new UnsupportedQueryLanguageException("Unsupported query language " + queryLanguage.getName());
    }

    // query string
    public String getQueryString() {
        return queryString;
    }
    public void setQueryString(String queryString) {
        this.queryString = queryString;
    }

    // bindings
    public void setBindingSet(SPARQLQueryBindingSet mapBindingSet) {
        this.mapBindingSet=mapBindingSet;
    }
    public SPARQLQueryBindingSet getBindingSet() {
        return this.mapBindingSet;
    }
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
        mapBindingSet.removeAll(mapBindingSet.getBindingNames());
    }

    // include inferred
    @Override
    public void setIncludeInferred(boolean includeInferred) {
        this.includeInferred=includeInferred;
    }
    @Override
    public boolean getIncludeInferred() {
        return this.includeInferred;
    }

    // dataset
    public void setDataset(Dataset dataset) {
    }
    public Dataset getDataset() {
        return null;
    }

    // execution time
    public void setMaxExecutionTime(int maxExecTime) {
    }
    public int getMaxExecutionTime() {
        return 0;
    }


}
