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
import com.marklogic.semantics.sesame.client.MarkLogicClientDependent;
import org.openrdf.model.Value;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.Dataset;
import org.openrdf.query.Query;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.impl.AbstractQuery;
import org.openrdf.repository.sparql.query.QueryStringUtil;
import org.openrdf.repository.sparql.query.SPARQLQueryBindingSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author James Fuller
 */
public class MarkLogicQuery extends AbstractQuery implements Query,MarkLogicClientDependent,MarkLogicQueryDependent {

    protected final Logger logger = LoggerFactory.getLogger(MarkLogicQuery.class);

    private MarkLogicClient client;

    private QueryLanguage queryLanguage = QueryLanguage.SPARQL;

    private String queryString;

    private String baseURI;

    private SPARQLQueryBindingSet bindingSet;

    private boolean includeInferred;

    // constructor
    public MarkLogicQuery(MarkLogicClient client, SPARQLQueryBindingSet bindingSet, String baseUri, String queryString) {
        super();
        setBaseURI(baseUri);
        setQueryString(queryString);
        setMarkLogicClient(client);
        setBindings(bindingSet);
        setIncludeInferred(true); // is default set true
    }

    // MarkLogicClient
    @Override
    public void setMarkLogicClient(MarkLogicClient client) {
        this.client=client;
    }
    @Override
    public MarkLogicClient getMarkLogicClient() {
        return this.client;
    }

    // query string
    public String getQueryString() {
        return QueryStringUtil.getQueryString(this.queryString, getBindings());
    }
    public void setQueryString(String queryString) {
        this.queryString = queryString;
    }

    // bindings
    public void setBindings(SPARQLQueryBindingSet bindingSet) {
        this.bindingSet=bindingSet;
    }
    @Override
    public SPARQLQueryBindingSet getBindings() {
        return this.bindingSet;
    }
    public void setBinding(String name, String stringValue) {
        bindingSet.addBinding(name, ValueFactoryImpl.getInstance().createURI(stringValue));
    }

    // binding
    @Override
    public void setBinding(String name, Value value) {
        bindingSet.addBinding(name, value);
    }
    @Override
    public void removeBinding(String name) {
        bindingSet.removeBinding(name);
    }
    @Override
    public void clearBindings() {
        bindingSet.removeAll(bindingSet.getBindingNames());
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
    @Override
    public void setDataset(Dataset dataset) {
    }
    @Override
    public Dataset getDataset() {
        return null;
    }

    // execution time
    @Override
    public void setMaxExecutionTime(int maxExecTime) {
    }
    @Override
    public int getMaxExecutionTime() {
        return 0;
    }

    // base uri
    @Override
    public String getBaseURI() {
        return baseURI;
    }
    @Override
    public void setBaseURI(String baseURI) {
        this.baseURI = baseURI;
    }

    // constraining query
    @Override
    public void setConstrainingQueryDefinition(Object constrainingQueryDefinition) {
        logger.debug("setting constraining query: {}",constrainingQueryDefinition.toString());
        getMarkLogicClient().setConstrainingQueryDefinition(constrainingQueryDefinition);
    }
    @Override
    public Object getConstrainingQueryDefinition() {
        return getMarkLogicClient().getConstrainingQueryDefinition();
    }

    // rulesets
    public void setRulesets(Object rulesets){
        getMarkLogicClient().setRulesets(rulesets);
    }
    public Object getRulesets(){
        return getMarkLogicClient().getRulesets();
    }

    // graph perms
    @Override
    public void setGraphPerms(Object graphPerms) {
        getMarkLogicClient().setGraphPerms(graphPerms);
    }
    @Override
    public Object getGraphPerms() {
        return getMarkLogicClient().getGraphPerms();
    }

}
