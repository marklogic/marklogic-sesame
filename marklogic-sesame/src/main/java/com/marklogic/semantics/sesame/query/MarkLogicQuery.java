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

import org.openrdf.model.Value;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.Dataset;
import org.openrdf.query.Query;
import org.openrdf.query.impl.AbstractQuery;
import org.openrdf.repository.sparql.query.QueryStringUtil;
import org.openrdf.repository.sparql.query.SPARQLQueryBindingSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.marklogic.semantics.sesame.client.MarkLogicClient;
import com.marklogic.semantics.sesame.client.MarkLogicClientDependent;

/**
 * base query class
 *
 * @author James Fuller
 */
public class MarkLogicQuery extends AbstractQuery implements Query,MarkLogicClientDependent,MarkLogicQueryDependent {

    protected final Logger logger = LoggerFactory.getLogger(MarkLogicQuery.class);

    private MarkLogicClient client;

    private String queryString;

    private String baseURI;

    private SPARQLQueryBindingSet bindingSet;

    private boolean includeInferred;

    /**
     * constructor
     *
     * @param client
     * @param bindingSet
     * @param baseUri
     * @param queryString
     */
    public MarkLogicQuery(MarkLogicClient client, SPARQLQueryBindingSet bindingSet, String baseUri, String queryString, Object graphPerms) {
        super();
        setBaseURI(baseUri);
        setQueryString(queryString);
        setMarkLogicClient(client);
        setBindings(bindingSet);
        setIncludeInferred(true); // is default set true
        setGraphPerms(graphPerms);
    }

    /**
     * sets MarkLogicClient
     *
     * @param client
     */
    @Override
    public void setMarkLogicClient(MarkLogicClient client) {
        this.client=client;
    }

    /**
     * get MarkLogicClient
     *
     * @return
     */
    @Override
    public MarkLogicClient getMarkLogicClient() {
        return this.client;
    }

    /**
     * sets the query string
     *
     * @return
     */
    public String getQueryString() {
        return QueryStringUtil.getQueryString(this.queryString, getBindings());
    }

    /**
     * sets the query string
     *
     * @param queryString
     */
    public void setQueryString(String queryString) {
        this.queryString = queryString;
    }

    /**
     * sets bindings used by query
     *
     * @param bindingSet
     */
    public void setBindings(SPARQLQueryBindingSet bindingSet) {
        this.bindingSet=bindingSet;
    }

    /**
     * gets bindings used by query
     *
     * @return
     */
    @Override
    public SPARQLQueryBindingSet getBindings() {
        return this.bindingSet;
    }

    /**
     * set individual binding
     *
     * @param name
     * @param stringValue
     */
    public void setBinding(String name, String stringValue) {
        bindingSet.addBinding(name, ValueFactoryImpl.getInstance().createURI(stringValue));
    }

    /**
     * set individual binding and value
     * @param name
     * @param value
     */
    @Override
    public void setBinding(String name, Value value) {
        bindingSet.addBinding(name, value);
    }

    /**
     * remove binding
     *
     * @param name
     */
    @Override
    public void removeBinding(String name) {
        bindingSet.removeBinding(name);
    }

    /**
     * clear bindings
     *
     */
    @Override
    public void clearBindings() {
        bindingSet.removeAll(bindingSet.getBindingNames());
    }

    /**
     * set true or false to use default inference ruleset
     *
     * @param includeInferred
     */
    @Override
    public void setIncludeInferred(boolean includeInferred) {
        this.includeInferred=includeInferred;
    }

    /**
     * return true or fales if using default inference ruleset
     *
     * @return boolean
     */
    @Override
    public boolean getIncludeInferred() {
        return this.includeInferred;
    }

    /**
     * implemented to honor interface
     *
     * @param dataset
     */
    @Override
    public void setDataset(Dataset dataset) {
    }

    /**
     * implemented to honor interface
     *
     * @return
     */
    @Override
    public Dataset getDataset() {
        return null;
    }

    /**
     * sets maximum execution time for query
     *
     * @param maxExecTime
     */
    @Override
    public void setMaxExecutionTime(int maxExecTime) {
    }

    /**
     *
     * @return int
     */
    @Override
    public int getMaxExecutionTime() {
        return 0;
    }

    /**
     *
     * @return
     */
    @Override
    public String getBaseURI() {
        return baseURI;
    }

    /**
     *
     * @param baseURI
     */
    @Override
    public void setBaseURI(String baseURI) {
        this.baseURI = baseURI;
    }

    /**
     * sets constraining query
     *
     * @param constrainingQueryDefinition
     */
    @Override
    public void setConstrainingQueryDefinition(Object constrainingQueryDefinition) {
        getMarkLogicClient().setConstrainingQueryDefinition(constrainingQueryDefinition);
    }

    /**
     *
     * @return
     */
    @Override
    public Object getConstrainingQueryDefinition() {
        return getMarkLogicClient().getConstrainingQueryDefinition();
    }

    /**
     * sets the inference rulesets to be used by query
     *
     * @param ruleset
     */
    public void setRulesets(Object ... ruleset){
        getMarkLogicClient().setRulesets(ruleset);
    }

    /**
     *
     * @return
     */
    public Object[] getRulesets(){
        return getMarkLogicClient().getRulesets();
    }

    /**
     * sets the graph permissions to be used by query
     *
     * @param graphPerms
     */
    @Override
    public void setGraphPerms(Object graphPerms) {
        getMarkLogicClient().setGraphPerms(graphPerms);
    }

    /**
     *
     * @return
     */
    @Override
    public Object getGraphPerms() {
        return getMarkLogicClient().getGraphPerms();
    }
}
