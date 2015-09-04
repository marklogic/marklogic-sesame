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

import com.marklogic.client.semantics.GraphPermissions;
import com.marklogic.semantics.sesame.client.MarkLogicClient;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryResults;
import org.openrdf.repository.sparql.query.SPARQLQueryBindingSet;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * graph query
 *
 * @author James Fuller
 */
public class MarkLogicGraphQuery extends MarkLogicQuery implements GraphQuery,MarkLogicQueryDependent {

    protected final Logger logger = LoggerFactory.getLogger(MarkLogicGraphQuery.class);

    /**
     * constructor
     *
     * @param client
     * @param bindingSet
     * @param baseUri
     * @param queryString
     */
    public MarkLogicGraphQuery(MarkLogicClient client, SPARQLQueryBindingSet bindingSet, String baseUri, String queryString, GraphPermissions graphPerms) {
        super(client, bindingSet, baseUri, queryString, graphPerms);
    }

    /**
     *
     * @return
     * @throws QueryEvaluationException
     */
    @Override
    public GraphQueryResult evaluate()
            throws QueryEvaluationException {
        try {
            return getMarkLogicClient().sendGraphQuery(getQueryString(),getBindings(),getIncludeInferred(),getBaseURI());
        } catch (IOException e) {
            throw new QueryEvaluationException(e);
        }
    }

    /**
     *
     * @param resultHandler
     * @throws QueryEvaluationException
     * @throws RDFHandlerException
     */
    @Override
    public void evaluate(RDFHandler resultHandler) throws QueryEvaluationException, RDFHandlerException {
        GraphQueryResult queryResult = evaluate();
        QueryResults.report(queryResult, resultHandler);
    }

}
