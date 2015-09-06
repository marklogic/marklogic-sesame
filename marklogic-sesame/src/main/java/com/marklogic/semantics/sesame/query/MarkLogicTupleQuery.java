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

import com.marklogic.client.FailedRequestException;
import com.marklogic.semantics.sesame.client.MarkLogicClient;
import org.openrdf.query.*;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sparql.query.SPARQLQueryBindingSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * tuple query
 *
 * @author James Fuller
 */
public class MarkLogicTupleQuery extends MarkLogicQuery implements TupleQuery,MarkLogicQueryDependent {

    protected final Logger logger = LoggerFactory.getLogger(MarkLogicTupleQuery.class);

    protected final long start = 1;
    protected final long pageLength = 1000000;

    /**
     * constructor
     *
     * @param client
     * @param bindingSet
     * @param baseUri
     * @param queryString
     */
    public MarkLogicTupleQuery(MarkLogicClient client, SPARQLQueryBindingSet bindingSet, String baseUri, String queryString, Object graphPerms) {
        super(client, bindingSet, baseUri, queryString, graphPerms);
    }

    /**
     *
     * @return
     * @throws QueryEvaluationException
     */
    @Override
    public TupleQueryResult evaluate() throws QueryEvaluationException {
        return evaluate(this.start,this.pageLength);
    }

    /**
     *
     * @param start
     * @param pageLength
     * @return
     * @throws QueryEvaluationException
     */
    public TupleQueryResult evaluate(long start, long pageLength)
            throws QueryEvaluationException {
        try {
            return getMarkLogicClient().sendTupleQuery(getQueryString(), getBindings(), start, pageLength, getIncludeInferred(), getBaseURI());
        }catch (RepositoryException e) {
            throw new QueryEvaluationException(e.getMessage(), e);
        }catch (MalformedQueryException e) {
            throw new QueryEvaluationException(e.getMessage(), e);
        }catch (IOException e) {
            throw new QueryEvaluationException(e.getMessage(), e);
        }catch(FailedRequestException e){
            throw new QueryEvaluationException(e.getMessage(), e);
        }
    }

    /**
     *
     * @param resultHandler
     * @throws QueryEvaluationException
     * @throws TupleQueryResultHandlerException
     */
    @Override
    public void evaluate(TupleQueryResultHandler resultHandler) throws QueryEvaluationException, TupleQueryResultHandlerException {
        TupleQueryResult queryResult = evaluate();
        QueryResults.report(queryResult, resultHandler);
    }
}
