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
package com.marklogic.semantics.sesame.client;

import com.marklogic.client.Transaction;
import com.marklogic.semantics.sesame.MarkLogicTransactionException;
import org.apache.commons.io.input.ReaderInputStream;
import org.openrdf.http.protocol.UnauthorizedException;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.*;
import org.openrdf.query.resultio.QueryResultIO;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultParser;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sparql.query.SPARQLQueryBindingSet;
import org.openrdf.rio.*;
import org.openrdf.rio.helpers.ParseErrorLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * internal class which straddles Sesame and MarkLogic java api client
 *
 * @author James Fuller
 */
public class MarkLogicClient {

	protected final Logger logger = LoggerFactory.getLogger(MarkLogicClient.class);

	protected static final Charset UTF8 = Charset.forName("UTF-8");
	protected static final Charset charset = UTF8;

	protected static final TupleQueryResultFormat format = TupleQueryResultFormat.JSON;
	protected static final RDFFormat rdfFormat = RDFFormat.NTRIPLES;
	private final MarkLogicClientImpl _client;

	private static Executor executor = Executors.newCachedThreadPool();

	private ValueFactory f;

	private ParserConfig parserConfig = new ParserConfig();

	private Transaction tx = null;

	/**
	 *
 	 * @param host
	 * @param port
	 * @param user
	 * @param password
	 * @param auth
	 */
	public MarkLogicClient(String host, int port, String user, String password,String auth) {
		this._client = new MarkLogicClientImpl(host,port,user,password,auth);
	}

	/**
	 *
	 * @param databaseClient
	 */
	public MarkLogicClient(Object databaseClient) {
		this._client = new MarkLogicClientImpl(databaseClient);
	}

	/**
	 *
	 * @return
	 */
	public ValueFactory getValueFactory() {
		return this.f;
	}

	/**
	 *
	 * @param f
	 */
	public void setValueFactory(ValueFactory f) {
		this.f=f;
	}

	/**
	 *
	 * @param queryString
	 * @param bindings
	 * @param start
	 * @param pageLength
	 * @param includeInferred
	 * @param baseURI
	 * @return
	 * @throws IOException
	 * @throws RepositoryException
	 * @throws MalformedQueryException
	 * @throws UnauthorizedException
	 * @throws QueryInterruptedException
	 */
	public TupleQueryResult sendTupleQuery(String queryString,SPARQLQueryBindingSet bindings, long start, long pageLength, boolean includeInferred, String baseURI) throws IOException, RepositoryException, MalformedQueryException, UnauthorizedException,
    QueryInterruptedException {
		InputStream stream = getClient().performSPARQLQuery(queryString, bindings, start, pageLength, this.tx, includeInferred, baseURI);
		TupleQueryResultParser parser = QueryResultIO.createParser(format, getValueFactory());
		MarkLogicBackgroundTupleResult tRes = new MarkLogicBackgroundTupleResult(parser,stream);
		execute(tRes);
		return tRes;
	}

	/**
	 *
	 * @param queryString
	 * @param bindings
	 * @param includeInferred
	 * @param baseURI
	 * @return
	 * @throws IOException
	 */
	public GraphQueryResult sendGraphQuery(String queryString, SPARQLQueryBindingSet bindings, boolean includeInferred, String baseURI) throws IOException {
		InputStream stream = getClient().performGraphQuery(queryString, bindings, this.tx, includeInferred, baseURI);

		RDFParser parser = Rio.createParser(rdfFormat, getValueFactory());
		parser.setParserConfig(getParserConfig());
		parser.setParseErrorListener(new ParseErrorLogger());
		parser.setPreserveBNodeIDs(true);

		MarkLogicBackgroundGraphResult gRes;

		// fixup - baseURI cannot be null
		if(baseURI != null){
			gRes= new MarkLogicBackgroundGraphResult(parser,stream,charset,baseURI);
		}else{
			gRes= new MarkLogicBackgroundGraphResult(parser,stream,charset,"");
		}

		execute(gRes);
		return gRes;

	}

	/**
	 *
	 * @param queryString
	 * @param bindings
	 * @param includeInferred
	 * @param baseURI
	 * @return
	 * @throws IOException
	 * @throws RepositoryException
	 * @throws MalformedQueryException
	 * @throws UnauthorizedException
	 * @throws QueryInterruptedException
	 */
	public boolean sendBooleanQuery(String queryString, SPARQLQueryBindingSet bindings, boolean includeInferred, String baseURI) throws IOException, RepositoryException, MalformedQueryException, UnauthorizedException,
    QueryInterruptedException {
		return getClient().performBooleanQuery(queryString, bindings, this.tx, includeInferred, baseURI);
	}

	/**
	 *
	 * @param queryString
	 * @param bindings
	 * @param includeInferred
	 * @param baseURI
	 * @throws IOException
	 * @throws RepositoryException
	 * @throws MalformedQueryException
	 * @throws UnauthorizedException
	 * @throws UpdateExecutionException
	 */
	public void sendUpdateQuery(String queryString, SPARQLQueryBindingSet bindings, boolean includeInferred, String baseURI) throws IOException, RepositoryException, MalformedQueryException, UnauthorizedException,
    UpdateExecutionException {
		getClient().performUpdateQuery(queryString, bindings, this.tx, includeInferred, baseURI);
	}

	/**
	 *
	 * @param file
	 * @param baseURI
	 * @param dataFormat
	 * @param contexts
	 * @throws RDFParseException
	 */
    public void sendAdd(File file, String baseURI, RDFFormat dataFormat, Resource... contexts) throws RDFParseException {
		getClient().performAdd(file, baseURI, dataFormat, this.tx, contexts);
    }

	/**
	 *
	 * @param in
	 * @param baseURI
	 * @param dataFormat
	 * @param contexts
	 */
	public void sendAdd(InputStream in, String baseURI, RDFFormat dataFormat, Resource... contexts){
		getClient().performAdd(in, baseURI, dataFormat, this.tx, contexts);
	}

	/**
	 *
	 * @param in
	 * @param baseURI
	 * @param dataFormat
	 * @param contexts
	 */
	public void sendAdd(Reader in, String baseURI, RDFFormat dataFormat, Resource... contexts){
		//TBD- must deal with char encoding
		getClient().performAdd(new ReaderInputStream(in), baseURI, dataFormat, this.tx, contexts);
	}

	/**
	 *
	 * @param baseURI
	 * @param subject
	 * @param predicate
	 * @param object
	 * @param contexts
	 */
	public void sendAdd(String baseURI, Resource subject, URI predicate, Value object, Resource... contexts){
		getClient().performAdd(baseURI, (Resource) skolemize(subject), (URI) skolemize(predicate), skolemize(object), this.tx, contexts);
	}

	/**
	 *
	 * @param baseURI
	 * @param subject
	 * @param predicate
	 * @param object
	 * @param contexts
	 */
	public void sendRemove(String baseURI, Resource subject,URI predicate, Value object, Resource... contexts){
		getClient().performRemove(baseURI, (Resource) skolemize(subject), (URI) skolemize(predicate), skolemize(object), this.tx, contexts);
	}

	/**
	 *
	 * @param contexts
	 */
	public void sendClear(Resource... contexts){
		getClient().performClear(this.tx, contexts);
	}
	public void sendClearAll(){
		getClient().performClearAll(this.tx);
	}

	/**
	 *
	 * @throws MarkLogicTransactionException
	 */
	public void openTransaction() throws MarkLogicTransactionException {
        if (!isActiveTransaction()) {
            this.tx = getClient().getDatabaseClient().openTransaction();
        }else{
            throw new MarkLogicTransactionException("Only one active transaction allowed.");
        }
	}

	/**
	 *
	 * @throws MarkLogicTransactionException
	 */
	public void commitTransaction() throws MarkLogicTransactionException {
        if (isActiveTransaction()) {
            this.tx.commit();
            this.tx=null;
        }else{
            throw new MarkLogicTransactionException("No active transaction to commit.");
        }
	}

	/**
	 *
	 * @throws MarkLogicTransactionException
	 */
	public void rollbackTransaction() throws MarkLogicTransactionException {
		if(this.tx != null) {
			this.tx.rollback();
			this.tx = null;
		}else{
			throw new MarkLogicTransactionException("No active transaction to rollback.");
		}
	}

	/**
	 *
	 * @return
	 */
	public boolean isActiveTransaction(){
		return this.tx != null;
	}

	/**
	 *
	 * @throws MarkLogicTransactionException
	 */
	public void setAutoCommit() throws MarkLogicTransactionException {
        if (isActiveTransaction()) {
            throw new MarkLogicTransactionException("Active transaction.");
        }else{
            this.tx=null;
        }
	}

	/**
	 *
	 * @return
	 */
	public ParserConfig getParserConfig() {
		return this.parserConfig;
	}

	/**
	 *
	 * @param parserConfig
	 */
	public void setParserConfig(ParserConfig parserConfig) {
		this.parserConfig=parserConfig;
	}

	/**
	 *
	 * @param rulesets
	 */
	public void setRulesets(Object ... rulesets){
		getClient().setRulesets(rulesets);
	}

	/**
	 *
	 * @return
	 */
	public Object[] getRulesets(){
		return getClient().getRulesets();
	}

	/**
	 *
	 * @param constrainingQueryDefinition
	 */
    public void setConstrainingQueryDefinition(Object constrainingQueryDefinition){
		getClient().setConstrainingQueryDefinition(constrainingQueryDefinition);
    }

	/**
	 *
	 * @return
	 */
	public Object getConstrainingQueryDefinition(){
        return getClient().getConstrainingQueryDefinition();
    }

	/**
	 *
	 * @param graphPerms
	 */
    public void setGraphPerms(Object graphPerms){
        getClient().setGraphPerms(graphPerms);
    }

	/**
	 *
	 * @return
	 */
	public Object getGraphPerms(){
        return getClient().getGraphPerms();
    }


	/**
	 *
	 * @param command
	 */
	protected void execute(Runnable command) {
		executor.execute(command);
	}


    ///////////////////////////////////////////////////////////////////////////////////////////////
    // private ////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 *
	 * @return
	 */
	private MarkLogicClientImpl getClient(){
		return this._client;
	}

	/**
	 *
	 * @param s
	 * @return
	 */
	private Value skolemize(Value s) {
		if (s instanceof org.openrdf.model.BNode) {
			return getValueFactory().createURI("http://marklogic.com/semantics/blank/" + s.toString());
		} else {
			return s;
		}
	}
}