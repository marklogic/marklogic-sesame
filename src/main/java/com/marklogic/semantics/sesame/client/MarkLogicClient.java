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
import org.openrdf.http.client.BackgroundGraphResult;
import org.openrdf.http.client.BackgroundTupleResult;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.resultio.QueryResultIO;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultParser;
import org.openrdf.repository.sparql.query.SPARQLQueryBindingSet;
import org.openrdf.rio.ParserConfig;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.ParseErrorLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 *
 * @author James Fuller
 */
public class MarkLogicClient {

	protected final Logger logger = LoggerFactory.getLogger(MarkLogicClient.class);

	protected static final Charset UTF8 = Charset.forName("UTF-8");

	protected static final TupleQueryResultFormat format = TupleQueryResultFormat.JSON;
	protected static final RDFFormat rdfFormat = RDFFormat.NTRIPLES;

    protected static final Charset charset = UTF8;

	private static Executor executor = Executors.newCachedThreadPool();

	private MarkLogicClientImpl _client;

	private ValueFactory f;

	private ParserConfig parserConfig = new ParserConfig();

	private Transaction tx = null;

	public MarkLogicClient(String host, int port, String user, String password,String auth) {
		this._client = new MarkLogicClientImpl(host,port,user,password,auth);
	}

	public ValueFactory getValueFactory() {
		return this.f;
	}

	public void setValueFactory(ValueFactory f) {
		this.f=f;
	}

	//tuple query
	public TupleQueryResult sendTupleQuery(String queryString,SPARQLQueryBindingSet bindings, long start, long pageLength, boolean includeInferred) throws IOException {
		InputStream stream = _client.performSPARQLQuery(queryString, bindings, start, pageLength, this.tx, includeInferred);
		TupleQueryResultParser parser = QueryResultIO.createParser(format, getValueFactory());
		BackgroundTupleResult tRes = new BackgroundTupleResult(parser,stream);
		execute(tRes);
		return tRes;
	}

	//graph query
	public GraphQueryResult sendGraphQuery(String queryString, SPARQLQueryBindingSet bindings, boolean includeInferred) throws IOException {
		InputStream stream = _client.performGraphQuery(queryString, bindings, this.tx, includeInferred);
		RDFParser parser = Rio.createParser(rdfFormat, getValueFactory());
		parser.setParserConfig(getParserConfig());
		parser.setParseErrorListener(new ParseErrorLogger());
		BackgroundGraphResult gRes = new BackgroundGraphResult(parser,stream,charset,"");
		execute(gRes);
		return gRes;
	}

	//boolean query
	public boolean sendBooleanQuery(String queryString, SPARQLQueryBindingSet bindings, boolean includeInferred) {
		return _client.performBooleanQuery(queryString, bindings,this.tx,includeInferred);
	}

	//update query
	public void sendUpdateQuery(String queryString, SPARQLQueryBindingSet bindings, boolean includeInferred) {
		_client.performUpdateQuery(queryString, bindings, this.tx, includeInferred);
	}

	//add
    public void sendAdd(File file, String baseURI, RDFFormat dataFormat, Resource... contexts){
        _client.performAdd(file, baseURI, dataFormat, this.tx, contexts);
    }
	public void sendAdd(InputStream in, String baseURI, RDFFormat dataFormat, Resource... contexts){
		_client.performAdd(in, baseURI, dataFormat, this.tx, contexts);
	}
	public void sendAdd(Resource subject,URI predicate, Value object, Resource... contexts){
		_client.performAdd(subject, predicate, object, this.tx, contexts);
	}

	//remove
	public void sendRemove(Resource subject,URI predicate, Value object, Resource... contexts){
		_client.performRemove(subject, predicate, object, this.tx, contexts);
	}

	//clear
	public void sendClear(Resource... contexts){
		_client.performClear(this.tx, contexts);
	}
	public void sendClearAll(){
		_client.performClearAll(this.tx);
	}

	//transaction
	public void openTransaction(){
		tx = _client.getDatabaseClient().openTransaction();
	}
	public void commitTransaction(){
		tx.commit();
		tx=null;
	}
	public void rollbackTransaction(){
		tx.rollback();
		tx=null;
	}
	public boolean isActiveTransaction(){
		return tx instanceof Transaction;
	}
	public void setAutoCommit(){
		//TBD-what to do if active ?
		this.tx=null;
	}

	//parser
	public ParserConfig getParserConfig() {
		return this.parserConfig;
	}
	public void setParserConfig(ParserConfig parserConfig) {
		this.parserConfig=parserConfig;
	}

	//execute
	protected void execute(Runnable command) {
		executor.execute(command);
	}

	private Value skolemize(Value s) {
		if (s instanceof org.openrdf.model.BNode) {
			return ValueFactoryImpl.getInstance().createURI("http://marklogic.com/semantics/blank/" + s.toString());
		} else {
			return s;
		}
	}
}