package com.marklogic.semantics.sesame.client;

import org.openrdf.http.client.BackgroundGraphResult;
import org.openrdf.http.client.BackgroundTupleResult;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.query.resultio.QueryResultIO;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultParser;
import org.openrdf.rio.ParserConfig;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.ParseErrorLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	public MarkLogicClient(String host, int port, String user, String password,String auth) {
		this._client = new MarkLogicClientImpl(host,port,user,password,auth);
		this.f = new ValueFactoryImpl();
	}

	public ValueFactory getValueFactory() {
		return this.f;
	}

	public void setValueFactory(ValueFactory f) {
		this.f=f;
	}

	//tuple query
	public TupleQueryResult sendTupleQuery(String queryString,MapBindingSet bindings, long start, long pageLength) throws IOException {
		InputStream stream = _client.performSPARQLQuery(queryString,bindings,start,pageLength,null);
		TupleQueryResultParser parser = QueryResultIO.createParser(format, getValueFactory());
		BackgroundTupleResult tRes = new BackgroundTupleResult(parser,stream);
		execute(tRes);
		return tRes;
	}

	//graph query
	public GraphQueryResult sendGraphQuery(String queryString,MapBindingSet bindings) throws IOException {
		InputStream stream = _client.performGraphQuery(queryString, bindings, null);
		RDFParser parser = Rio.createParser(rdfFormat, getValueFactory());
		parser.setParserConfig(getParserConfig());
		parser.setParseErrorListener(new ParseErrorLogger());
		BackgroundGraphResult gRes = new BackgroundGraphResult(parser,stream,charset,"");
		execute(gRes);
		return gRes;
	}

	//boolean query
	public boolean sendBooleanQuery(String queryString,MapBindingSet bindings) {
		return _client.performBooleanQuery(queryString, bindings,null);
	}

	//update query
	public void sendUpdateQuery(String queryString,MapBindingSet bindings) {
		_client.performUpdateQuery(queryString, bindings,null);
	}

	public ParserConfig getParserConfig() {
		return parserConfig;
	}
	public void setParserConfig(ParserConfig parserConfig) {
		this.parserConfig=parserConfig;
	}

	protected void execute(Runnable command) {
		executor.execute(command);
	}
}
