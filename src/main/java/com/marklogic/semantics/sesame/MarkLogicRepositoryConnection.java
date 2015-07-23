package com.marklogic.semantics.sesame;

import com.marklogic.semantics.sesame.client.MarkLogicClient;
import com.marklogic.semantics.sesame.query.*;
import info.aduna.iteration.*;
import org.openrdf.IsolationLevel;
import org.openrdf.model.*;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.*;
import org.openrdf.query.impl.DatasetImpl;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.query.parser.QueryParserUtil;
import org.openrdf.repository.*;
import org.openrdf.rio.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URL;

import static org.openrdf.query.QueryLanguage.SPARQL;

public class MarkLogicRepositoryConnection implements RepositoryConnection {

    protected final Logger logger = LoggerFactory.getLogger(MarkLogicRepositoryConnection.class);

    private static final String EVERYTHING = "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }";

    private static final String EVERYTHING_WITH_GRAPH = "SELECT * WHERE {  ?s ?p ?o . OPTIONAL { GRAPH ?ctx { ?s ?p ?o } } }";

    private static final String SOMETHING = "ASK { ?s ?p ?o }";

    private static final String NAMEDGRAPHS = "SELECT DISTINCT ?_ WHERE { GRAPH ?_ { ?s ?p ?o } }";

    private StringBuffer sparqlTransaction;

    private final Object transactionLock = new Object();

    private final boolean quadMode;

    private MarkLogicClient client;

    private MarkLogicRepository repository;

//    public MarkLogicRepositoryConnection(MarkLogicRepository repository,MarkLogicClient client) {
//        this(repository, new MarkLogicClient(),false);
//    }

    public MarkLogicRepositoryConnection(MarkLogicRepository repository, MarkLogicClient client, boolean quadMode) {
        super();
        this.client = client;
        this.repository = repository;
        this.quadMode = quadMode;
    }

    @Override
    public Repository getRepository() {
        return repository;
    }

    @Override
    public ParserConfig getParserConfig() {
        return client.getParserConfig();
    }

    @Override
    public void setParserConfig(ParserConfig config) {
        client.setParserConfig(config);
    }

    @Override
    public ValueFactory getValueFactory() {
        return client.getValueFactory();
    }
    public void setValueFactory(ValueFactory f) {
        client.setValueFactory(f);
    }

    @Override
    public boolean isOpen() throws RepositoryException {
        return false;
    }

    @Override
    public void close() throws RepositoryException {
    }

    // prepareQuery entrypoint
    @Override
    public Query prepareQuery(QueryLanguage queryLanguage, String queryString) throws RepositoryException, MalformedQueryException {
        return prepareTupleQuery(queryLanguage, queryString, "");
    }
    @Override
    public Query prepareQuery(QueryLanguage queryLanguage, String queryString, String baseURI)
            throws RepositoryException, MalformedQueryException
    {
        if (SPARQL.equals(queryLanguage)) {
            String strippedQuery = QueryParserUtil.removeSPARQLQueryProlog(queryString).toUpperCase();
            if (strippedQuery.startsWith("SELECT")) {
                return prepareTupleQuery(queryLanguage, queryString, baseURI);
            }
            else if (strippedQuery.startsWith("ASK")) {
                return prepareBooleanQuery(queryLanguage, queryString, baseURI);
            }
            else {
                return prepareGraphQuery(queryLanguage, queryString, baseURI);
            }
        }
        throw new UnsupportedOperationException("Unsupported query language " + queryLanguage.getName());
    }


    // prepareTupleQuery
    public TupleQuery prepareTupleQuery(String queryString) throws RepositoryException, MalformedQueryException {
        return prepareTupleQuery(QueryLanguage.SPARQL, queryString);
    }
    @Override
    public TupleQuery prepareTupleQuery(QueryLanguage queryLanguage, String queryString) throws RepositoryException, MalformedQueryException {
        return prepareTupleQuery(queryLanguage, queryString, "");
    }
    @Override
    public TupleQuery prepareTupleQuery(QueryLanguage queryLanguage, String queryString, String baseURI) throws RepositoryException, MalformedQueryException {
        if (QueryLanguage.SPARQL.equals(queryLanguage)) {
            return new MarkLogicTupleQuery(client, new MapBindingSet(), baseURI, queryString);
        }
        throw new UnsupportedQueryLanguageException("Unsupported query language " + queryLanguage.getName());
    }

    //prepareGraphQuery
    public GraphQuery prepareGraphQuery(String queryString) throws RepositoryException, MalformedQueryException {
        return prepareGraphQuery(QueryLanguage.SPARQL, queryString, "");
    }
    @Override
    public GraphQuery prepareGraphQuery(QueryLanguage queryLanguage, String queryString) throws RepositoryException, MalformedQueryException {
        return prepareGraphQuery(queryLanguage, queryString, "");
    }
    @Override
    public GraphQuery prepareGraphQuery(QueryLanguage queryLanguage, String queryString, String baseURI)
            throws RepositoryException, MalformedQueryException
    {
        if (QueryLanguage.SPARQL.equals(queryLanguage)) {
            return new MarkLogicGraphQuery(client, new MapBindingSet(), baseURI, queryString);
        }
        throw new UnsupportedQueryLanguageException("Unsupported query language " + queryLanguage.getName());
    }

    //prepareBooleanQuery
    public BooleanQuery prepareBooleanQuery(String queryString) throws RepositoryException, MalformedQueryException {
        return prepareBooleanQuery(QueryLanguage.SPARQL,queryString, "");
    }
    @Override
    public BooleanQuery prepareBooleanQuery(QueryLanguage queryLanguage, String queryString) throws RepositoryException, MalformedQueryException {
        return prepareBooleanQuery(queryLanguage, queryString, "");
    }
    @Override
    public BooleanQuery prepareBooleanQuery(QueryLanguage queryLanguage, String queryString, String baseURI) throws RepositoryException, MalformedQueryException {
        if (QueryLanguage.SPARQL.equals(queryLanguage)) {
            return new MarkLogicBooleanQuery(client, new MapBindingSet(), baseURI, queryString);
        }
        throw new UnsupportedQueryLanguageException("Unsupported query language " + queryLanguage.getName());
    }

    //prepareUpdate
    @Override
    public Update prepareUpdate(QueryLanguage queryLanguage, String queryString) throws RepositoryException, MalformedQueryException {
       return prepareUpdate(queryLanguage,queryString,"");
    }
    @Override
    public Update prepareUpdate(QueryLanguage queryLanguage, String queryString, String baseURI) throws RepositoryException, MalformedQueryException {
        if (QueryLanguage.SPARQL.equals(queryLanguage)) {
            return (Update) new MarkLogicUpdateQuery(client, new MapBindingSet(), baseURI, queryString);
        }
        throw new UnsupportedQueryLanguageException("Unsupported query language " + queryLanguage.getName());
    }

    //
    @Override
    public RepositoryResult<Resource> getContextIDs() throws RepositoryException {

        try{
            String queryString = "SELECT DISTINCT ?_ WHERE { GRAPH ?_ { ?s ?p ?o } }";
            TupleQuery tupleQuery = prepareTupleQuery(QueryLanguage.SPARQL, queryString);
            TupleQueryResult result = tupleQuery.evaluate();
            return
                    new RepositoryResult<Resource>(
                            new ExceptionConvertingIteration<Resource, RepositoryException>(
                                    new ConvertingIteration<BindingSet, Resource, QueryEvaluationException>(result) {

                                        @Override
                                        protected Resource convert(BindingSet bindings)
                                                throws QueryEvaluationException {
                                            return (Resource) bindings.getValue("_");
                                        }
                                    }) {

                                @Override
                                protected RepositoryException convert(Exception e) {
                                    return new RepositoryException(e);
                                }
                            });

        } catch (MalformedQueryException e) {
            throw new RepositoryException(e);
        } catch (QueryEvaluationException e) {
            throw new RepositoryException(e);
        }
    }

    @Override
    public RepositoryResult<Statement> getStatements(Resource subj, URI pred, Value obj, boolean includeInferred, Resource... contexts) throws RepositoryException {
        try {
            if (isQuadMode()) {
                TupleQuery tupleQuery = prepareTupleQuery(SPARQL, EVERYTHING_WITH_GRAPH);
                setBindings(tupleQuery, subj, pred, obj, contexts);
                tupleQuery.setIncludeInferred(includeInferred);
                TupleQueryResult qRes = tupleQuery.evaluate();
                return new RepositoryResult<Statement>(
                        new ExceptionConvertingIteration<Statement, RepositoryException>(
                                toStatementIteration(qRes, subj, pred, obj)) {
                            @Override
                            protected RepositoryException convert(Exception e) {
                                return new RepositoryException(e);
                            }
                        });
            }
            if (subj != null && pred != null && obj != null) {
                if (hasStatement(subj, pred, obj, includeInferred, contexts)) {
                    Statement st = new StatementImpl(subj, pred, obj);
                    CloseableIteration<Statement, RepositoryException> cursor;
                    cursor = new SingletonIteration<Statement, RepositoryException>(st);
                    return new RepositoryResult<Statement>(cursor);
                } else {
                    return new RepositoryResult<Statement>(new EmptyIteration<Statement, RepositoryException>());
                }
            }
            GraphQuery query = prepareGraphQuery(SPARQL, EVERYTHING, "");
            setBindings(query, subj, pred, obj, contexts);
            GraphQueryResult result = query.evaluate();
            return new RepositoryResult<Statement>(
                    new ExceptionConvertingIteration<Statement, RepositoryException>(result) {

                        @Override
                        protected RepositoryException convert(Exception e) {
                            return new RepositoryException(e);
                        }
                    });
        } catch (MalformedQueryException e) {
            throw new RepositoryException(e);
        } catch (QueryEvaluationException e) {
            throw new RepositoryException(e);
        }
    }

    //
    @Override
    public boolean hasStatement(Resource subj, URI pred, Value obj, boolean includeInferred, Resource... contexts) throws RepositoryException {
        try {
            BooleanQuery query = prepareBooleanQuery(SPARQL, SOMETHING, "");
            setBindings(query, subj, pred, obj, contexts);
            return query.evaluate();
        }
        catch (MalformedQueryException e) {
            throw new RepositoryException(e);
        }
        catch (QueryEvaluationException e) {
            throw new RepositoryException(e);
        }
    }

    @Override
    public boolean hasStatement(Statement st, boolean includeInferred, Resource... contexts) throws RepositoryException {
        return false;
    }

    @Override
    public void exportStatements(Resource subj, URI pred, Value obj, boolean includeInferred, RDFHandler handler, Resource... contexts) throws RepositoryException, RDFHandlerException {

    }

    //
    @Override
    public void export(RDFHandler handler, Resource... contexts) throws RepositoryException, RDFHandlerException {

    }

    @Override
    public long size(Resource... contexts) throws RepositoryException {
        return 0;
    }

    @Override
    public boolean isEmpty() throws RepositoryException {
        return false;
    }

    @Override
    public boolean isAutoCommit() throws RepositoryException {
        return false;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws RepositoryException {

    }

    @Override
    public boolean isActive() throws UnknownTransactionStateException, RepositoryException {
        return false;
    }

    @Override
    public IsolationLevel getIsolationLevel() {
        return null;
    }

    @Override
    public void setIsolationLevel(IsolationLevel level) throws IllegalStateException {

    }

    @Override
    public void begin() throws RepositoryException {
        synchronized (transactionLock) {
            if (!isActive()) {
                synchronized (transactionLock) {
                    sparqlTransaction = new StringBuffer();
                }
            }
            else {
                throw new RepositoryException("active transaction already exists");
            }
        }
    }

    @Override
    public void begin(IsolationLevel level) throws RepositoryException {

    }

    @Override
    public void commit() throws RepositoryException {
        synchronized (transactionLock) {
            if (isActive()) {
                synchronized (transactionLock) {
                    MarkLogicUpdate transaction = new MarkLogicUpdate(client, null,
                            sparqlTransaction.toString());
                    try {
                        transaction.execute();
                    }
                    catch (UpdateExecutionException e) {
                        throw new RepositoryException("error executing transaction", e);
                    }

                    sparqlTransaction = null;
                }
            }
            else {
                throw new RepositoryException("no transaction active.");
            }
        }
    }

    @Override
    public void rollback() throws RepositoryException {
        synchronized (transactionLock) {
            if (isActive()) {
                synchronized (transactionLock) {
                    sparqlTransaction = null;
                }
            }
            else {
                throw new RepositoryException("no transaction active.");
            }
        }
    }

    @Override
    public void add(InputStream in, String baseURI, RDFFormat dataFormat, Resource... contexts) throws IOException, RDFParseException, RepositoryException {

    }

    @Override
    public void add(Reader reader, String baseURI, RDFFormat dataFormat, Resource... contexts) throws IOException, RDFParseException, RepositoryException {

    }

    @Override
    public void add(URL url, String baseURI, RDFFormat dataFormat, Resource... contexts) throws IOException, RDFParseException, RepositoryException {

    }

    @Override
    public void add(File file, String baseURI, RDFFormat dataFormat, Resource... contexts) throws IOException, RDFParseException, RepositoryException {

    }

    @Override
    public void add(Resource subject, URI predicate, Value object, Resource... contexts) throws RepositoryException {

    }

    @Override
    public void add(Statement st, Resource... contexts) throws RepositoryException {

    }

    @Override
    public void add(Iterable<? extends Statement> statements, Resource... contexts) throws RepositoryException {

    }

    @Override
    public <E extends Exception> void add(Iteration<? extends Statement, E> statements, Resource... contexts) throws RepositoryException, E {

    }

    @Override
    public void remove(Resource subject, URI predicate, Value object, Resource... contexts) throws RepositoryException {

    }

    @Override
    public void remove(Statement st, Resource... contexts) throws RepositoryException {

    }

    @Override
    public void remove(Iterable<? extends Statement> statements, Resource... contexts) throws RepositoryException {

    }

    @Override
    public <E extends Exception> void remove(Iteration<? extends Statement, E> statements, Resource... contexts) throws RepositoryException, E {

    }

    @Override
    public void clear(Resource... contexts) throws RepositoryException {

    }


    @Override
    public RepositoryResult<Namespace> getNamespaces() throws RepositoryException {
        return null;
    }

    @Override
    public String getNamespace(String prefix) throws RepositoryException {
        return null;
    }

    @Override
    public void setNamespace(String prefix, String name) throws RepositoryException {

    }

    @Override
    public void removeNamespace(String prefix) throws RepositoryException {

    }

    @Override
    public void clearNamespaces() throws RepositoryException {

    }

    private void setBindings(Query query, Resource subj, URI pred, Value obj, Resource... contexts)
            throws RepositoryException {
        if (subj != null) {
            query.setBinding("s", subj);
        }
        if (pred != null) {
            query.setBinding("p", pred);
        }
        if (obj != null) {
            query.setBinding("o", obj);
        }
        if (contexts != null && contexts.length > 0) {
            DatasetImpl dataset = new DatasetImpl();
            for (Resource ctx : contexts) {
                if (ctx == null || ctx instanceof URI) {
                    dataset.addDefaultGraph((URI) ctx);
                } else {
                    throw new RepositoryException("Contexts must be URIs");
                }
            }
            query.setDataset(dataset);
        }
    }

    protected boolean isQuadMode() {
        return quadMode;
    }

    protected Iteration<Statement, QueryEvaluationException> toStatementIteration(TupleQueryResult iter, final Resource subj, final URI pred, final Value obj) {

        return new ConvertingIteration<BindingSet, Statement, QueryEvaluationException>(iter) {

            @Override
            protected Statement convert(BindingSet b) throws QueryEvaluationException {

                Resource s = subj == null ? (Resource) b.getValue("s") : subj;
                URI p = pred == null ? ValueFactoryImpl.getInstance().createURI(b.getValue("o").stringValue()) : pred;
                Value o = obj == null ? b.getValue("o") : obj;
                Resource ctx = (Resource) b.getValue("ctx");

                return ValueFactoryImpl.getInstance().createStatement(s, p, o, ctx);
            }

        };
    }
}
