package com.marklogic.semantics.sesame;

import org.junit.*;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.*;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class MarkLogicRepositoryConnectionTest {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    private Repository rep;

    protected RepositoryConnection conn;

    protected ValueFactory f;

    @Before
    public void setUp()
            throws Exception {
        logger.debug("setting up test");

        // extrude to semantics.utils
        Properties props = new Properties();
        try {
            props.load(new FileInputStream("gradle.properties"));
        } catch (IOException e) {
            System.err.println("problem loading properties file.");
            System.exit(1);
        }
        String host = props.getProperty("mlHost");
        int port = Integer.parseInt(props.getProperty("mlRestPort"));
        String user = props.getProperty("mlAdminUsername");
        String pass = props.getProperty("mlAdminPassword");
        // extrude to semantics.utils

        this.rep = new MarkLogicRepository(host, port, user, pass, "DIGEST");
        rep.initialize();

        f = rep.getValueFactory();
        conn = rep.getConnection();

        logger.info("test setup complete.");
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown()
            throws Exception {
        logger.debug("tearing down...");
        conn.close();
        conn = null;

        rep.shutDown();
        rep = null;

        logger.info("tearDown complete.");
    }

    @Test
    public void testSPARQLQuery()
            throws Exception {

        rep.shutDown();
        rep.initialize();

        Assert.assertTrue(conn != null);
        String queryString = "select ?s ?p ?o { ?s ?p ?o } limit 2 ";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
        TupleQueryResult results = tupleQuery.evaluate();

        Assert.assertEquals(results.getBindingNames().get(0), "s");
        Assert.assertEquals(results.getBindingNames().get(1), "p");
        Assert.assertEquals(results.getBindingNames().get(2), "o");

        results.hasNext();
        BindingSet bindingSet = results.next();

        Value sV = bindingSet.getValue("s");
        Value pV = bindingSet.getValue("p");
        Value oV = bindingSet.getValue("o");

        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#AlexandriaGeodata", sV.stringValue());
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#altitude", pV.stringValue());
        Assert.assertEquals("0", oV.stringValue());

        results.hasNext();
        BindingSet bindingSet1 = results.next();

        Value sV1 = bindingSet1.getValue("s");
        Value pV1 = bindingSet1.getValue("p");
        Value oV1 = bindingSet1.getValue("o");

        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#AmphipolisGeodata", sV1.stringValue());
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#altitude", pV1.stringValue());
        Assert.assertEquals("0", oV1.stringValue());

    }

    @Test
    public void testSPARQLQueryWithResultsHandler()
            throws Exception {

        rep.shutDown();
        rep.initialize();

        Assert.assertTrue(conn != null);
        String queryString = "select ?s ?p ?o { ?s ?p ?o } limit 10";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);

        tupleQuery.evaluate(new TupleQueryResultHandler() {
            @Override
            public void startQueryResult(List<String> bindingNames) {
                Assert.assertEquals(bindingNames.get(0), "s");
                Assert.assertEquals(bindingNames.get(1), "p");
                Assert.assertEquals(bindingNames.get(2), "o");
            }

            @Override
            public void handleSolution(BindingSet bindingSet) {
                Assert.assertEquals(bindingSet.getBinding("o").getValue().stringValue(), "0");
            }

            @Override
            public void endQueryResult() {
            }

            @Override
            public void handleBoolean(boolean arg0)
                    throws QueryResultHandlerException {
            }

            @Override
            public void handleLinks(List<String> arg0)
                    throws QueryResultHandlerException {
            }
        });
        tupleQuery.evaluate();

    }

    @Test
    public void testSPARQLQueryBindings()
            throws Exception {

        rep.shutDown();
        rep.initialize();

        Assert.assertTrue(conn != null);
        String queryString = "select ?s ?p ?o { ?s ?p ?o . filter (?s = ?b) filter (?p = ?c) }";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);

        tupleQuery.setBinding("b", ValueFactoryImpl.getInstance().createURI("http://semanticbible.org/ns/2006/NTNames#Jim"));
        tupleQuery.setBinding("c", ValueFactoryImpl.getInstance().createURI("http://semanticbible.org/ns/2006/NTNames#parentOf"));

        tupleQuery.removeBinding("c");

        tupleQuery.clearBindings();

        tupleQuery.setBinding("b", ValueFactoryImpl.getInstance().createURI("http://semanticbible.org/ns/2006/NTNames#Jotham"));
        tupleQuery.setBinding("c", ValueFactoryImpl.getInstance().createURI("http://semanticbible.org/ns/2006/NTNames#parentOf"));

        TupleQueryResult results = tupleQuery.evaluate();

        Assert.assertEquals(results.getBindingNames().get(0), "s");
        Assert.assertEquals(results.getBindingNames().get(1), "p");
        Assert.assertEquals(results.getBindingNames().get(2), "o");

        logger.info(results.getBindingNames().toString());

        results.hasNext();
        BindingSet bindingSet = results.next();

        Value sV = bindingSet.getValue("s");
        Value pV = bindingSet.getValue("p");
        Value oV = bindingSet.getValue("o");

        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#Jotham", sV.stringValue());
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#parentOf", pV.stringValue());
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#Ahaz", oV.stringValue());

    }

    @Ignore
    public void testContextIDs()
            throws Exception {

        RepositoryResult<Statement> result = conn.getStatements(RDF.TYPE, RDF.TYPE, null, true);
        try {
            Assert.assertTrue("result should not be empty", result.hasNext());
        } finally {
            result.close();
        }

        result = conn.getStatements(RDF.TYPE, RDF.TYPE, null, false);
        try {
            Assert.assertFalse("result should be empty", result.hasNext());
        } finally {
            result.close();
        }
    }


}
