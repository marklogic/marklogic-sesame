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
package com.marklogic.semantics.sesame;

import com.marklogic.client.semantics.SPARQLRuleset;
import com.marklogic.semantics.sesame.query.MarkLogicTupleQuery;
import info.aduna.iteration.ConvertingIteration;
import info.aduna.iteration.ExceptionConvertingIteration;
import info.aduna.iteration.Iteration;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.openrdf.model.*;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.*;
import org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.repository.sparql.SPARQLRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;
import org.openrdf.rio.rdfxml.RDFXMLWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Properties;

/**
 *
 * @author James Fuller
 */
// @FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MarkLogicRepositoryConnectionTest extends SesameTestBase {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected MarkLogicRepositoryConnection conn;
    protected ValueFactory f;

    @Before
    public void setUp()
            throws Exception {
        logger.debug("setting up test");
        rep.initialize();
        f = rep.getValueFactory();
        conn =rep.getConnection();
        logger.info("test setup complete.");
    }

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
    public void testMarkLogicRepositoryConnectionOpen()
            throws Exception {
        Assert.assertEquals(true, conn.isOpen());
    }

    @Test
    public void testMarkLogicRepositoryConnection()
            throws Exception {

        Assert.assertNotNull("Expected repository to exist.", rep);
        Assert.assertTrue("Expected repository to be initialized.", rep.isInitialized());
        rep.shutDown();
        Properties props = new Properties();
        try {
            props.load(new FileInputStream("gradle.properties"));
        } catch (IOException e) {
            System.err.println("problem loading properties file.");
            System.exit(1);
        }
        String host = props.getProperty("mlHost");
        int port = Integer.parseInt(props.getProperty("mlRestPort"));
        String user = props.getProperty("mlUsername");
        String pass = props.getProperty("mlPassword");

        rep = new MarkLogicRepository(host, port, user, pass, "DIGEST");

        Assert.assertNotNull("Expected repository to exist.", rep);
        Assert.assertFalse("Expected repository to not be initialized.", rep.isInitialized());
        rep.initialize();
        Assert.assertTrue("Expected repository to be initialized.", rep.isInitialized());
        rep.shutDown();
        Assert.assertFalse("Expected repository to not be initialized.", rep.isInitialized());
        rep.initialize();
        Assert.assertNotNull("Expected repository to exist.", rep);
    }

    @Test
    public void testSPARQLQueryWithPrepareQuery()
            throws Exception {

        String queryString = "select ?s ?p ?o { ?s ?p ?o } limit 1 ";
        Query q = conn.prepareQuery(QueryLanguage.SPARQL, queryString);

        if (q instanceof TupleQuery) {
            TupleQueryResult result = ((TupleQuery) q).evaluate();
            while (result.hasNext()) {
                BindingSet tuple = result.next();
                Assert.assertEquals("s", tuple.getBinding("s").getName());
                Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#AlexandriaGeodata", tuple.getBinding("s").getValue().stringValue());
            }
        }
    }

    @Test
    public void testSPARQLQuery()
            throws Exception {

        String queryString = "select * { ?s ?p ?o } limit 2 ";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
        TupleQueryResult results = tupleQuery.evaluate();

        Assert.assertEquals(results.getBindingNames().get(0), "s");
        Assert.assertEquals(results.getBindingNames().get(1), "p");
        Assert.assertEquals(results.getBindingNames().get(2), "o");

        BindingSet bindingSet = results.next();

        Value sV = bindingSet.getValue("s");
        Value pV = bindingSet.getValue("p");
        Value oV = bindingSet.getValue("o");

        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#AlexandriaGeodata", sV.stringValue());
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#altitude", pV.stringValue());
        Assert.assertEquals("0", oV.stringValue());

        BindingSet bindingSet1 = results.next();

        Value sV1 = bindingSet1.getValue("s");
        Value pV1 = bindingSet1.getValue("p");
        Value oV1 = bindingSet1.getValue("o");

        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#AmphipolisGeodata", sV1.stringValue());
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#altitude", pV1.stringValue());
        Assert.assertEquals("0", oV1.stringValue());
    }

    @Test
    public void testPrepareTupleQueryQueryStringMethod() throws Exception{
        String queryString = "select ?s ?p ?o { ?s ?p ?o } limit 10 ";
        TupleQuery tupleQuery = conn.prepareTupleQuery(queryString);
        tupleQuery = conn.prepareTupleQuery(queryString,"http://marklogic.com/test/baseuri");
        TupleQueryResult results = tupleQuery.evaluate();

        Assert.assertEquals(results.getBindingNames().get(0), "s");
        Assert.assertEquals(results.getBindingNames().get(1), "p");
        Assert.assertEquals(results.getBindingNames().get(2), "o");
    }

    @Test
    public void testSPARQLQueryWithDefaultInferred()
            throws Exception {

        String queryString = "select ?s ?p ?o { ?s ?p ?o } limit 2 ";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
        tupleQuery.setIncludeInferred(true);
        TupleQueryResult results = tupleQuery.evaluate();
        Assert.assertEquals(results.getBindingNames().get(0), "s");
        Assert.assertEquals(results.getBindingNames().get(1), "p");
        Assert.assertEquals(results.getBindingNames().get(2), "o");

        BindingSet bindingSet = results.next();

        Value sV = bindingSet.getValue("s");
        Value pV = bindingSet.getValue("p");
        Value oV = bindingSet.getValue("o");

        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#AlexandriaGeodata", sV.stringValue());
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#altitude", pV.stringValue());
        Assert.assertEquals("0", oV.stringValue());

        BindingSet bindingSet1 = results.next();

        Value sV1 = bindingSet1.getValue("s");
        Value pV1 = bindingSet1.getValue("p");
        Value oV1 = bindingSet1.getValue("o");

        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#AmphipolisGeodata", sV1.stringValue());
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#altitude", pV1.stringValue());
        Assert.assertEquals("0", oV1.stringValue());
    }

    @Test
    public void testSPARQLQueryDistinct()
            throws Exception {

        try {
            String queryString = "SELECT DISTINCT ?_ WHERE { GRAPH ?_ { ?s ?p ?o } }";
            TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
            TupleQueryResult result = tupleQuery.evaluate();
            RepositoryResult<Resource> rr =
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

            Resource resource = rr.next();

            logger.debug(resource.stringValue());

        } catch (MalformedQueryException e) {
            throw new RepositoryException(e);
        } catch (QueryEvaluationException e) {
            throw new RepositoryException(e);
        }
    }

    @Test
    public void testSPARQLQueryWithPagination()
            throws Exception {
        String queryString = "select ?s ?p ?o { ?s ?p ?o } limit 100 ";
        MarkLogicTupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
        TupleQueryResult results = tupleQuery.evaluate(3, 1);

        Assert.assertEquals(results.getBindingNames().get(0), "s");
        Assert.assertEquals(results.getBindingNames().get(1), "p");
        Assert.assertEquals(results.getBindingNames().get(2), "o");

        BindingSet bindingSet = results.next();

        Value sV = bindingSet.getValue("s");
        Value pV = bindingSet.getValue("p");
        Value oV = bindingSet.getValue("o");

        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#AntiochGeodata", sV.stringValue());
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#altitude", pV.stringValue());
        Assert.assertEquals("0", oV.stringValue());

        Assert.assertFalse(results.hasNext());
    }

    @Test
    public void testSPARQLQueryWithRuleset()
            throws Exception {
        String queryString = "select ?s ?p ?o { ?s ?p ?o } limit 100 ";
        MarkLogicTupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);

        tupleQuery.setRulesets(SPARQLRuleset.RDFS_FULL);
        TupleQueryResult results = tupleQuery.evaluate();

        Assert.assertEquals(results.getBindingNames().get(0), "s");
        Assert.assertEquals(results.getBindingNames().get(1), "p");
        Assert.assertEquals(results.getBindingNames().get(2), "o");

        BindingSet bindingSet = results.next();

        Value sV = bindingSet.getValue("s");
        Value pV = bindingSet.getValue("p");
        Value oV = bindingSet.getValue("o");

        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#AlexandriaGeodata", sV.stringValue());
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#altitude", pV.stringValue());
        Assert.assertEquals("0", oV.stringValue());

    }

    @Test
    public void testSPARQLQueryWithResultsHandler()
            throws Exception {
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
        //tupleQuery.evaluate();
    }

    @Test
    public void testSPARQLQueryBindings()
            throws Exception {

        String queryString = "select ?s ?p ?o { ?s ?p ?o . filter (?s = ?b) filter (?p = ?c) }";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);

        tupleQuery.setBinding("b", ValueFactoryImpl.getInstance().createURI("http://semanticbible.org/ns/2006/NTNames#Jim"));
        tupleQuery.setBinding("c", ValueFactoryImpl.getInstance().createURI("http://semanticbible.org/ns/2006/NTNames#parentOf"));

        tupleQuery.removeBinding("c");

        // TBD -  Assert. for confirmation of removal

        Assert.assertEquals(null, tupleQuery.getBindings().getBinding("c"));

        tupleQuery.clearBindings();

        Assert.assertEquals(null, tupleQuery.getBindings().getBinding("b"));

        tupleQuery.setBinding("b", ValueFactoryImpl.getInstance().createURI("http://semanticbible.org/ns/2006/NTNames#Jotham"));
        tupleQuery.setBinding("c", ValueFactoryImpl.getInstance().createURI("http://semanticbible.org/ns/2006/NTNames#parentOf"));

        TupleQueryResult results = tupleQuery.evaluate();

        Assert.assertEquals(results.getBindingNames().get(0), "s");
        Assert.assertEquals(results.getBindingNames().get(1), "p");
        Assert.assertEquals(results.getBindingNames().get(2), "o");

        logger.info(results.getBindingNames().toString());

        Assert.assertTrue(results.hasNext());
        BindingSet bindingSet = results.next();

        Value sV = bindingSet.getValue("s");
        Value pV = bindingSet.getValue("p");
        Value oV = bindingSet.getValue("o");

        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#Jotham", sV.stringValue());
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#parentOf", pV.stringValue());
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#Ahaz", oV.stringValue());
    }


    @Test
    public void testSPARQLWithWriter()
            throws Exception {

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        SPARQLResultsXMLWriter sparqlWriter = new SPARQLResultsXMLWriter(out);

        String expected = "<?xml version='1.0' encoding='UTF-8'?>\n" +
                "<sparql xmlns='http://www.w3.org/2005/sparql-results#'>\n" +
                "\t<head>\n" +
                "\t\t<variable name='s'/>\n" +
                "\t\t<variable name='p'/>\n" +
                "\t\t<variable name='o'/>\n" +
                "\t</head>\n" +
                "\t<results>\n" +
                "\t\t<result>\n" +
                "\t\t\t<binding name='s'>\n" +
                "\t\t\t\t<uri>http://semanticbible.org/ns/2006/NTNames#AlexandriaGeodata</uri>\n" +
                "\t\t\t</binding>\n" +
                "\t\t\t<binding name='p'>\n" +
                "\t\t\t\t<uri>http://semanticbible.org/ns/2006/NTNames#altitude</uri>\n" +
                "\t\t\t</binding>\n" +
                "\t\t\t<binding name='o'>\n" +
                "\t\t\t\t<literal datatype='http://www.w3.org/2001/XMLSchema#int'>0</literal>\n" +
                "\t\t\t</binding>\n" +
                "\t\t</result>\n" +
                "\t</results>\n" +
                "</sparql>\n";

        String queryString = "select * { ?s ?p ?o . } limit 1";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);

        tupleQuery.evaluate(sparqlWriter);

        Assert.assertEquals(expected,out.toString());

    }

    @Ignore
    public void incrementallyBuildQueryTest() throws MalformedQueryException, RepositoryException {

        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, "");

//        StringBuffer sb = new StringBuffer();
//        sb.append("SELECT ?g ?s ?p ?o where {GRAPH ?g { ?s ?p ?o }");
//        if (s != Node) {
//            tupleQuery.setBinding("a", s.getURI());
//            sb.append("FILTER (?s = ?a) ");
//        }
//        if (p != Node.ANY) {
//            tupleQuery.setBinding("b", p.getURI());
//            sb.append("FILTER (?p = ?b) ");
//        }
//        if (o != Node.ANY) {
//            tupleQuery.setBinding("c",o);
//            sb.append("FILTER (?o = ?c) ");
//        }
//        sb.append("}");
//
//        qdef.setSparql(sb.toString());

    }

    @Test
    public void testConstructQuery()
            throws Exception {
        String queryString = "PREFIX nn: <http://semanticbible.org/ns/2006/NTNames#>\n" +
                "PREFIX test: <http://marklogic.com#test>\n" +
                "\n" +
                "construct { ?s  test:test \"0\"} where  {GRAPH <http://marklogic.com/test/my-graph> {?s nn:childOf nn:Eve . }}";
        GraphQuery graphQuery = conn.prepareGraphQuery(QueryLanguage.SPARQL, queryString);
        GraphQueryResult results = graphQuery.evaluate();
        Statement st1 = results.next();
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#Abel", st1.getSubject().stringValue());
        Statement st2 = results.next();
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#Cain", st2.getSubject().stringValue());
    }

    @Test
    public void testGraphQueryWithBaseURIInline()
            throws Exception {
        String queryString ="BASE <http://marklogic.com/test/baseuri>\n" +
                "PREFIX nn: <http://semanticbible.org/ns/2006/NTNames#>\n" +
                "PREFIX test: <http://marklogic.com#test>\n" +
                "construct { ?s  test:test <relative>} where  {GRAPH <http://marklogic.com/test/my-graph> {?s nn:childOf nn:Eve . }}";
        GraphQuery graphQuery = conn.prepareGraphQuery(QueryLanguage.SPARQL, queryString);
        GraphQueryResult results = graphQuery.evaluate();
        Statement st1 = results.next();
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#Abel", st1.getSubject().stringValue());
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#Abel", st1.getSubject().stringValue());
        Statement st2 = results.next();
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#Cain", st2.getSubject().stringValue());
    }

    @Test
    public void testGraphQueryWithBaseURI()
            throws Exception {
        String queryString =
                "PREFIX nn: <http://semanticbible.org/ns/2006/NTNames#>\n" +
                "PREFIX test: <http://marklogic.com#test>\n" +
                "construct { ?s  test:test <relative>} where  {GRAPH <http://marklogic.com/test/my-graph> {?s nn:childOf nn:Eve . }}";
        GraphQuery graphQuery = conn.prepareGraphQuery( queryString,"http://marklogic.com/test/baseuri");
        GraphQueryResult results = graphQuery.evaluate();
        Statement st1 = results.next();
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#Abel", st1.getSubject().stringValue());
        Statement st2 = results.next();
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#Cain", st2.getSubject().stringValue());
    }

    @Ignore
    public void testConstructQueryWithWriter()
            throws Exception {
        RDFWriter writer = Rio.createWriter(RDFFormat.TURTLE, System.out);

        String queryString = "PREFIX nn: <http://semanticbible.org/ns/2006/NTNames#>\n" +
                "PREFIX test: <http://marklogic.com#test>\n" +
                "\n" +
                "construct { ?s  test:test \"0\"} where  {GRAPH <http://marklogic.com/test/my-graph> {?s nn:childOf nn:Eve . }}";
        conn.prepareGraphQuery(QueryLanguage.SPARQL,
                "CONSTRUCT {?s ?p ?o } WHERE {?s ?p ?o } ").evaluate(writer);
    }


    @Test
    public void testDescribeQuery()
            throws Exception {
        String queryString = "DESCRIBE <http://semanticbible.org/ns/2006/NTNames#Shelah>";
        GraphQuery graphQuery = conn.prepareGraphQuery(QueryLanguage.SPARQL, queryString);
        GraphQueryResult results = graphQuery.evaluate();
        Statement st1 = results.next();
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#Shelah", st1.getSubject().stringValue());
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#childOf", st1.getPredicate().stringValue());
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#CainanSonOfArphaxad", st1.getObject().stringValue());
    }

    @Test
    public void testBooleanQuery()
            throws Exception {
        String queryString = "ASK { GRAPH <http://marklogic.com/test/my-graph> {<http://semanticbible.org/ns/2006/NTNames#Shelah1> ?p ?o}}";
        BooleanQuery booleanQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, queryString);
        boolean results = booleanQuery.evaluate();
        Assert.assertEquals(false, results);
        queryString = "ASK { GRAPH <http://marklogic.com/test/my-graph> { <http://semanticbible.org/ns/2006/NTNames#Shelah> ?p ?o}}";
        booleanQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, queryString);
        results = booleanQuery.evaluate();
        Assert.assertEquals(true, results);
    }

    @Test
    public void testBooleanQueryWithOverloadedMethods()
            throws Exception {
        String queryString = "ASK { GRAPH <http://marklogic.com/test/my-graph> {<http://semanticbible.org/ns/2006/NTNames#Shelah1> ?p ?o}}";
        BooleanQuery booleanQuery = conn.prepareBooleanQuery(queryString);
        booleanQuery = conn.prepareBooleanQuery(queryString,"http://marklogic.com/test/baseuri");
        boolean results = booleanQuery.evaluate();
        Assert.assertEquals(false, results);
        queryString = "ASK { GRAPH <http://marklogic.com/test/my-graph> { <http://semanticbible.org/ns/2006/NTNames#Shelah> ?p ?o}}";
        booleanQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, queryString);
        results = booleanQuery.evaluate();
        Assert.assertEquals(true, results);
    }
    @Test
    public void testUpdateQuery()
            throws Exception {
        String defGraphQuery = "INSERT DATA { GRAPH <http://marklogic.com/test/g27> { <http://marklogic.com/test> <pp1> <oo1> } }";
        String checkQuery = "ASK WHERE { <http://marklogic.com/test> <pp1> <oo1> }";
        Update updateQuery = conn.prepareUpdate(QueryLanguage.SPARQL, defGraphQuery);
        updateQuery.execute();
        BooleanQuery booleanQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, checkQuery);
        boolean results = booleanQuery.evaluate();
        Assert.assertEquals(true, results);
    }

    @Test
    public void testUpdateQueryWithBaseURI()
            throws Exception {
        String defGraphQuery = "BASE <http://marklogic.com/test/baseuri> INSERT DATA { GRAPH <http://marklogic.com/test/context10> {  <http://marklogic.com/test/subject> <pp1> <oo1> } }";
        String checkQuery = "BASE <http://marklogic.com/test/baseuri> ASK WHERE { <http://marklogic.com/test/subject> <pp1> <oo1> }";
        Update updateQuery = conn.prepareUpdate(QueryLanguage.SPARQL, defGraphQuery,"http://marklogic.com/test/baseuri");
        updateQuery.execute();
        BooleanQuery booleanQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, checkQuery);
        boolean results = booleanQuery.evaluate();
        Assert.assertEquals(true, results);
        conn.clear(conn.getValueFactory().createURI("http://marklogic.com/test/context10"));
    }

    @Test
    public void testClearWithContext()
            throws Exception {
        String defGraphQuery = "INSERT DATA { GRAPH <http://marklogic.com/test/ns/cleartest> { <http://marklogic.com/cleartest> <pp1> <oo1> } }";
        String checkQuery = "ASK WHERE { <http://marklogic.com/cleartest> <pp1> <oo1> }";
        Update updateQuery = conn.prepareUpdate(QueryLanguage.SPARQL, defGraphQuery);
        updateQuery.execute();
        BooleanQuery booleanQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, checkQuery);
        boolean results = booleanQuery.evaluate();
        Assert.assertEquals(true, results);

        conn.clear(conn.getValueFactory().createURI("http://marklogic.com/test/ns/cleartest"));
        booleanQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, checkQuery);
        results = booleanQuery.evaluate();
        Assert.assertEquals(false, results);

    }

    @Test
    public void testAddTurtle() throws Exception {
        File inputFile = new File("src/test/resources/testdata/default-graph-1.ttl");
        String baseURI = "http://example.org/example1/";
        Resource context1 = conn.getValueFactory().createURI("http://marklogic.com/test/context1");
        Resource context2 = conn.getValueFactory().createURI("http://marklogic.com/test/context2");
        conn.add(inputFile, baseURI, RDFFormat.TURTLE, context1, context2);
        conn.clear(context1, context2);
    }

    // https://github.com/marklogic/marklogic-sesame/issues/65
    @Test
    public void testAddMalformedTurtle() throws Exception {
        File inputFile = new File("src/test/resources/testdata/malformed-literals.ttl");
        String baseURI = "http://example.org/example1/";
        Resource context1 = conn.getValueFactory().createURI("http://marklogic.com/test/context1");
        exception.expect(MarkLogicSesameException.class);
        conn.add(inputFile, baseURI, RDFFormat.TURTLE, context1);
        conn.clear(context1);
    }

    @Test
    @Ignore
    public void testAddGZippedRDF() throws Exception {
        File inputFile = new File("src/test/resources/testdata/databases.rdf.gz");

        FileInputStream fis = new FileInputStream(inputFile);

        String baseURI = "http://example.org/example1/";
        Resource context1 = conn.getValueFactory().createURI("http://marklogic.com/test/context1");
        Resource context2 = conn.getValueFactory().createURI("http://marklogic.com/test/context2");
        conn.add(fis, baseURI, RDFFormat.RDFXML, context1, context2);
        conn.clear(context1, context2);
    }

    // https://github.com/marklogic/marklogic-sesame/issues/19
    @Test
    public void testAddTurtleWithDefaultContext() throws Exception {
        File inputFile = new File("src/test/resources/testdata/default-graph-2.ttl");

        conn.add(inputFile, null, RDFFormat.TURTLE);

        String checkQuery = "PREFIX dc:<http://purl.org/dc/elements/1.1/> PREFIX xsd:<http://www.w3.org/2001/XMLSchema#> ASK { <urn:x-local:graph1> dc:publisher ?o .}";
        BooleanQuery booleanQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, checkQuery);
        conn.setNamespace("dc", "http://purl.org/dc/elements/1.1/");
        Assert.assertTrue(booleanQuery.evaluate());
    }

    @Test
    public void testAddWithInputStream() throws Exception {
        File inputFile = new File("src/test/resources/testdata/default-graph-1.ttl");
        FileInputStream is = new FileInputStream(inputFile);
        String baseURI = "http://example.org/example1/";
        Resource context3 = conn.getValueFactory().createURI("http://marklogic.com/test/context3");
        Resource context4 = conn.getValueFactory().createURI("http://marklogic.com/test/context4");
        conn.add(is, baseURI, RDFFormat.TURTLE, context3, context4);
        conn.clear(context3, context4);
    }

    // this test requires access to https://raw.githubusercontent.com/marklogic/marklogic-sesame/develop/marklogic-sesame/src/test/resources/testdata/testData.trig
    @Test
    public void testAddTrigWithURL() throws Exception {
        URL dataURL = new URL("https://raw.githubusercontent.com/marklogic/marklogic-sesame/develop/marklogic-sesame/src/test/resources/testdata/testData.trig?token=AApzyAXWDMZiXGGf9DFnhq534MpEP-tKks5VwxFswA%3D%3D");

        Resource context1 = conn.getValueFactory().createURI("http://example.org/g1");
        Resource context2 = conn.getValueFactory().createURI("http://example.org/g2");
        Resource context3 = conn.getValueFactory().createURI("http://example.org/g3");
        Resource context4 = conn.getValueFactory().createURI("http://example.org/g4");
        Resource context5 = conn.getValueFactory().createURI("http://example.org/o1");
        Resource context6 = conn.getValueFactory().createURI("http://example.org/o2");
        Resource context7 = conn.getValueFactory().createURI("http://example.org/o3");
        Resource context8 = conn.getValueFactory().createURI("http://example.org/o4");
        conn.add(dataURL, dataURL.toString(), RDFFormat.TRIG, context3, context4);

        String checkQuery = "ASK { <http://example.org/r1> <http://example.org/p1> \"string value 0\" .}";
        BooleanQuery booleanQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, checkQuery);
        Assert.assertTrue(booleanQuery.evaluate());

        conn.clear(context1,context2,context3,context4,context5,context6,context7,context8);
    }

    @Test
    public void testAddNQuads() throws Exception{
        File inputFile = new File("src/test/resources/testdata/nquads1.nq");
        String baseURI = "http://example.org/example1/";

        Resource graph1 = conn.getValueFactory().createURI("http://example.org/graph1");
        Resource graph2 = conn.getValueFactory().createURI("http://example.org/graph2");
        Resource graph3 = conn.getValueFactory().createURI("http://example.org/graph3");
        Resource graph4 = conn.getValueFactory().createURI("http://example.org/graph4");

        conn.add(inputFile,baseURI,RDFFormat.NQUADS);

        String checkQuery = "ASK {GRAPH <http://example.org/graph4> { <http://example.org/kennedy/person1> <http://example.org/kennedy/death-year> '1969' . } }";
        BooleanQuery booleanQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, checkQuery);
        Assert.assertTrue(booleanQuery.evaluate());

        conn.clear(graph1,graph2,graph3,graph4);
    }

    @Test
    public void testAddNquadWithInputStream() throws Exception {
        File inputFile = new File("src/test/resources/testdata/nquads1.nq");
        FileInputStream is = new FileInputStream(inputFile);
        String baseURI = "http://example.org/example1/";
        Resource graph1 = conn.getValueFactory().createURI("http://example.org/graph1");
        Resource graph2 = conn.getValueFactory().createURI("http://example.org/graph2");
        Resource graph3 = conn.getValueFactory().createURI("http://example.org/graph3");
        Resource graph4 = conn.getValueFactory().createURI("http://example.org/graph4");
        conn.add(is, baseURI, RDFFormat.NQUADS);
        conn.clear(graph1, graph2, graph3, graph4);
    }

    @Test
    public void testAddRemoveStatementWithMultipleContext() throws Exception {

        Resource context5 = conn.getValueFactory().createURI("http://marklogic.com/test/context7");
        Resource context6 = conn.getValueFactory().createURI("http://marklogic.com/test/context8");
        ValueFactory f= conn.getValueFactory();

        URI alice = f.createURI("http://example.org/people/alice");
        URI bob = f.createURI("http://example.org/people/bob");
        URI name = f.createURI("http://example.org/ontology/name");
        URI person = f.createURI("http://example.org/ontology/Person");
        Literal bobsName = f.createLiteral("Bob");
        Literal alicesName = f.createLiteral("Alice");

        conn.add(alice, RDF.TYPE, person, context5);
        conn.add(alice, name, alicesName,context5, context6);
        conn.add(bob, RDF.TYPE, person, context5);
        conn.add(bob, name, bobsName, context5, context6);

        String checkAliceQuery = "ASK { <http://example.org/people/alice> <http://example.org/ontology/name> 'Alice' .}";
        BooleanQuery booleanAliceQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, checkAliceQuery);
        Assert.assertTrue(booleanAliceQuery.evaluate());

        conn.remove(alice, RDF.TYPE, person, context5);
        conn.remove(alice, name, alicesName, context5, context6);
        conn.remove(bob, RDF.TYPE, person, context5);
        conn.remove(bob, name, bobsName, context5, context6);

        Assert.assertFalse(booleanAliceQuery.evaluate());

        conn.clear(context5,context6);
    }

    @Test
    public void testContextIDs()
            throws Exception {
        RepositoryResult<Resource> result = conn.getContextIDs();
        try {
            Assert.assertTrue("result should not be empty", result.hasNext());
            logger.debug("ContextIDs");
            Resource result1 = result.next();
            logger.debug(result1.stringValue());
            Assert.assertEquals("http://marklogic.com/test/my-graph", result1.stringValue());
        } finally {
            result.close();
        }
    }

    @Test
    public void testTransaction1() throws Exception {
        File inputFile = new File("src/test/resources/testdata/named-graph-1.ttl");
        String baseURI = "http://example.org/example1/";
        Resource context1 = conn.getValueFactory().createURI("http://marklogic.com/test/transactiontest");
        conn.begin();
        conn.add(inputFile, baseURI, RDFFormat.TURTLE, context1);
        conn.rollback();
    }

    @Test
    public void testTransaction2() throws Exception {
        File inputFile = new File("src/test/resources/testdata/named-graph-1.ttl");

        String baseURI = "http://example.org/example1/";

        Resource context1 = conn.getValueFactory().createURI("http://marklogic.com/test/transactiontest");
        conn.begin();
        conn.add(inputFile, baseURI, RDFFormat.TURTLE, context1);
        conn.commit();
        conn.clear(context1);
    }

    @Test
    public void testTransaction3() throws Exception {
        Resource context1 = conn.getValueFactory().createURI("http://marklogic.com/test/my-graph");
        conn.begin();
        conn.clear(context1);
        conn.rollback();
    }

    @Test
    public void testOpen() throws Exception {
        Assert.assertEquals(true, conn.isOpen());
        conn.close();
        Assert.assertEquals(false, conn.isOpen());
    }

    @Test
    public void testActive() throws Exception {
        Assert.assertEquals(false, conn.isActive());
        conn.begin();
        Assert.assertEquals(true, conn.isActive());
    }

    @Ignore
    public void testSizeWithLargerGraph() throws Exception {
        Resource context1 = conn.getValueFactory().createURI("http://marklogic.com/test/my-graph");
        Assert.assertEquals(4036, conn.size(context1));
    }

    @Test
    public void testSizeWithEmptyGraph() throws Exception {
        Resource context1 = conn.getValueFactory().createURI("http://marklogic.com/test/nonexistent");
        Assert.assertEquals(0, conn.size(context1));
    }

    @Test
    public void testSizeWithSmallerGraph() throws Exception {
        Resource context1 = conn.getValueFactory().createURI("http://marklogic.com/test/context1");
        ValueFactory f= conn.getValueFactory();
        URI alice = f.createURI("http://example.org/people/alice");
        URI name = f.createURI("http://example.org/ontology/name");
        URI person = f.createURI("http://example.org/ontology/Person");
        Literal alicesName = f.createLiteral("Alice");
        conn.add(alice, RDF.TYPE, person, context1);
        conn.add(alice, name, alicesName,context1);
        Assert.assertEquals(2, conn.size(context1));
        conn.clear(context1);
    }

    @Test
    public void testModel() throws Exception{
        Resource context5 = conn.getValueFactory().createURI("http://marklogic.com/test/context5");
        Resource context6 = conn.getValueFactory().createURI("http://marklogic.com/test/context6");

        ValueFactory f= conn.getValueFactory();

        URI alice = f.createURI("http://example.org/people/alice");
        URI bob = f.createURI("http://example.org/people/bob");
        URI name = f.createURI("http://example.org/ontology/name");
        URI person = f.createURI("http://example.org/ontology/Person");
        Literal bobsName = f.createLiteral("Bob");
        Literal alicesName = f.createLiteral("Alice");

        conn.add(alice, RDF.TYPE, person, context5);
        conn.add(alice, name, alicesName,context5, context6);
        conn.add(bob, RDF.TYPE, person, context5);
        conn.add(bob, name, bobsName, context5, context6);

        //TBD- need to be able to set baseURI
       // RepositoryResult<Statement> statements = conn.getStatements(alice, null, null, true,context5);

        //Model aboutAlice = Iterations.addAll(statements, new LinkedHashModel());

        String checkAliceQuery = "ASK { GRAPH <http://marklogic.com/test/context5> {<http://example.org/people/alice> <http://example.org/ontology/name> 'Alice' .}}";
        BooleanQuery booleanAliceQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, checkAliceQuery);
        Assert.assertTrue(booleanAliceQuery.evaluate());

        conn.remove(alice, RDF.TYPE, person, context5);
        conn.remove(alice, name, alicesName, context5, context6);
        conn.remove(bob, RDF.TYPE, person, context5);
        conn.remove(bob, name, bobsName, context5, context6);

        Assert.assertFalse(booleanAliceQuery.evaluate());

        conn.clear(context5,context6);
    }

    @Test
    public void testAddStatements() throws Exception{
        Resource context = conn.getValueFactory().createURI("http://marklogic.com/test/context");

        ValueFactory f= conn.getValueFactory();

        URI alice = f.createURI("http://example.org/people/alice");
        URI bob = f.createURI("http://example.org/people/bob");
        URI name = f.createURI("http://example.org/ontology/name");
        URI age = f.createURI("http://example.org/ontology/age");
        URI person = f.createURI("http://example.org/ontology/Person");
        Literal bobsAge = f.createLiteral(123123123123D);
        Literal alicesName = f.createLiteral("Alice");

        conn.add(alice, name, alicesName,context);
        conn.add(bob, age, bobsAge, context);

        String checkAliceQuery = "ASK { <http://example.org/people/alice> <http://example.org/ontology/name> 'Alice' .}";
        BooleanQuery booleanAliceQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, checkAliceQuery);
        Assert.assertTrue(booleanAliceQuery.evaluate());

        String checkBobQuery = "ASK { <http://example.org/people/bob> <http://example.org/ontology/age> '123123123123'^^<http://www.w3.org/2001/XMLSchema#double> .}";
        BooleanQuery booleanBobQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, checkBobQuery);
        Assert.assertTrue(booleanBobQuery.evaluate());

        conn.clear(context);
    }

    @Test
    public void testModelWithIterator() throws Exception{
        Resource context1 = conn.getValueFactory().createURI("http://marklogic.com/test/context1");
        Resource context2 = conn.getValueFactory().createURI("http://marklogic.com/test/context2");

        ValueFactory f= conn.getValueFactory();

        URI alice = f.createURI("http://example.org/people/alice");
        URI name = f.createURI("http://example.org/ontology/name");
        URI person = f.createURI("http://example.org/ontology/Person");
        Literal alicesName = f.createLiteral("Alice1");

        conn.add(alice, RDF.TYPE, person, context1);
        conn.add(alice, name, alicesName, context1);

        String checkAliceQuery = "ASK { GRAPH <http://marklogic.com/test/context1> {<http://example.org/people/alice> <http://example.org/ontology/name> 'Alice1' .}}";
        BooleanQuery booleanAliceQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, checkAliceQuery);
        Assert.assertTrue(booleanAliceQuery.evaluate());

        RepositoryResult<Statement> statements = conn.getStatements(alice, null, null, true,context1);

        conn.add(statements,context2);
        conn.clear(context1);

        checkAliceQuery = "ASK { GRAPH <http://marklogic.com/test/context2> {<http://example.org/people/alice> <http://example.org/ontology/name> 'Alice1' .}}";
        booleanAliceQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, checkAliceQuery);
        Assert.assertTrue(booleanAliceQuery.evaluate());

        conn.clear(context2);
    }

    @Test
    public void testStatementWithDefinedContext1() throws Exception{
        Resource context1 = conn.getValueFactory().createURI("http://marklogic.com/test/context1");

        ValueFactory f= conn.getValueFactory();

        URI alice = f.createURI("http://example.org/people/alice");
        URI name = f.createURI("http://example.org/ontology/name");
        URI person = f.createURI("http://example.org/ontology/Person");
        Literal alicesName = f.createLiteral("Alice1");

        Statement st1 = f.createStatement(alice, name, alicesName, context1);
        conn.add(st1);

        String checkAliceQuery = "ASK { GRAPH <http://marklogic.com/test/context1> {<http://example.org/people/alice> <http://example.org/ontology/name> 'Alice1' .}}";
        BooleanQuery booleanAliceQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, checkAliceQuery);
        Assert.assertTrue(booleanAliceQuery.evaluate());

        conn.clear(context1);
    }

    @Test
    public void testStatementWithDefinedContext2() throws Exception{
        Resource context1 = conn.getValueFactory().createURI("http://marklogic.com/test/context1");

        ValueFactory f= conn.getValueFactory();

        URI alice = f.createURI("http://example.org/people/alice");
        URI name = f.createURI("http://example.org/ontology/name");
        URI person = f.createURI("http://example.org/ontology/Person");
        Literal alicesName = f.createLiteral("Alice1");

        Statement st1 = f.createStatement(alice, name, alicesName);
        conn.add(st1,context1);

        String checkAliceQuery = "ASK { GRAPH <http://marklogic.com/test/context1> {<http://example.org/people/alice> <http://example.org/ontology/name> 'Alice1' .}}";
        BooleanQuery booleanAliceQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, checkAliceQuery);
        Assert.assertTrue(booleanAliceQuery.evaluate());

        conn.clear(context1);
    }

    @Test
    public void testGetStatements() throws Exception{
        Resource context1 = conn.getValueFactory().createURI("http://marklogic.com/test/my-graph");

        ValueFactory f= conn.getValueFactory();
        URI subj = f.createURI("http://semanticbible.org/ns/2006/NTNames#AlexandriaGeodata");
        RepositoryResult<Statement> statements = conn.getStatements(subj, null, null, true, context1);

        Assert.assertTrue(statements.hasNext());
    }

    @Test
    public void testGetStatementsEmpty() throws Exception{
        Resource context1 = conn.getValueFactory().createURI("http://marklogic.com/test/my-graph");

        ValueFactory f= conn.getValueFactory();
        URI subj = f.createURI("http://semanticbible.org/ns/2006/NTNames#AlexandriaGeodata1");
        RepositoryResult<Statement> statements = conn.getStatements(subj, null, null, true, context1);

        Assert.assertFalse(statements.hasNext());
    }

    @Test
    public void testPrepareGraphQueryWithSingleResult() throws Exception
    {
        Resource context1 = conn.getValueFactory().createURI("http://marklogic.com/test/context1");

        ValueFactory f= conn.getValueFactory();

        URI alice = f.createURI("http://example.org/people/alice");
        URI name = f.createURI("http://example.org/ontology/name");
        URI person = f.createURI("http://example.org/ontology/Person");
        Literal alicesName = f.createLiteral("Alice1");

        Statement st1 = f.createStatement(alice, name, alicesName);
        conn.add(st1,context1);

        String query = " DESCRIBE <http://example.org/people/alice> ";
        GraphQuery queryObj = conn.prepareGraphQuery(query);
        GraphQueryResult result = queryObj.evaluate();

        Assert.assertTrue(result != null);
        Assert.assertTrue(result.hasNext());
        Statement st = result.next();
        Assert.assertFalse(result.hasNext());
        conn.clear(context1);
    }

    @Test
    public void testPrepareGraphQueryWithNoResult() throws Exception
    {

        String query = "DESCRIBE <http://example.org/nonexistant>";
        GraphQuery queryObj = conn.prepareGraphQuery(query);
        GraphQueryResult result = queryObj.evaluate();

        Assert.assertTrue(result != null);
        Assert.assertFalse(result.hasNext());
    }

    @Test
    public void testHasStatement() throws Exception
    {
        Resource context1 = conn.getValueFactory().createURI("http://marklogic.com/test/context1");
        ValueFactory f= conn.getValueFactory();
        URI alice = f.createURI("http://example.org/people/alice");
        URI name = f.createURI("http://example.org/ontology/name");
        Literal alicesName = f.createLiteral("Alice");

        Statement st1 = f.createStatement(alice, name, alicesName);
        conn.add(st1,context1);

        Assert.assertTrue(conn.hasStatement(st1, false, context1));

        conn.clear(context1);
    }

    @Test
    public void testSPARQLQueryWithEmptyResults()
            throws Exception {
        String queryString = "select * { <http://marklogic.com/nonexistent> ?p ?o } limit 100 ";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
        TupleQueryResult results = tupleQuery.evaluate();
        Assert.assertFalse(results.hasNext());
    }

    @Test
    public void testExportStatements()
            throws Exception {
        Resource context1 = conn.getValueFactory().createURI("http://marklogic.com/test/context1");
        ValueFactory f= conn.getValueFactory();
        final URI alice = f.createURI("http://example.org/people/alice");
        URI name = f.createURI("http://example.org/ontology/name");
        Literal alicesName = f.createLiteral("Alice");

        Statement st1 = f.createStatement(alice, name, alicesName);
        conn.add(st1, context1);

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        RDFXMLWriter rdfWriter = new RDFXMLWriter(out);

        String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<rdf:RDF\n" +
                "\txmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">\n" +
                "\n" +
                "<rdf:Description rdf:about=\"http://example.org/people/alice\">\n" +
                "\t<name xmlns=\"http://example.org/ontology/\" rdf:datatype=\"http://www.w3.org/2001/XMLSchema#string\">Alice</name>\n" +
                "</rdf:Description>\n" +
                "\n" +
                "</rdf:RDF>";

        conn.exportStatements(alice, null, alicesName, true, rdfWriter, context1);
        Assert.assertEquals(expected,out.toString());
        conn.clear(context1);
    }

    @Test
    public void testIntegrateWithRemoteRepository() throws Exception{
        final Resource context1 = conn.getValueFactory().createURI("http://marklogic.com/test/context1");

        String endpointURL = "http://lod.openlinksw.com/sparql/";
        Repository remoteSPARQL = new SPARQLRepository(endpointURL);

        remoteSPARQL.initialize();

        RepositoryConnection remoteconn =
                remoteSPARQL.getConnection();
        try {
            String sparqlQuery =
                    "SELECT ?s,?p,?o WHERE {\n" +
                            "  <http://www.w3.org/People/Berners-Lee/card#i> ?p ?o .\n" +
                            "}";
            TupleQuery tupleQuery = remoteconn.prepareTupleQuery(QueryLanguage.SPARQL, sparqlQuery);
            tupleQuery.evaluate(new TupleQueryResultHandler() {
                @Override
                public void startQueryResult(List<String> bindingNames) {
                }

                @Override
                public void handleSolution(BindingSet bindingSet) {
                    Resource subject = f.createURI("http://www.w3.org/People/Berners-Lee/card#i");
                    Statement st = conn.getValueFactory().createStatement(subject,(URI) bindingSet.getValue("p"), bindingSet.getValue("o"));
                    try {
                        conn.add(st, context1);
                    } catch (RepositoryException e) {
                        e.printStackTrace();
                    }
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

            String checkAliceQuery = "ASK { GRAPH <http://marklogic.com/test/context1> {<http://www.w3.org/People/Berners-Lee/card#i> <http://data.semanticweb.org/ns/swc/ontology#holdsRole> <http://events.linkeddata.org/ldow2009/#chairrole> .}}";
            BooleanQuery booleanAliceQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, checkAliceQuery);
            Assert.assertTrue(booleanAliceQuery.evaluate());

            conn.clear(context1);

        }
        finally {
            remoteconn.close();
        }

    }

    // https://github.com/marklogic/marklogic-sesame/issues/66
    @Test
    public void testRemoveStatementIteration()
            throws Exception
    {
        ValueFactory f= conn.getValueFactory();
        Resource context1 = f.createURI("http://marklogic.com/test/context1");
        final URI alice = f.createURI("http://example.org/people/alice");
        URI name = f.createURI("http://example.org/ontology/name");
        Literal alicesName = f.createLiteral("Alice");

        Statement st1 = f.createStatement(alice, name, alicesName);
        conn.begin();
        conn.add(st1, context1);
        conn.commit();

        Assert.assertEquals(1L, conn.size(context1));

        Iteration<? extends Statement, RepositoryException> iter = conn.getStatements(alice, name,
                null, false);

        conn.remove(iter);
        Assert.assertEquals(0L,conn.size(context1));
    }

    // https://github.com/marklogic/marklogic-sesame/issues/68
    @Test
    public void getStatementWithNullContext()
            throws Exception
    {
        ValueFactory f= conn.getValueFactory();
        Resource context1 = f.createURI("http://marklogic.com/test/context1");
        final URI alice = f.createURI("http://example.org/people/alice");
        URI name = f.createURI("http://example.org/ontology/name");
        Literal alicesName = f.createLiteral("Alice");

        Statement st1 = f.createStatement(alice, name, alicesName);
        conn.begin();
        conn.add(st1, context1);
        conn.commit();

        Assert.assertEquals(1L, conn.size(context1));

        Iteration<? extends Statement, RepositoryException> iter = conn.getStatements(alice, name,
                null, false,null,context1);

        Assert.assertEquals(1L, conn.size(context1));

        conn.clear(context1);
    }

    // https://github.com/marklogic/marklogic-sesame/issues/61
    @Test
    public void removeWithNullObject()
            throws Exception
    {
        ValueFactory f= conn.getValueFactory();
        Resource context1 = f.createURI("http://marklogic.com/test/context1");
        final URI alice = f.createURI("http://example.org/people/alice");
        URI name = f.createURI("http://example.org/ontology/name");
        URI age = f.createURI("http://example.org/ontology/age");
        Literal alicesName = f.createLiteral("Alice");
        Literal alicesAge = f.createLiteral(22);

        Statement st1 = f.createStatement(alice, name, alicesName);
        Statement st2 = f.createStatement(alice, age, alicesAge);
        conn.begin();
        conn.add(st1, context1);
        conn.add(st2, context1);
        conn.commit();

        Assert.assertEquals(2L, conn.size(context1));
        conn.remove(alice, age, null, context1);
        Assert.assertEquals(1L, conn.size(context1));
        conn.remove(alice, null, alicesName, context1);
    }

    // https://github.com/marklogic/marklogic-sesame/issues/63
    @Test
    public void addWithNull()
            throws Exception
    {
        ValueFactory f= conn.getValueFactory();
        final URI alice = f.createURI("http://example.org/people/alice");
        URI name = f.createURI("http://example.org/ontology/name");

        exception.expect(AssertionError.class);
        Statement st = f.createStatement(alice, name, null);
    }

    // https://github.com/marklogic/marklogic-sesame/issues/70
    @Test
    public void testAddWithNullContext() throws Exception {
        ValueFactory f= conn.getValueFactory();
        Resource context1 = f.createURI("http://marklogic.com/test/context1");
        final URI william = f.createURI("http://example.org/people/william");
        URI name = f.createURI("http://example.org/ontology/name");
        URI age = f.createURI("http://example.org/ontology/age");
        Literal williamName = f.createLiteral("William");
        Literal williamAge = f.createLiteral(22);

        Statement st1 = f.createStatement(william, name, williamName);
        Statement st2 = f.createStatement(william, age, williamAge);

        conn.add(st1, null);
        conn.add(st2, null);

        conn.remove(william, age, williamName);
        conn.remove(william, name, williamAge);
    }
}
