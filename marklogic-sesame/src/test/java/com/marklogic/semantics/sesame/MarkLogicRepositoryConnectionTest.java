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

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.Iteration;
import info.aduna.iteration.Iterations;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.openrdf.model.*;
import org.openrdf.model.impl.LinkedHashModel;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.*;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.repository.sparql.SPARQLRepository;
import org.openrdf.rio.RDFFormat;
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
        conn.clear();
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
        String user = props.getProperty("validUsername");
        String pass = props.getProperty("validPassword");

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
    public void testClearAll()
            throws Exception {
        Resource context1 = conn.getValueFactory().createURI("http://marklogic.com/test/context1");
        File inputFile1 = new File("src/test/resources/testdata/default-graph-1.ttl");
        conn.add(inputFile1, "http://example.org/example1/", RDFFormat.TURTLE, null);
        File inputFile2 = new File("src/test/resources/testdata/default-graph-2.ttl");
        conn.add(inputFile2, "http://example.org/example1/", RDFFormat.TURTLE, context1);
        String defGraphQuery = "INSERT DATA { GRAPH <http://marklogic.com/test/ns/cleartest> { <http://marklogic.com/cleartest> <pp1> <oo1> } }";
        Update updateQuery = conn.prepareUpdate(QueryLanguage.SPARQL, defGraphQuery);
        updateQuery.execute();
        conn.clear();
        Assert.assertEquals(0, conn.size());
    }

    @Test
    public void testClearSome()
            throws Exception {
        Resource context1 = conn.getValueFactory().createURI("http://marklogic.com/test/context1");
        Resource context2 = conn.getValueFactory().createURI("http://marklogic.com/test/context2");
        File inputFile1 = new File("src/test/resources/testdata/default-graph-1.ttl");
        conn.add(inputFile1, "http://example.org/example1/", RDFFormat.TURTLE, null);
        File inputFile2 = new File("src/test/resources/testdata/default-graph-2.ttl");
        conn.add(inputFile2, "http://example.org/example1/", RDFFormat.TURTLE, context1);
        File inputFile3 = new File("src/test/resources/testdata/default-graph-2.ttl");
        conn.add(inputFile3, "http://example.org/example1/", RDFFormat.TURTLE, context2);
        String defGraphQuery = "INSERT DATA { GRAPH <http://marklogic.com/test/ns/cleartest> { <http://marklogic.com/cleartest> <http://marklogic.com/test/pp1> \"oo1\" } }";
        Update updateQuery = conn.prepareUpdate(QueryLanguage.SPARQL, defGraphQuery);
        updateQuery.execute();
        conn.clear(null, context1);
        Assert.assertEquals(5, conn.size());
        conn.clear();
    }

    @Test
    public void testGetStatement1()
            throws Exception {
        Resource context1 = conn.getValueFactory().createURI("http://marklogic.com/test/context1");
        Resource context2 = conn.getValueFactory().createURI("http://marklogic.com/test/context2");
        File inputFile1 = new File("src/test/resources/testdata/default-graph-1.ttl");
        conn.add(inputFile1, "http://example.org/example1/", RDFFormat.TURTLE, null);
        File inputFile2 = new File("src/test/resources/testdata/default-graph-2.ttl");
        conn.add(inputFile2, "http://example.org/example1/", RDFFormat.TURTLE, context1);
        File inputFile3 = new File("src/test/resources/testdata/default-graph-2.ttl");
        conn.add(inputFile3, "http://example.org/example1/", RDFFormat.TURTLE, context2);

        conn.clear(null,context1);
        RepositoryResult<Statement> statements = conn.getStatements(null, null, null, true);
        Model model = Iterations.addAll(statements, new LinkedHashModel());

        Assert.assertEquals(4, model.size());
        conn.clear();
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

    // https://github.com/marklogic/marklogic-sesame/issues/70
    @Test
    public void testAddTurtleWithNullContext() throws Exception {
        File inputFile = new File("src/test/resources/testdata/default-graph-1.ttl");
        conn.add(inputFile, "http://example.org/example1/", RDFFormat.TURTLE, null);
        Assert.assertEquals(4, conn.size(null));
        Assert.assertEquals(4, conn.size());
        conn.clear(null);
    }

    // https://github.com/marklogic/marklogic-sesame/issues/64
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
        conn.clear(conn.getValueFactory().createURI("http://marklogic.com/semantics#default-graph"));
    }

    @Test
    public void testAddWithInputStream() throws Exception {
        File inputFile = new File("src/test/resources/testdata/default-graph-1.ttl");
        FileInputStream is = new FileInputStream(inputFile);
        String baseURI = "http://example.org/example1/";
        Resource context3 = conn.getValueFactory().createURI("http://marklogic.com/test/context3");
        Resource context4 = conn.getValueFactory().createURI("http://marklogic.com/test/context4");
        conn.add(is, baseURI, RDFFormat.TURTLE, context3); // TBD - add multiple context
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
    @Ignore
    public void testContextIDs()
            throws Exception {
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

        RepositoryResult<Resource> result = conn.getContextIDs();
        try {
            Assert.assertTrue("result should not be empty", result.hasNext());
            logger.debug("ContextIDs");
            Resource result1 = result.next();
            logger.debug(result1.stringValue());
            Assert.assertEquals("http://marklogic.com/test/context7", result1.stringValue());
        } finally {
            result.close();
        }

        conn.clear(context5,context6);

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
        File inputFile = new File("src/test/resources/testdata/test.owl");
        conn.add(inputFile,null,RDFFormat.RDFXML);
        ValueFactory f= conn.getValueFactory();
        URI subj = f.createURI("http://semanticbible.org/ns/2006/NTNames#AlexandriaGeodata");
        RepositoryResult<Statement> statements = conn.getStatements(subj, null, null, true);
        Assert.assertTrue(statements.hasNext());
        conn.clear(conn.getValueFactory().createURI("http://marklogic.com/semantics#default-graph"));
    }

    @Test
    public void testGetStatementsEmpty() throws Exception{
        File inputFile = new File("src/test/resources/testdata/test.owl");
        conn.add(inputFile,null,RDFFormat.RDFXML);
        Resource context1 = conn.getValueFactory().createURI("http://marklogic.com/test/my-graph");
        conn.add(inputFile,null,RDFFormat.RDFXML,context1);

        ValueFactory f= conn.getValueFactory();
        URI subj = f.createURI("http://semanticbible.org/ns/2006/NTNames#AlexandriaGeodata1");
        RepositoryResult<Statement> statements = conn.getStatements(subj, null, null, true, context1);

        Assert.assertFalse(statements.hasNext());
        conn.clear(context1);
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
        Assert.assertEquals(expected, out.toString());
        conn.clear(context1);
    }

    @Ignore
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
        Assert.assertEquals(0L, conn.size(context1));
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
                null, false, null, context1);

        Assert.assertEquals(1L, conn.size(context1));

        conn.clear(context1);
    }

    // https://github.com/marklogic/marklogic-sesame/issues/61
    @Test
    public void testRemoveWithNullObject()
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

        conn.remove(william, age, williamName, null);
        conn.remove(william, name, williamAge, null);
    }

    // https://github.com/marklogic/marklogic-sesame/issues/83
    @Test
    public void testSizeWithNull() throws Exception {
        File inputFile = new File("src/test/resources/testdata/test.owl");
        conn.add(inputFile,null,RDFFormat.RDFXML);
        Assert.assertEquals(4036L, conn.size(null));
        conn.clear(conn.getValueFactory().createURI("http://marklogic.com/semantics#default-graph"));
    }

    // https://github.com/marklogic/marklogic-sesame/issues/82
    @Test
    public void testGetStatementWithMultipleContexts() throws Exception{
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

        RepositoryResult<Statement> statements = conn.getStatements(null, null, null, true, context6);

        Model aboutEveryone = Iterations.addAll(statements, new LinkedHashModel());

        Assert.assertEquals(6L, aboutEveryone.size());

        statements = conn.getStatements(null, null, null, true, context5,context6);
        List aboutList = Iterations.asList(statements);

        Assert.assertEquals(6L, aboutList.size()); // TBD- why does it dedupe ?

        conn.clear(context5,context6);
    }

    // https://github.com/marklogic/marklogic-sesame/issues/81
    @Test
    public void testGetStatementReturnCorrectContext() throws Exception{
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

        CloseableIteration<? extends Statement, RepositoryException> iter = conn.getStatements(null, null, null, false, context5);


        while(iter.hasNext()){
            Statement st = iter.next();
            Assert.assertTrue(st.getContext().equals(context5)  || st.getContext().equals(context6));
        }
        conn.clear(context5,context6);
    }


    // https://github.com/marklogic/marklogic-sesame/issues/90
    @Test
    public void testGetStatementIsEqualToSize() throws Exception{
        Resource context5 = conn.getValueFactory().createURI("http://marklogic.com/test/context5");

        ValueFactory f= conn.getValueFactory();

        URI alice = f.createURI("http://example.org/people/alice");
        URI bob = f.createURI("http://example.org/people/bob");
        URI name = f.createURI("http://example.org/ontology/name");
        URI person = f.createURI("http://example.org/ontology/Person");
        Literal bobsName = f.createLiteral("Bob");
        Literal alicesName = f.createLiteral("Alice");

        conn.add(alice, RDF.TYPE, person, null,context5);
        conn.add(alice, name, alicesName,null,context5);
        conn.add(bob, RDF.TYPE, person, context5);
        conn.add(bob, name, bobsName, context5);

        RepositoryResult<Statement> statements = conn.getStatements(null, null, null, true, null,context5);
        Model aboutPeople = Iterations.addAll(statements, new LinkedHashModel());

        Assert.assertEquals(conn.size(null,context5),aboutPeople.size());
        conn.clear(null,context5);
    }

    @Test
    public void testCompareSizeAWithNullContext() throws Exception {
        Resource context5 = conn.getValueFactory().createURI("http://marklogic.com/test/context5");

        File inputFile1 = new File("src/test/resources/testdata/default-graph-1.ttl");
        conn.add(inputFile1, "http://example.org/example1/", RDFFormat.TURTLE, null);

        File inputFile2 = new File("src/test/resources/testdata/default-graph-2.ttl");
        conn.add(inputFile2, "http://example.org/example1/", RDFFormat.TURTLE, context5);

        Assert.assertEquals(8, conn.size());
        Assert.assertEquals(8, conn.size(null));
        Assert.assertEquals(8, conn.size(context5));
        Assert.assertEquals(8, conn.size(null,context5));
        Assert.assertEquals(8, conn.size(context5,null,context5));
        conn.clear();
    }

}
