package com.marklogic.semantics.sesame.query;

import com.marklogic.client.io.FileHandle;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.semantics.GraphManager;
import com.marklogic.client.semantics.RDFMimeTypes;
import com.marklogic.semantics.sesame.MarkLogicRepositoryConnection;
import com.marklogic.semantics.sesame.SesameTestBase;
import org.junit.*;
import org.openrdf.model.*;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;

/**
 * Created by jfuller on 8/11/15.
 */
public class MarkLogicGraphQueryTest extends SesameTestBase {

    private QueryManager qmgr;

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected MarkLogicRepositoryConnection conn;
    protected ValueFactory f;

    @Before
    public void setUp() throws RepositoryException, FileNotFoundException {
        logger.debug("setting up test");
        rep.initialize();
        f = rep.getValueFactory();
        conn = rep.getConnection();
        logger.info("test setup complete.");
        File testData = new File("src/test/resources/testdata/test.owl");

        GraphManager gmgr = writerClient.newGraphManager();
        gmgr.setDefaultMimetype(RDFMimeTypes.RDFXML);
        gmgr.write("http://example.org/test/g27", new FileHandle(testData));
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
        GraphManager gmgr = writerClient.newGraphManager();
        gmgr.delete("http://example.org/test/g27");
    }


    @Test
    public void testConstructQuery()
            throws Exception {
        String queryString = "PREFIX nn: <http://semanticbible.org/ns/2006/NTNames#>\n" +
                "PREFIX test: <http://marklogic.com#test>\n" +
                "\n" +
                "construct { ?s  test:test \"0\"} WHERE  {?s nn:childOf nn:Eve . }";
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
                "construct { ?s  test:test <relative>} WHERE {?s nn:childOf nn:Eve . }";
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
                        "construct { ?s  test:test <relative>} WHERE {?s nn:childOf nn:Eve . }";
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
                "construct { ?s  test:test \"0\"} WHERE {?s nn:childOf nn:Eve . }";
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

    @Ignore
    public void testCreateGraph() throws Exception
    {
        conn.prepareUpdate("CREATE GRAPH <http://example1.org>").execute();
    }

}
