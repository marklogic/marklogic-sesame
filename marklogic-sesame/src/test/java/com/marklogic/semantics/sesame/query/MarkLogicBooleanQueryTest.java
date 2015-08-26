package com.marklogic.semantics.sesame.query;

import java.io.File;
import java.io.FileNotFoundException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.marklogic.client.io.FileHandle;
import com.marklogic.client.semantics.GraphManager;
import com.marklogic.client.semantics.RDFMimeTypes;
import com.marklogic.semantics.sesame.MarkLogicRepositoryConnection;
import com.marklogic.semantics.sesame.SesameTestBase;

/**
 * Created by jfuller on 8/11/15.
 */
public class MarkLogicBooleanQueryTest extends SesameTestBase {

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
    public void testBooleanQuery()
            throws Exception {
        String queryString = "ASK {GRAPH <http://example.org/test/g27> {<http://semanticbible.org/ns/2006/NTNames#Shelah1> ?p ?o}}";
        BooleanQuery booleanQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, queryString);
        boolean results = booleanQuery.evaluate();
        Assert.assertEquals(false, results);
        queryString = "ASK {GRAPH <http://example.org/test/g27> {<http://semanticbible.org/ns/2006/NTNames#Shelah> ?p ?o}}";
        booleanQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, queryString);
        results = booleanQuery.evaluate();
        Assert.assertEquals(true, results);
    }

    @Test
    public void testBooleanQueryWithOverloadedMethods()
            throws Exception {
        String queryString = "ASK { <http://semanticbible.org/ns/2006/NTNames#Shelah1> ?p ?o}";
        BooleanQuery booleanQuery = conn.prepareBooleanQuery(queryString);
        booleanQuery = conn.prepareBooleanQuery(queryString,"http://marklogic.com/test/baseuri");
        boolean results = booleanQuery.evaluate();
        Assert.assertEquals(false, results);
        queryString = "ASK { <http://semanticbible.org/ns/2006/NTNames#Shelah> ?p ?o}";
        booleanQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, queryString);
        results = booleanQuery.evaluate();
        Assert.assertEquals(true, results);
    }

}
