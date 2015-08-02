package com.marklogic.semantics.sesame;


import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.*;
import com.marklogic.client.semantics.Capability;
import com.marklogic.client.semantics.GraphManager;
import com.marklogic.semantics.sesame.query.MarkLogicBooleanQuery;
import com.marklogic.semantics.sesame.query.MarkLogicUpdateQuery;
import org.junit.*;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MarkLogicCombinationQueryTest extends SesameTestBase {

    private QueryManager qmgr;

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected RepositoryConnection conn;
    protected ValueFactory f;


    @Before
    public void setUp() throws RepositoryException {
        logger.debug("setting up test");
        rep.initialize();
        f = rep.getValueFactory();
        conn = rep.getConnection();
        logger.info("test setup complete.");

        String tripleDocOne =

                "<semantic-document>\n" +
                        "<title>First Title</title>\n" +
                        "<size>100</size>\n" +
                        "<sem:triples xmlns:sem=\"http://marklogic.com/semantics\">" +
                        "<sem:triple><sem:subject>http://example.org/r9928</sem:subject>" +
                        "<sem:predicate>http://example.org/p3</sem:predicate>" +
                        "<sem:object datatype=\"http://www.w3.org/2001/XMLSchema#int\">1</sem:object></sem:triple>" +
                        "</sem:triples>\n" +
                        "</semantic-document>";

        String tripleDocTwo =

                "<semantic-document>\n" +
                        "<title>Second Title</title>\n" +
                        "<size>500</size>\n" +
                        "<sem:triples xmlns:sem=\"http://marklogic.com/semantics\">" +
                        "<sem:triple><sem:subject>http://example.org/r9929</sem:subject>" +
                        "<sem:predicate>http://example.org/p3</sem:predicate>" +
                        "<sem:object datatype=\"http://www.w3.org/2001/XMLSchema#int\">2</sem:object></sem:triple>" +
                        "</sem:triples>\n" +
                        "</semantic-document>";

        XMLDocumentManager docMgr = writerClient.newXMLDocumentManager();
        docMgr.write("/directory1/doc1.xml", new StringHandle().with(tripleDocOne));
        docMgr.write("/directory2/doc2.xml", new StringHandle().with(tripleDocTwo));
        qmgr = writerClient.newQueryManager();

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

        XMLDocumentManager docMgr = writerClient.newXMLDocumentManager();

        docMgr.delete("/directory1/doc1.xml");
        docMgr.delete("/directory2/doc2.xml");

    }

    @Test
    public void testCombinationQuery() throws MalformedQueryException, RepositoryException, QueryEvaluationException {

        String query1 = "ASK WHERE {<http://example.org/r9928> ?p ?o .}";
        String query2 = "ASK WHERE {<http://example.org/r9929> ?p ?o .}";

        // case one, rawcombined
        String combinedQuery =
                "{\"search\":" +
                        "{\"qtext\":\"First Title\"}}";
        String negCombinedQuery =
                "{\"search\":" +
                        "{\"qtext\":\"Second Title\"}}";

        RawCombinedQueryDefinition rawCombined = qmgr.newRawCombinedQueryDefinition(new StringHandle().with(combinedQuery).withFormat(Format.JSON));
        RawCombinedQueryDefinition negRawCombined = qmgr.newRawCombinedQueryDefinition(new StringHandle().with(negCombinedQuery).withFormat(Format.JSON));

        MarkLogicBooleanQuery askQuery = (MarkLogicBooleanQuery) conn.prepareBooleanQuery(QueryLanguage.SPARQL,query1);
        askQuery.setConstrainingQueryDefinition(rawCombined);
        Assert.assertEquals(true, askQuery.evaluate());

//        askQuery = (MarkLogicBooleanQuery) conn.prepareBooleanQuery(QueryLanguage.SPARQL,query2);
//        askQuery.setConstrainingQueryDefinition(rawCombined);
//        Assert.assertEquals(false, askQuery.evaluate());

    }

    @Test
    public void testUpdateQueryWithPerms()
            throws Exception {

        GraphManager gmgr = adminClient.newGraphManager();
        String defGraphQuery = "INSERT DATA { GRAPH <http://marklogic.com/test/g27> { <http://marklogic.com/test> <pp1> <oo1> } }";
        String checkQuery = "ASK WHERE { <http://marklogic.com/test> <pp1> <oo1> }";
        MarkLogicUpdateQuery updateQuery = (MarkLogicUpdateQuery) conn.prepareUpdate(QueryLanguage.SPARQL, defGraphQuery);
        updateQuery.setGraphPerms(gmgr.permission("read-privileged", Capability.READ));

        updateQuery.execute();
        BooleanQuery booleanQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, checkQuery);
        boolean results = booleanQuery.evaluate();
        Assert.assertEquals(true, results);
    }

}