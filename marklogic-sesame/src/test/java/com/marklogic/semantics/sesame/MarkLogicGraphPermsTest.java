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
package com.marklogic.semantics.sesame;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.openrdf.model.Resource;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.semantics.Capability;
import com.marklogic.client.semantics.GraphManager;
import com.marklogic.semantics.sesame.query.MarkLogicUpdateQuery;

public class MarkLogicGraphPermsTest extends SesameTestBase {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected MarkLogicRepositoryConnection conn;
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
    public void testUpdateQueryWithPerms()
            throws Exception {

        GraphManager gmgr = adminClient.newGraphManager();
        Resource context = conn.getValueFactory().createURI("http://marklogic.com/test/graph/permstest");

        String defGraphQuery = "INSERT DATA { GRAPH <http://marklogic.com/test/graph/permstest> { <http://marklogic.com/test> <pp1> <oo1> } }";
        String checkQuery = "ASK WHERE {  GRAPH <http://marklogic.com/test/graph/permstest> {<http://marklogic.com/test> <pp1> <oo1> }}";
        MarkLogicUpdateQuery updateQuery = conn.prepareUpdate(QueryLanguage.SPARQL, defGraphQuery);
        updateQuery.setGraphPerms(gmgr.permission("admin", Capability.READ));
        updateQuery.execute();

        BooleanQuery booleanQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, checkQuery);
        boolean results = booleanQuery.evaluate();
        Assert.assertEquals(true, results);

        conn.clear(context);
    }

    // https://github.com/marklogic/marklogic-sesame/issues/122
    @Test
    public void testUpdateQueryWithPermsFromConnectionDefaults()
            throws Exception {

        GraphManager gmgr = adminClient.newGraphManager();
        conn.setGraphPerms(gmgr.permission("app-user", Capability.READ));

        Resource context = conn.getValueFactory().createURI("http://marklogic.com/test/graph/permstest");

        String defGraphQuery = "INSERT DATA { GRAPH <http://marklogic.com/test/graph/permstest> { <http://marklogic.com/test> <pp1> <oo1> } }";
        String checkQuery = "ASK WHERE {  GRAPH <http://marklogic.com/test/graph/permstest> {<http://marklogic.com/test> <pp1> <oo1> }}";
        MarkLogicUpdateQuery updateQuery = conn.prepareUpdate(QueryLanguage.SPARQL, defGraphQuery);
        updateQuery.execute();

        BooleanQuery booleanQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, checkQuery);
        boolean results = booleanQuery.evaluate();
        Assert.assertEquals(true, results);

        conn.clear(context);
    }

    @Ignore
    public void testGetGraphPermsofResult(){

    }

    @Ignore
    public void testGetGraphPermsofStatement(){

    }

}