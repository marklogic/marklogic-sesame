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
package com.marklogic.semantics.sesame.query;

import info.aduna.iteration.ConvertingIteration;
import info.aduna.iteration.ExceptionConvertingIteration;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.Resource;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.Query;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.QueryResultHandlerException;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.TupleQueryResultHandler;
import org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.marklogic.client.io.FileHandle;
import com.marklogic.client.semantics.GraphManager;
import com.marklogic.client.semantics.RDFMimeTypes;
import com.marklogic.client.semantics.SPARQLRuleset;
import com.marklogic.semantics.sesame.MarkLogicRepositoryConnection;
import com.marklogic.semantics.sesame.SesameTestBase;

/**
 * Created by jfuller on 8/11/15.
 */
public class MarkLogicTupleQueryTest extends SesameTestBase {

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
        File testData = new File(TESTFILE_OWL);

        GraphManager gmgr = writerClient.newGraphManager();
        gmgr.setDefaultMimetype(RDFMimeTypes.RDFXML);
        gmgr.write("/directory1/test.rdf", new FileHandle(testData));
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
        gmgr.delete("/directory1/test.rdf");
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
                Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#AttaliaGeodata", tuple.getBinding("s").getValue().stringValue());
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

        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#AttaliaGeodata", sV.stringValue());
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#altitude", pV.stringValue());
        Assert.assertEquals("0", oV.stringValue());

        BindingSet bindingSet1 = results.next();

        Value sV1 = bindingSet1.getValue("s");
        Value pV1 = bindingSet1.getValue("p");
        Value oV1 = bindingSet1.getValue("o");

        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#BabylonGeodata", sV1.stringValue());
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

        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#AttaliaGeodata", sV.stringValue());
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#altitude", pV.stringValue());
        Assert.assertEquals("0", oV.stringValue());

        BindingSet bindingSet1 = results.next();

        Value sV1 = bindingSet1.getValue("s");
        Value pV1 = bindingSet1.getValue("p");
        Value oV1 = bindingSet1.getValue("o");

        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#BabylonGeodata", sV1.stringValue());
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#altitude", pV1.stringValue());
        Assert.assertEquals("0", oV1.stringValue());
    }

    @Test
    public void testSPARQLQueryDistinct()
            throws Exception {

        try {
            String queryString = "SELECT DISTINCT ?_ WHERE { GRAPH ?ctx { ?s ?p ?o . } }";
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

            Assert.assertTrue(rr.hasNext()); //Resource resource = rr.next();

            //logger.debug(resource.stringValue());

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

        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#BethanyBeyondtheJordanGeodata", sV.stringValue());
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

        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#AttaliaGeodata", sV.stringValue());
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
                "\t\t\t\t<uri>http://semanticbible.org/ns/2006/NTNames#AttaliaGeodata</uri>\n" +
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

    @Test
    public void testSPARQLQueryWithEmptyResults()
            throws Exception {
        String queryString = "select * { <http://marklogic.com/nonexistent> ?p ?o } limit 100 ";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
        TupleQueryResult results = tupleQuery.evaluate();
        Assert.assertFalse(results.hasNext());
    }
}
