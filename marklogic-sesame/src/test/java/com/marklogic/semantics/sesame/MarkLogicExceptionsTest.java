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

import com.marklogic.client.ResourceNotFoundException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.openrdf.IsolationLevels;
import org.openrdf.model.Resource;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;

/**
 *
 * @author James Fuller
 */
public class MarkLogicExceptionsTest extends SesameTestBase {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Rule
    public ExpectedException exception = ExpectedException.none();

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
    public void TestMalformedQuery() throws RepositoryException, MalformedQueryException, QueryEvaluationException {
        exception.expect(QueryEvaluationException.class);
        TupleQueryResult results = conn.prepareTupleQuery("A malformed query").evaluate();
    }

    @Test
    public void TestMalformedBooleanQuery() throws RepositoryException, MalformedQueryException, QueryEvaluationException {
        exception.expect(QueryEvaluationException.class);
        boolean results = conn.prepareBooleanQuery("A malformed query").evaluate();
    }

    @Test
    public void TestMalformedUpdateQUery() throws RepositoryException, MalformedQueryException, UpdateExecutionException {
        exception.expect(UpdateExecutionException.class);
        conn.prepareUpdate("A malformed query").execute();
    }

    // https://github.com/marklogic/marklogic-sesame/issues/65
    @Test
    public void testAddMalformedTurtle() throws Exception {
        File inputFile = new File("src/test/resources/testdata/malformed-literals.ttl");
        String baseURI = "http://example.org/example1/";
        Resource context1 = conn.getValueFactory().createURI("http://marklogic.com/test/context1");
        exception.expect(RDFParseException.class);
        conn.add(inputFile, baseURI, RDFFormat.TURTLE, context1);
        conn.clear(context1);
    }

    @Test
    public void testIncorrectIsolatedLevel() throws Exception {
        exception.expect(IllegalStateException.class);
        conn.setIsolationLevel(IsolationLevels.READ_UNCOMMITTED);
    }

    @Test
    public void updateWithWrongPerms() throws RepositoryException, MalformedQueryException, UpdateExecutionException {
        readerRep.initialize();
        MarkLogicRepositoryConnection testReaderCon = readerRep.getConnection();
        exception.expect(UpdateExecutionException.class);
        testReaderCon.prepareUpdate("CREATE GRAPH <abc>").execute();
    }

    @Test
    public void testAddWithInputStream() throws Exception {
        exception.expect(ResourceNotFoundException.class);

        File inputFile = new File("src/test/resources/testdata/default-graph-1.ttl");
        FileInputStream is = new FileInputStream(inputFile);
        String baseURI = "http://example.org/example1/";
        Resource context3 = conn.getValueFactory().createURI("http://marklogic.com/test/context3");
        Resource context4 = conn.getValueFactory().createURI("http://marklogic.com/test/context4");
        conn.add(is, baseURI, RDFFormat.TURTLE, context3); // TBD - add multiple context
        conn.clear(context3, context4); // ensure we throw error as context not defined
    }


    @Test
    public void testTransaction3() throws Exception {
        exception.expect(ResourceNotFoundException.class);
        Resource context1 = conn.getValueFactory().createURI("http://marklogic.com/test/my-graph");
        conn.begin();
        conn.clear(context1);
        conn.rollback();
    }
}
