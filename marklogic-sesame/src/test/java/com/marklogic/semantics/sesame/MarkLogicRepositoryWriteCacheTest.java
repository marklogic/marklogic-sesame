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

import org.junit.*;
import org.junit.rules.ExpectedException;
import org.openrdf.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

/**
 * tests write cache
 *
 * @author James Fuller
 */
// @FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MarkLogicRepositoryWriteCacheTest extends SesameTestBase {

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
        if(conn.isActive() && conn.isOpen()){conn.rollback();}
        if(conn.isOpen()){conn.clear();}
        conn.close();
        conn = null;
        rep.shutDown();
        rep = null;
        logger.info("tearDown complete.");
    }

    // https://github.com/marklogic/marklogic-sesame/issues/140
    // https://github.com/marklogic/marklogic-sesame/issues/140
    // waiting on resolution of 183 and mergeGraphs with txid
    @Test @Ignore
    public void testStatementWithWriteCache() throws Exception {
        Resource context1 = conn.getValueFactory().createURI("http://marklogic.com/test/context1");
        Resource context2 = conn.getValueFactory().createURI("http://marklogic.com/test/context2");

        ValueFactory f= conn.getValueFactory();

        URI alice = f.createURI("http://example.org/people/alice");
        URI name = f.createURI("http://example.org/ontology/name");
        Literal alicesName = f.createLiteral("Alice1");

        Statement st1 = f.createStatement(alice, name, alicesName, context1);
        conn.add(st1);
        conn.begin();
        int count = 0;
        for (int i=0 ; i<10000 ; i++){
            Literal obj = f.createLiteral("Alice" + count);
            if ( (i & 1) == 0 ) {
                Statement st = f.createStatement(alice, name,obj);
                conn.add(st,context1,context2);
            }else{
                Statement st = f.createStatement(alice, name,obj);
                conn.add(st);
            }
            count = count + 1;
        }
        conn.commit();

        assertEquals("Incorrect number of triples.", 15001, conn.size());
        conn.clear();
    }
}
