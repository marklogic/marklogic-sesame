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

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author James Fuller
 */
public class MarkLogicRepositoryTest extends SesameTestBase {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void testRepo1()
            throws Exception {

        logger.info("setting up repo");
        rep.initialize();

        Assert.assertTrue(rep instanceof Repository);
        rep.shutDown();
    }

    @Test
    public void TestRepo2()
            throws Exception {

        // TBD this will pass, so do we want to throw connection error ?
        Repository rep = new MarkLogicRepository("localhost", 8200, "admin", "admin", "DIGEST");
        rep.initialize();
        rep.shutDown();

        exception.expect(RepositoryException.class);
        exception.expectMessage("MarkLogicRepository not initialized.");
        RepositoryConnection conn = rep.getConnection();
    }
}
