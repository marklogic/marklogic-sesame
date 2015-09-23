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
package com.marklogic.semantics.sesame.config;

import com.marklogic.semantics.sesame.MarkLogicRepositoryConnection;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.config.RepositoryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * test factory
 *
 * @author James Fuller
 */
public class MarkLogicRepositoryFactoryTest {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void testGetRepository() throws Exception {
        MarkLogicRepositoryConfig config = new MarkLogicRepositoryConfig();

        config.setHost("localhost");
        config.setPort(8200);
        config.setUser("admin");
        config.setPassword("admin");
        config.setAuth("DIGEST");

        RepositoryFactory factory = new MarkLogicRepositoryFactory();
        Assert.assertEquals("marklogic:MarkLogicRepository", factory.getRepositoryType());
        Repository repo = factory.getRepository(config);
        repo.initialize();
        Assert.assertTrue(repo.getConnection() instanceof MarkLogicRepositoryConnection);

        Repository otherrepo = factory.getRepository(config);
        exception.expect(RepositoryException.class);
        //try to get connection without initialising repo, throws error
        @SuppressWarnings("unused")
        RepositoryConnection conn = otherrepo.getConnection();
    }

    @Test
    public void testGetRepositoryWithConstructor() throws Exception {
        MarkLogicRepositoryConfig config = new MarkLogicRepositoryConfig("localhost",8200,"admin","admin","DIGEST");

        RepositoryFactory factory = new MarkLogicRepositoryFactory();
        Assert.assertEquals("marklogic:MarkLogicRepository", factory.getRepositoryType());
        Repository repo = factory.getRepository(config);
        repo.initialize();
        Assert.assertTrue(repo.getConnection() instanceof MarkLogicRepositoryConnection);

        Repository otherrepo = factory.getRepository(config);
        exception.expect(RepositoryException.class);
        //try to get connection without initialising repo, will throw error
        @SuppressWarnings("unused")
        RepositoryConnection conn = otherrepo.getConnection();
    }
}