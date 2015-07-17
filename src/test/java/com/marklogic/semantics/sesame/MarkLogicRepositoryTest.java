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
 * Created by jfuller on 7/13/15.
 */
public class MarkLogicRepositoryTest {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void testRepo1()
            throws Exception {

        logger.info("setting up repo");
        Repository rep = new MarkLogicRepository("localhost", 8200, "admin", "admin", "DIGEST");
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
