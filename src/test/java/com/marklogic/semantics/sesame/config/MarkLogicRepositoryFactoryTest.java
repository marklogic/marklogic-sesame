package com.marklogic.semantics.sesame.config;

import com.marklogic.semantics.sesame.MarkLogicRepositoryConnection;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.config.RepositoryFactory;

public class MarkLogicRepositoryFactoryTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Ignore
    public void testGetRepositoryType() throws Exception {

    }

    @Ignore
    public void testGetConfig() throws Exception {

    }

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
        RepositoryConnection conn = otherrepo.getConnection();
    }
}