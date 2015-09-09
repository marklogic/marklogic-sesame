package com.marklogic.sesame.examples;

import com.marklogic.semantics.sesame.MarkLogicRepository;
import com.marklogic.semantics.sesame.MarkLogicRepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BasicUsage {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    MarkLogicRepository repo;
    MarkLogicRepositoryConnection conn;

    public BasicUsage(MarkLogicRepository repo) throws RepositoryException {
        this.repo =ExampleUtils.loadPropsAndInit();
        this.conn = repo.getConnection();
    }

    public void run() throws RepositoryException {
        conn.size();
    }
}

