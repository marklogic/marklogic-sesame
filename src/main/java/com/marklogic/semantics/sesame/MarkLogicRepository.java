package com.marklogic.semantics.sesame;

import com.marklogic.semantics.sesame.client.MarkLogicClientDependent;
import com.marklogic.semantics.sesame.client.MarkLogicClient;
import org.openrdf.model.ValueFactory;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.base.RepositoryBase;

import java.io.File;

public class MarkLogicRepository extends RepositoryBase implements MarkLogicClientDependent {

    private MarkLogicClient client;

    public MarkLogicRepository() {
        super();
    }

    public MarkLogicRepository(String endpointUrl) {
    }

    @Override
    protected void initializeInternal() throws RepositoryException {

    }

    @Override
    protected void shutDownInternal() throws RepositoryException {

    }

    @Override
    public void setDataDir(File dataDir) {

    }

    @Override
    public File getDataDir() {
        return null;
    }

    @Override
    public boolean isWritable() throws RepositoryException {
        return false;
    }

    public RepositoryConnection getConnection()
            throws RepositoryException
    {
        if (!isInitialized()) {
            throw new RepositoryException("MarkLogicRepository not initialized.");
        }
        return new MarkLogicRepositoryConnection(this,client);
    }

    @Override
    public ValueFactory getValueFactory() {
        return null;
    }

    @Override
    public synchronized MarkLogicClient getMarkLogicClient() {
        if (client == null) {
            client =  new MarkLogicClient();
        }
        return client;
    }

    @Override
    public synchronized void setMarkLogicClient(MarkLogicClient client) {
        this.client = client;
    }
}
