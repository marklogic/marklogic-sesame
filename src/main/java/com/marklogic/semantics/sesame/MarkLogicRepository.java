package com.marklogic.semantics.sesame;

import com.marklogic.semantics.sesame.client.MarkLogicClient;
import com.marklogic.semantics.sesame.client.MarkLogicClientDependent;
import org.openrdf.model.ValueFactory;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.base.RepositoryBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class MarkLogicRepository extends RepositoryBase implements MarkLogicClientDependent {

    protected final Logger logger = LoggerFactory.getLogger(MarkLogicRepository.class);

    private MarkLogicClient client;

    private String host;

    private int port;

    private String user;

    private String password;

    private String auth = "DIGEST";

    private boolean quadMode = false;

    public MarkLogicRepository() {
        super();
        this.client = getMarkLogicClient();
    }

    public MarkLogicRepository(String host, int port, String user, String password, String auth) {
        super();
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.auth = auth;
        this.client = getMarkLogicClient();
    }

//    public MarkLogicRepository(String endpointUrl) {
//    }

    @Override
    protected void initializeInternal() throws RepositoryException {
    }

    @Override
    protected void shutDownInternal() throws RepositoryException {
    }

    @Override
    public File getDataDir() {
        return null;
    }

    @Override
    public void setDataDir(File dataDir) {
    }

    @Override
    public boolean isWritable() throws RepositoryException {
        return false;
    }

    public RepositoryConnection getConnection()
            throws RepositoryException {
        if (!isInitialized()) {
            throw new RepositoryException("MarkLogicRepository not initialized.");
        }
        return new MarkLogicRepositoryConnection(this, client, quadMode);
    }

    @Override
    public ValueFactory getValueFactory() {
        return null;
    }

    @Override
    public synchronized MarkLogicClient getMarkLogicClient() {
        if (client == null) {
            client = new MarkLogicClient(host, port, user, password, auth); // consider factory method ?
        }
        return client;
    }

    @Override
    public synchronized void setMarkLogicClient(MarkLogicClient client) {
        this.client = client;
    }

    public boolean isQuadMode() {
        return quadMode;
    }

    public void setQuadMode(boolean quadMode) {
        this.quadMode = quadMode;
    }
}