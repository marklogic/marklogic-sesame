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

import com.marklogic.semantics.sesame.client.MarkLogicClient;
import com.marklogic.semantics.sesame.client.MarkLogicClientDependent;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.base.RepositoryBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 *
 * @author James Fuller
 */
public class MarkLogicRepository extends RepositoryBase implements MarkLogicClientDependent {

    protected final Logger logger = LoggerFactory.getLogger(MarkLogicRepository.class);

    private MarkLogicClient client;

    private String host;

    private int port;

    private String user;

    private String password;

    private String auth;

    private boolean quadMode;

    private ValueFactory f;

    // constructors
    public MarkLogicRepository() {
        super();
        this.client = getMarkLogicClient();
        this.f = new ValueFactoryImpl();
        this.quadMode = false;
        this.auth = "DIGEST";
    }
    public MarkLogicRepository(String host, int port, String user, String password, String auth) {
        super();
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.auth = auth;
        this.client = getMarkLogicClient();
        this.f = new ValueFactoryImpl();
        this.quadMode = false;
    }

    // valuefactory
    public ValueFactory getValueFactory() {
        return this.f;
    }
    public void setValueFactory(ValueFactory f) {
        this.f=f;
    }

    // initialize
    @Override
    protected void initializeInternal() throws RepositoryException {
    }

    //shutdown
    @Override
    protected void shutDownInternal() throws RepositoryException {
    }

    // DataDir has no relevance to MarkLogic
    @Override
    public File getDataDir() {
        return null;
    }
    @Override
    public void setDataDir(File dataDir) {
    }

    //
    @Override
    public boolean isWritable() throws RepositoryException {
        return true;
    }

    //
    public RepositoryConnection getConnection()
            throws RepositoryException {
        if (!isInitialized()) {
            throw new RepositoryException("MarkLogicRepository not initialized.");
        }
        return new MarkLogicRepositoryConnection(this, client, quadMode);
    }

    // MarkLogicClient
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

    // quad mode
    public boolean isQuadMode() {
        return quadMode;
    }
    public void setQuadMode(boolean quadMode) {
        this.quadMode = quadMode;
    }
}