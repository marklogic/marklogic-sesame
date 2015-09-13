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

import com.marklogic.client.DatabaseClient;
import com.marklogic.semantics.sesame.client.MarkLogicClient;
import com.marklogic.semantics.sesame.client.MarkLogicClientDependent;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.base.RepositoryBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 *
 * Sesame repository representing a MarkLogic's triple store,
 * exposing MarkLogic specific features;
 *
 *  * SPARQL and Graph queries all SPARQL forms
 *  * rulesets for inferencing
 *  * efficient size queries
 *  * combination queries
 *  * base uri
 *  * permissions
 *
 * @author James Fuller
 * @version 1.0.0
 *
 */
public class MarkLogicRepository extends RepositoryBase implements Repository,MarkLogicClientDependent {

    protected final Logger logger = LoggerFactory.getLogger(MarkLogicRepository.class);

    // MarkLogicClient vars
    private MarkLogicClient client;
    private String host;
    private int port;
    private String user;
    private String password;
    private String auth;

    private boolean quadMode;

    private ValueFactory f;

    /**
     *
     * constructor inited with connection vars to MarkLogic server
     *
     * @param host
     * @param port
     * @param user
     * @param password
     * @param auth
     */
    public MarkLogicRepository(String host, int port, String user, String password, String auth) {
        super();
        this.f = new ValueFactoryImpl();
        this.quadMode = true;
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.auth = auth;
        this.client = getMarkLogicClient();
    }

    /**
     * constructor inited with java api client DatabaseClient
     *
     * @param databaseClient
     */
    public MarkLogicRepository(DatabaseClient databaseClient) {
        super();
        this.f = new ValueFactoryImpl();
        this.quadMode = true;
        this.client = new MarkLogicClient(databaseClient);
    }
    
    /**
     * gets the Valuefactory used for creating URIs, blank nodes, literals and statements.
     *
     * @return ValueFactory
     */
    public ValueFactory getValueFactory() {
        return this.f;
    }

    /**
     * sets the ValueFactory used for creating URIs, blank nodes, literals and statements
     *
     * @param f
     */
    public void setValueFactory(ValueFactory f) {
        this.f=f;
    }

    /**
     * @deprecated
     * implemented to honor Repository interface
     *
     * @throws RepositoryException
     */
    @Override
    protected void initializeInternal() throws RepositoryException {
    }

    /**
     * @deprecated
     * implemented to honor Repository interface
     *
     * @throws RepositoryException
     */
    @Override
    protected void shutDownInternal() throws RepositoryException {
    }

    /**
     * MarkLogic has no concept of data directory, so this returns null
     * implemented to honor Repository interface
     *
     * @return always returns null
     */
    @Override
    public File getDataDir() {
        return null;
    }

    /**
     * MarkLogic has no concept of data directory, so this does nothing
     * implemented to honor Repository interface
     *
     * @param dataDir
     */
    @Override
    public void setDataDir(File dataDir) {
    }

    /**
     * MarkLogic, with the correct perms is always writeable
     * implemented to honor Repository interface
     *
     * @return boolean
     * @throws RepositoryException
     */
    @Override
    public boolean isWritable() throws RepositoryException {
        return true;
    }

    /**
     * returns a MarkLogicConnection object which is the entry point to
     * performing all queries.
     *
     * @return MarkLogicRepositoryConnection
     * @throws RepositoryException
     */
    @Override
    public MarkLogicRepositoryConnection getConnection()
            throws RepositoryException {
        if (!isInitialized()) {
            throw new RepositoryException("MarkLogicRepository not initialized.");
        }
        return new MarkLogicRepositoryConnection(this, client, quadMode);
    }

    /**
     * returns MarkLogicClient object which manages communication to ML server via Java api client
     *
     * @return MarkLogicClient
     */
    @Override
    public synchronized MarkLogicClient getMarkLogicClient() {
        if (this.client == null) {
            this.client = new MarkLogicClient(host, port, user, password, auth); // consider factory method ?
        }
        return this.client;
    }

    /**
     * sets MarkLogicClient used by this repository
     *
     * @param client
     */
    @Override
    public synchronized void setMarkLogicClient(MarkLogicClient client) {
        this.client = client;
    }

    /**
     * returns if repository is in quadmode or not
     *
     * @return boolean
     */
    public boolean isQuadMode() {
        return quadMode;
    }

    /**
     * sets quadmode for this repository
     *
     * @param quadMode
     */
    public void setQuadMode(boolean quadMode) {
        this.quadMode = quadMode;
    }
}