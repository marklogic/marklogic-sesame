package com.marklogic.semantics.sesame.client;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.impl.SPARQLBindingsImpl;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.client.semantics.SPARQLBindings;
import com.marklogic.client.semantics.SPARQLQueryDefinition;
import com.marklogic.client.semantics.SPARQLQueryManager;
import org.openrdf.query.Binding;
import org.openrdf.query.impl.MapBindingSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

/**
 *
 * @author James Fuller
 */
public class MarkLogicClientImpl {

    protected final Logger logger = LoggerFactory.getLogger(MarkLogicClientImpl.class);

    private String host;

    private int port;

    private String user;

    private String password;

    private String auth;

    // tbd
    private long start = 1;
    private long pageLength = 1000;

    static public SPARQLQueryManager sparqlManager;

    protected static DatabaseClientFactory.Authentication authType = DatabaseClientFactory.Authentication.valueOf(
            "DIGEST"
    );

    protected DatabaseClient databaseClient;

    public MarkLogicClientImpl(String host, int port, String user, String password, String auth) {
        this.databaseClient = DatabaseClientFactory.newClient(host, port, user, password, DatabaseClientFactory.Authentication.valueOf(auth));
    }

    public DatabaseClientFactory.Authentication getAuthType() {
        return authType;
    }

    public void setAuthType(DatabaseClientFactory.Authentication authType) {
        MarkLogicClientImpl.authType = authType;
    }

    public String getHost() {
        return this.host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return this.port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUser() {
        return this.user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword() {
        this.password = password;
    }

    public String getAuth() {
        return auth;
    }

    public void setAuth(String auth) {
        this.auth = auth;
        this.authType = DatabaseClientFactory.Authentication.valueOf(
                auth
        );
    }

    public DatabaseClient getDatabaseClient() {
        return databaseClient;
    }

    public InputStream performSPARQLQuery(String queryString, MapBindingSet bindings) throws JsonProcessingException {
        return performSPARQLQuery(queryString, bindings, new InputStreamHandle());
    }

    public InputStream performSPARQLQuery(String queryString, MapBindingSet bindings, InputStreamHandle handle) throws JsonProcessingException {
        sparqlManager = getDatabaseClient().newSPARQLQueryManager();
        SPARQLQueryDefinition qdef = sparqlManager.newQueryDefinition(queryString);
        SPARQLBindings sps = new SPARQLBindingsImpl();
        for (Binding binding : bindings) {
            sps.bind(binding.getName(), binding.getValue().stringValue());
            logger.debug("binding:" + binding.getName() + "=" + binding.getValue());
        }
        qdef.setBindings(sps);
        sparqlManager.executeSelect(qdef, handle);
        return handle.get();
    }
}