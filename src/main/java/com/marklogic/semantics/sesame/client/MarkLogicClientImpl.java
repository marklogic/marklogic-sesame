package com.marklogic.semantics.sesame.client;


import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.semantics.SPARQLQueryDefinition;
import com.marklogic.client.semantics.SPARQLQueryManager;
import com.marklogic.client.semantics.SPARQLTupleResults;

/**
 * implements client using MarkLogic java api client
 *
 * @author James Fuller
 */
public class MarkLogicClientImpl {

    protected static String host = "localhost";

    protected static int port = 8200;

    protected static String user = "admin";

    protected static String password = "admin";

    protected DatabaseClient databaseClient;

    public static DatabaseClientFactory.Authentication getAuthType() {
        return authType;
    }

    public static void setAuthType(DatabaseClientFactory.Authentication authType) {
        MarkLogicClientImpl.authType = authType;
    }

    public static String getHost() {
        return host;
    }

    public static void setHost(String host) {
        MarkLogicClientImpl.host = host;
    }

    public static int getPort() {
        return port;
    }

    public static void setPort(int port) {
        MarkLogicClientImpl.port = port;
    }

    public static String getUser() {
        return user;
    }

    public static void setUser(String user) {
        MarkLogicClientImpl.user = user;
    }

    public static String getPassword() {
        return password;
    }

    public static void setPassword(String password) {
        MarkLogicClientImpl.password = password;
    }

    protected static DatabaseClientFactory.Authentication authType = DatabaseClientFactory.Authentication.valueOf(
            "DIGEST"
    );

    public DatabaseClient getDatabaseClient() {
        return databaseClient;
    }

    public MarkLogicClientImpl() {
        this.databaseClient = DatabaseClientFactory.newClient(host, port, user, password, authType);
    }

    public SPARQLTupleResults performSPARQLQuery(String queryString){
        SPARQLQueryManager smgr = this.databaseClient.newSPARQLQueryManager();
        SPARQLQueryDefinition qdef = smgr.newQueryDefinition(queryString);
        return smgr.executeSelect(qdef);
    }
}