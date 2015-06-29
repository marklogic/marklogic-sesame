package com.marklogic.semantics.sesame.client;


import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;

/**
 * implements client using MarkLogic java api client
 *
 * @author James Fuller
 */
public class MarkLogicClient {

    protected static String host = "localhost";

    protected static int port = 8000;

    protected static String user = "admin";

    protected static String password = "admin";

    protected static DatabaseClientFactory.Authentication authType = DatabaseClientFactory.Authentication.valueOf(
            "DIGEST"
    );

    private DatabaseClient databaseClient;

    public MarkLogicClient() {
        this.databaseClient = DatabaseClientFactory.newClient(host, port, user, password, authType);
    }

}