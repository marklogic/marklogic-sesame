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
package com.marklogic.semantics.sesame;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class SesameTestBase {

    public static String host;
    public static int port;
    public static String user;
    public static String password;

    public static String adminUser;
    public static String adminPassword;
    public static String validUser;
    public static String validPassword;
    public static String invalidUser;
    public static String invalidPassword;
    public static String writerUser;
    public static String writerPassword;
    public static String readerUser;
    public static String readerPassword;

    public MarkLogicRepository rep;
    public MarkLogicRepository writerRep;
    public MarkLogicRepository readerRep;

    public DatabaseClient readerClient;
    public DatabaseClient writerClient;
    public DatabaseClient adminClient;

    public SesameTestBase() {
        Properties props = new Properties();
        try {
            props.load(new FileInputStream("gradle.properties"));
        } catch (IOException e) {
            System.err.println("Properties file not loaded.");
            System.exit(1);
        }
        host = props.getProperty("mlHost");
        port = Integer.parseInt(props.getProperty("mlRestPort"));
        user = props.getProperty("mlUsername");
        password = props.getProperty("mlPassword");
        adminUser = props.getProperty("mlAdminUsername");
        adminPassword = props.getProperty("mlAdminPassword");

        validUser = props.getProperty("validUsername");
        validPassword = props.getProperty("validPassword");
        invalidUser = props.getProperty("invalidUsername");
        invalidPassword = props.getProperty("invalidPassword");

        writerUser = props.getProperty("writerUser");
        writerPassword = props.getProperty("writerPassword");
        readerUser = props.getProperty("readerUser");
        readerPassword = props.getProperty("readerPassword");

        adminClient = DatabaseClientFactory.newClient(host, port, validUser, validPassword, DatabaseClientFactory.Authentication.DIGEST);
        writerClient = DatabaseClientFactory.newClient(host, port, writerUser, writerPassword, DatabaseClientFactory.Authentication.DIGEST);
        readerClient = DatabaseClientFactory.newClient(host, port, writerUser, writerPassword, DatabaseClientFactory.Authentication.DIGEST);

        rep = new MarkLogicRepository(host,port,validUser,validPassword, "DIGEST");
        writerRep = new MarkLogicRepository(host,port,writerUser,writerPassword, "DIGEST");
        readerRep = new MarkLogicRepository(host,port,readerUser,readerPassword, "DIGEST");
    }
}


