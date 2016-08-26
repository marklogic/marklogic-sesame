/*
 * Copyright 2015-2016 MarkLogic Corporation
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
package com.marklogic.semantics.sesame.query;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.io.FileHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.semantics.GraphManager;
import com.marklogic.client.semantics.RDFMimeTypes;
import com.marklogic.client.semantics.SPARQLRuleset;
import com.marklogic.semantics.sesame.MarkLogicRepository;
import com.marklogic.semantics.sesame.MarkLogicRepositoryConnection;
import com.marklogic.semantics.sesame.SesameTestBase;
import info.aduna.iteration.ConvertingIteration;
import info.aduna.iteration.ExceptionConvertingIteration;
import org.junit.*;
import org.openrdf.model.Resource;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.*;
import org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Properties;

/**
 * test CLOSE_WAIT
 *
 * @author James Fuller
 */
public class MarkLogicQueryTest  {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    private static GraphManager gmgr;

    public static DatabaseClient writerClient;

    protected static final String TESTFILE_OWL = "src/test/resources/testdata/test-small.owl";

    @Before
    public void setUp() throws RepositoryException, FileNotFoundException {

        Properties props = new Properties();
        try {
            props.load(new FileInputStream("gradle.properties"));
        } catch (IOException e) {
            System.err.println("Properties file not loaded.");
            System.exit(1);
        }
        String host = props.getProperty("mlHost");
        int port = Integer.parseInt(props.getProperty("mlRestPort"));
        String user = props.getProperty("mlUsername");
        String password = props.getProperty("mlPassword");
        String adminUser = props.getProperty("mlAdminUsername");
        String adminPassword = props.getProperty("mlAdminPassword");

        String validUser = props.getProperty("validUsername");
        String validPassword = props.getProperty("validPassword");
        String invalidUser = props.getProperty("invalidUsername");
        String invalidPassword = props.getProperty("invalidPassword");

        String writerUser = props.getProperty("writerUser");
        String writerPassword = props.getProperty("writerPassword");
        String readerUser = props.getProperty("readerUser");
        String readerPassword = props.getProperty("readerPassword");

        writerClient = DatabaseClientFactory.newClient(host, port, writerUser, writerPassword, DatabaseClientFactory.Authentication.DIGEST);

    }

    @After
    public void afterClass() {
        writerClient.release();
        //DatabaseClientFactory.getHandleRegistry().register(StringHandle.newFactory());
    }

    // https://github.coom/marklogic/marklogic-sesame/issues/287
    // a somewhat strange and generic test to gaurd against changes upstream to java api client
    //
    @Test
    public void testCloseWait()
            throws Exception {
        try {
            File testData = new File(TESTFILE_OWL);
            FileHandle testHandle = new FileHandle(testData);
            gmgr = writerClient.newGraphManager();
            gmgr.setDefaultMimetype(RDFMimeTypes.RDFXML);

            gmgr.write("/directory1/test.rdf", testHandle);
            Assert.assertEquals(1, 1);
            gmgr.delete("/directory1/test.rdf");
        } catch (Exception ex) {
            throw ex;
        } finally {
            //    Thread.sleep(10000);
        }
    }
}
