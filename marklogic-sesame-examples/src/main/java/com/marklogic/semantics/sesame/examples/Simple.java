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
package com.marklogic.semantics.sesame.examples;

import com.marklogic.semantics.sesame.MarkLogicRepository;
import com.marklogic.semantics.sesame.MarkLogicRepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Simple {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    MarkLogicRepository repo;
    MarkLogicRepositoryConnection conn;

    public Simple() throws RepositoryException {
        System.out.println("setup");
        this.repo = Setup.loadPropsAndInit(); // invoke new MarkLogicRepository(host,port,user,pass,"DIGEST");
        this.repo.initialize(); // initialise repository
        this.conn = repo.getConnection(); // get a repository connection
    }

    public void teardown() throws RepositoryException {
        System.out.println("teardown");
        this.conn.close(); // close connection
        this.repo.shutDown();
    }

    public void tripleCount() throws RepositoryException {
            System.out.println("number of triples");
            System.out.println(this.conn.size());
    }

    public static void main(String... args) throws RepositoryException {
        System.out.println("instantiate Simple class");
        Simple simple = new Simple(); // we instantiate so we can call non static methods
        try {
            System.out.println("start examples");
            simple.tripleCount(); // return number of triples in repo
            System.out.println("finished examples");
        }finally {
            simple.teardown();
        }
    }
}

