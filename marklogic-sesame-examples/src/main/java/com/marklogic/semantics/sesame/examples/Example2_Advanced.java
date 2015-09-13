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

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.semantics.sesame.MarkLogicRepository;
import com.marklogic.semantics.sesame.MarkLogicRepositoryConnection;
import com.marklogic.semantics.sesame.query.MarkLogicTupleQuery;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.FOAF;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.*;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Example2_Advanced {

    protected static Logger logger =LoggerFactory.getLogger(Example2_Advanced.class);

    public static void main(String... args) throws RepositoryException, IOException, RDFParseException, MalformedQueryException, QueryEvaluationException {

        // instantiate MarkLogicRepository with Java api client DatabaseClient
        DatabaseClient adminClient = DatabaseClientFactory.newClient("localhost", 8200, "admin","admin", DatabaseClientFactory.Authentication.DIGEST);
        MarkLogicRepository repo = new MarkLogicRepository(adminClient);
        repo.initialize();

        // get repository connection
        MarkLogicRepositoryConnection conn = repo.getConnection();

        // set default rulesets
        //conn.setDefaultRulesets();

        // set default perms
        //conn.setDefaultGraphPerms();

        // set default Constraining Query
        //conn.setDefaultQueryDef();

        // return number of triples contained in repository
        logger.info("number of triples: {}", conn.size());

        // add a few constructed triples
        Resource context1 = conn.getValueFactory().createURI("http://marklogic.com/examples/context1");
        Resource context2 = conn.getValueFactory().createURI("http://marklogic.com/examples/context2");
        ValueFactory f= conn.getValueFactory();
        String namespace = "http://example.org/";
        URI john = f.createURI(namespace, "john");

        conn.begin();
        conn.add(john, RDF.TYPE, FOAF.PERSON, context1);
        conn.add(john, RDFS.LABEL, f.createLiteral("John", XMLSchema.STRING), context2);
        logger.info("number of uncommited triples: {}", conn.size());
        conn.rollback();
        logger.info("number of triples: {}", conn.size());

        // perform SPARQL query with pagination
        String queryString = "select * { ?s ?p ?o }";
        MarkLogicTupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
        tupleQuery.setIncludeInferred(true);

        //tupleQuery.setBaseURI();
        //tupleQuery.setRulesets();
        //tupleQuery.setConstrainingQueryDefinition();
        //tupleQuery.setGraphPerms();

        TupleQueryResult results = tupleQuery.evaluate(1,10);

        //iterate through query results
        while(results.hasNext()){
            BindingSet bindings = results.next();
            logger.info("subject:{}",bindings.getValue("s"));
            logger.info("predicate:{}", bindings.getValue("p"));
            logger.info("object:{}", bindings.getValue("o"));
        }

        //update query

        // clear all triples
        conn.clear();
        logger.info("number of triples: {}", conn.size());

        // close connection and shutdown repository
        conn.close();
        repo.shutDown();
    }
}