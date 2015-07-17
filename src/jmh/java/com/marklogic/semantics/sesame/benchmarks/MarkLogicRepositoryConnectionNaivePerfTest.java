package com.marklogic.semantics.sesame.benchmarks;

import com.marklogic.semantics.sesame.MarkLogicRepository;
import org.openjdk.jmh.annotations.Benchmark;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class MarkLogicRepositoryConnectionNaivePerfTest {

    @Benchmark
    public void perfNaiveQuery1()
            throws Exception {

        Properties props = new Properties();
        try {
            props.load(new FileInputStream("gradle.properties"));
        } catch (IOException e) {
            System.err.println("problem loading properties file.");
            System.exit(1);
        }
        String host = props.getProperty("mlHost");
        int port = Integer.parseInt(props.getProperty("mlRestPort"));
        String user = props.getProperty("adminUser");
        String pass = props.getProperty("adminPassword");
        // extrude to semantics.utils

        Repository rep = new MarkLogicRepository(host,port,user,pass,"DIGEST");
        rep.initialize();

        ValueFactory f = rep.getValueFactory();
        RepositoryConnection conn = rep.getConnection();
        rep.shutDown();
        rep.initialize();

        String queryString = "select ?s ?p ?o { ?s ?p ?o } limit 2 ";
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
        TupleQueryResult results = tupleQuery.evaluate();

        conn.close();

        //rep.shutDown();
//        results.hasNext();
//        BindingSet bindingSet = results.next();
//
//        Value sV = bindingSet.getValue("s");
//        Value pV = bindingSet.getValue("p");
//        Value oV = bindingSet.getValue("o");
//
//        results.hasNext();
//        BindingSet bindingSet1 = results.next();
//
//        Value sV1 = bindingSet1.getValue("s");
//        Value pV1 = bindingSet1.getValue("p");
//        Value oV1 = bindingSet1.getValue("o");
    }

}
