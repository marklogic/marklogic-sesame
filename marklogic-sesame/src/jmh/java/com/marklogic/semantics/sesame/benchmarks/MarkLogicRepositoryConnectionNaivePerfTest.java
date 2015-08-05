package com.marklogic.semantics.sesame.benchmarks;

import com.marklogic.semantics.sesame.MarkLogicRepository;
import com.marklogic.semantics.sesame.MarkLogicRepositoryConnection;
import org.openjdk.jmh.annotations.Benchmark;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.BindingSet;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class MarkLogicRepositoryConnectionNaivePerfTest {

    private static final Logger logger = LoggerFactory.getLogger(MarkLogicRepositoryConnectionNaivePerfTest.class);

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
        String user = props.getProperty("mlUsername");
        String pass = props.getProperty("mlPassword");
        // extrude to semantics.utils

        MarkLogicRepository rep = new MarkLogicRepository(host,port,user,pass,"DIGEST");
        rep.initialize();

        ValueFactory f = rep.getValueFactory();
        MarkLogicRepositoryConnection conn = rep.getConnection();
        rep.shutDown();
        rep.initialize();

        File inputFile = new File("src/jmh/resources/testdata/default-graph-1.ttl");
        FileInputStream is = new FileInputStream(inputFile);
        String baseURI = "http://example.org/example1/";
        Resource context1 = conn.getValueFactory().createURI("http://marklogic.com/test/context3");
        conn.add(is, baseURI, RDFFormat.TURTLE, context1);

        String queryString = "select ?s ?p ?o { ?s ?p ?o } limit 100 ";
        TupleQuery tupleQuery = conn.prepareTupleQuery(queryString);
        TupleQueryResult results = tupleQuery.evaluate();

        while(results.hasNext()) {
            BindingSet bindingSet = results.next();
            Value sV = bindingSet.getValue("s");
            Value pV = bindingSet.getValue("p");
            Value oV = bindingSet.getValue("o");
        }
        Resource subject = conn.getValueFactory().createURI("urn:x-local:graph1");
        RepositoryResult<Statement> statements = conn.getStatements(subject, null, null, true, context1);

        if(statements.hasNext()){
            logger.debug("getStatements worked");
        }

        logger.debug("size:{}",conn.size(context1));
        conn.clear(context1);
        conn.close();
        rep.shutDown();
    }
}
