package com.marklogic.semantics.sesame;

import org.junit.Assert;
import org.junit.Test;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;

public class MarkLogicRepositoryConnectionTest {

    @Test
    public void testSPARQLQuery()
            throws Exception
    {
        MarkLogicRepository mr = new MarkLogicRepository();

        mr.shutDown();
        mr.initialize();

        MarkLogicRepositoryConnection con = (MarkLogicRepositoryConnection) mr.getConnection();

        Assert.assertTrue( con != null );
        String queryString = "select ?s ?p ?o { ?s ?p ?o } limit 1 ";
        TupleQuery tupleQuery =  con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
        TupleQueryResult results = tupleQuery.evaluate();

        try {
            while (results.hasNext()) {
                BindingSet bindingSet = results.next();

                System.out.println(bindingSet.size());
                Assert.assertTrue(bindingSet.size() > 1);
                Value sV = bindingSet.getValue("s");
                Value pV = bindingSet.getValue("p");
                Value oV = bindingSet.getValue("o");

                Assert.assertEquals("http://example.org/marklogic/people/Jack_Smith",sV.stringValue());
                Assert.assertEquals("http://example.org/marklogic/predicate/livesIn",pV.stringValue());
                Assert.assertEquals("Glasgow", oV.stringValue());
            }
        }
        finally {
            results.close();
        }
        con.close();
        }
    }
