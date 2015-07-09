package com.marklogic.semantics.sesame.example;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.semantics.*;
import org.junit.Assert;
import org.junit.Ignore;

/**
 * Created by jfuller on 7/2/15.
 */
public class MarkLogicSessionTest {


    @Ignore
    public void testDatabaseClientForSanity() throws Exception {
        DatabaseClient mc = DatabaseClientFactory.newClient(
                "localhost", 8012, "admin", "admin", DatabaseClientFactory.Authentication.DIGEST);
        SPARQLQueryManager smgr = mc.newSPARQLQueryManager();
        SPARQLQueryDefinition qdef = smgr.newQueryDefinition("select ?s ?p ?o { ?s ?p ?o } limit 1");
        SPARQLTupleResults results = (SPARQLTupleResults) smgr.executeSelect(qdef);
        Assert.assertTrue(smgr instanceof SPARQLQueryManager);

        String[] bindingNames = results.getBindingNames();
        int i=0;
        for ( SPARQLTuple tuple : results ) {
            i++;
            System.out.println("Result number " + i);
            System.out.println(bindingNames[2]);
            for ( String bindingName: bindingNames ) {
                SPARQLBinding binding = tuple.get(bindingName);

                System.out.println(bindingName +
                        ":" + binding.getType() +
                        "@" + binding.getLanguageTag() +
                        "=[" + binding.getValue() + "]");
            }

        };
        Assert.assertTrue(results instanceof SPARQLTupleResults);
    }
}