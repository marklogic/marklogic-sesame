package com.marklogic.semantics.sesame.example;

import com.marklogic.semantics.sesame.example.toberemoved.ExampleJavaAPICreateJson;
import org.junit.Assert;
import org.junit.Ignore;

import java.io.IOException;

/**
 * Created by jfuller on 6/24/15.
 */
public class ExampleJunitTest {
    @Ignore
    public void testHello() {
        String message = "Hello World!";
        Assert.assertEquals(message,"Hello World!");
    }

    @Ignore
    public void testObject() throws IOException {

        ExampleJavaAPICreateJson.main();
        Assert.assertEquals("test","test");
    }
}
