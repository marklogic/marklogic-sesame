package com.marklogic.semantic.sesame;

import com.marklogic.client.example.Example_01_CreateJSON;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by jfuller on 6/24/15.
 */
public class ExampleJunitTest {
    @Test
    public void testHello() {
        String message = "Hello World!";
        Assert.assertEquals(message,"Hello World!");
    }

    @Test
    public void testObject() throws IOException {

        Example_01_CreateJSON.main();
        Assert.assertEquals("test","test");
    }
}
