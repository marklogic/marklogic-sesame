package com.marklogic.semantics.sesame.client;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by jfuller on 6/24/15.
 */
public class MarkLogicClientTest {
    @Test
    public void testClient() {
        MarkLogicClientImpl.setPort(9000);
        MarkLogicClientImpl.setHost("localhost");
        MarkLogicClientImpl.setPassword("admin");
        MarkLogicClientImpl.setUser("admin");

        Assert.assertEquals(MarkLogicClientImpl.getPort(), 9000);
        Assert.assertEquals(MarkLogicClientImpl.getHost(),"localhost");
        Assert.assertEquals(MarkLogicClientImpl.getUser(),"admin");
        Assert.assertEquals(MarkLogicClientImpl.getPassword(),"admin");
    }

}
