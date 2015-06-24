package com.marklogic.semantic.sesame;

import org.junit.Test;

import static com.jayway.restassured.RestAssured.given;


/**
 * Created by jfuller on 31/03/14.
 */
public class ExampleRestAssuredTest {
    @Test
    public void testGetEndpoint() {
        given().
                auth().digest("admin", "admin").
                given().
                get("http://localhost:8000").
                then().assertThat().statusCode(200);

    }
}