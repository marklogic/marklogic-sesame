package com.marklogic.semantics.sesame.example;

import org.junit.Ignore;

import static com.jayway.restassured.RestAssured.given;


/**
 * Created by jfuller on 31/03/14.
 */
public class ExampleRestAssuredTest {
    @Ignore
    public void testGetEndpoint() {
        given().
                auth().digest("admin", "admin").
                given().
                get("http://localhost:8000").
                then().assertThat().statusCode(200);

    }
}