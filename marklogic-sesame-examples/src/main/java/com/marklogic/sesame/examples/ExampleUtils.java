package com.marklogic.sesame.examples;

import com.marklogic.semantics.sesame.MarkLogicRepository;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ExampleUtils {

    public static MarkLogicRepository loadPropsAndInit() {
        Properties props = new Properties();
        try {
            props.load(new FileInputStream("gradle.properties"));
        } catch (IOException e) {
            System.err.println("problem loading properties file.");
            System.exit(1);
        }
        String host = props.getProperty("mlHost");
        int port = Integer.parseInt(props.getProperty("mlRestPort"));
        String user = props.getProperty("writerUser");
        String pass = props.getProperty("writerPassword");

        return new MarkLogicRepository(host,port,user,pass,"DIGEST");
    }
}
