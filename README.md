# marklogic-sesame-repository v1.0.0

## Introduction

markLogic-sesame-repository is a [Sesame](http://rdf4j.org/) Repository implementation that exposes MarkLogic semantic capabilities.

## Quick Start

_IMPORTANT_ - NO RELEASE TO MAVEN YET, YOU MUST BUILD SOFTWARE

For gradle projects, include:

```
dependencies {
    compile group: 'com.marklogic', name: 'marklogic-sesame-repository', version: '1.0.0'
}
```

To use the API in your maven project, include in your pom.xml:

```
<dependency>
    <groupId>com.marklogic</groupId>
    <artifactId>marklogic-sesame-repository</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Build and Test


#### Setup Java API Client

marklogic-sesame-repository depends on _develop_ branch version of Java API Client

1) clone or download Java API client _develop_ branch

[https://github.com/marklogic/java-client-api/tree/develop](https://github.com/marklogic/java-client-api/tree/develop)


2) build and deploy Java API client

```
 mvn -Dmaven.test.skip=true -Dmaven.javadoc.skip=true deploy
 ```

verify that Java API client has been deployed to your local maven repo.


#### Setup and Test MarkLogic Sesame Repository

marklogic-sesame-repository depends on MarkLogic (latest Nightly) installed and running;

1) clone or download marklogic-sesame _develop_ branch

[https://github.com/marklogic/marklogic-sesame/tree/develop](https://github.com/marklogic/marklogic-sesame/tree/develop)

2) run gradle target that provisions MarkLogic with everything required (database,REST server,etc)

review [marklogic-sesame/gradle.properties](marklogic-sesame/gradle.properties) that define test deployment settings then run target
```
gradle :marklogic-sesame:mlDeploy
```

3) test MarkLogic Sesame repository

```
gradle test

```
loads test data, builds and run unit tests.

(note- gradle requires Curl to be installed and available on your system's PATH)

#### Deployment

Build and deploy a local maven snapshot by running;

```
gradle install

```

### Links

[marklogic-sesame-repository](https://github.com/marklogic/marklogic-sesame)

[marklogic-sesame-examples](https://github.com/marklogic/marklogic-sesame/tree/develop/marklogic-sesame-examples)

[Javadocs](http://marklogic.github.io/marklogic-sesame/marklogic-sesame/build/docs/javadoc/)

[JHM perf results](http://marklogic.github.io/marklogic-sesame/marklogic-sesame/build/reports/jhm/human.txt)

### Support

TBD

### License

[Apache License v2.0](LICENSE)
