# Marklogic-Sesame-Repository v1.0.0

_IMPORTANT_ - NO RELEASE HAS BEEN MADE YET

## Introduction

MarkLogic-Sesame is a [Sesame](http://rdf4j.org/) Repository implementation that exposes MarkLogic semantic capabilities.

## Quick Start (draft, for final release version)

_IMPORTANT_ - NO RELEASE TO MAVEN HAS BEEN MADE YET

For gradle projects, include the following:

```
dependencies {
    compile group: 'com.marklogic', name: 'marklogic-sesame-repository', version: '1.0.0'
}
```

To use the API in your maven project, include the following in your pom.xml:

```
<dependency>
    <groupId>com.marklogic</groupId>
    <artifactId>marklogic-sesame-repository</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Support (draft, for final release version)

# Development Notes

These notes will be purged after initial release.

### Building and Testing

_Note: To use this library prior to the release of MarkLogic Server 8.0-4 and Java Client API 3.0.4,
you must have contacted MarkLogic Product Management for access to an early version of the server._


#### Setup Java API Client

1) clone or download Java API client _develop_ branch

```
https://github.com/marklogic/java-client-api/tree/develop
```

2) build and deploy Java API client

```
 mvn -Dmaven.test.skip=true -Dmaven.javadoc.skip=true deploy
 ```

you should verify that Java API client has been deployed to your local maven repo.

#### Setup Marklogic

Ensure MarkLogic (Nightly) is installed and running;

1) run gradle target that provisions everything needed for MarkLogic

```
gradle mlDeploy
```

alternately you may provision manually

```
//setup Documents database
curl -v -X PUT --anyauth --user admin:admin --header "Content-Type: application/json" -d'{"collection-lexicon":true,"triple-index":true}' "http://localhost:8002/manage/v2/databases/Documents/properties"

//setup server
curl -v -X POST --anyauth --user admin:admin --header "Content-Type: application/json" -d@test/resources/setup/rest.json "http://localhost:8002/manage/v2/servers?server-type=http&group-id=Default"

//load data triples
curl --anyauth --user admin:admin -i -X POST -d@test/resources/setup/test.owl -H "Content-type: application/rdf+xml" http://localhost:8200/v1/graphs?graph=my-graph
```

#### Setup  MarkLogic Sesame Repository

1) clone or download marklogic-sesame _develop_ branch

```
https://github.com/marklogic/marklogic-sesame/tree/develop
```

2) build MarkLogic Sesame repository

```
gradle test

```

will build and run unit tests.


### Usage

To use in your own code, deploy into local maven repo or copy snapshot jars from /build directory.

```
gradle deploy

```

The following examples demonstrate idiomatic Sesame usage of the MarkLogic Sesame Repository.

(TBD - provide /samples dir)


#### query examples
```
Repository mr = new MarkLogicRepository();

mr.shutDown();
mr.initialize();

RepositoryConnection con = mr.getConnection();

Assert.assertTrue( con != null );
String queryString = "select ?s ?p ?o { ?s ?p ?o } limit 2 ";
TupleQuery tupleQuery =  con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
TupleQueryResult results = tupleQuery.evaluate();

try {
results.hasNext();
BindingSet bindingSet = results.next();

Value sV = bindingSet.getValue("s");
Value pV = bindingSet.getValue("p");
Value oV = bindingSet.getValue("o");

Assert.assertEquals("http://example.org/marklogic/people/Jack_Smith",sV.stringValue());
Assert.assertEquals("http://example.org/marklogic/predicate/livesIn",pV.stringValue());
Assert.assertEquals("Glasgow", oV.stringValue());

results.hasNext();
BindingSet bindingSet1 = results.next();

Value sV1 = bindingSet1.getValue("s");
Value pV1 = bindingSet1.getValue("p");
Value oV1 = bindingSet1.getValue("o");

Assert.assertEquals("http://example.org/marklogic/people/Jane_Smith",sV1.stringValue());
Assert.assertEquals("http://example.org/marklogic/predicate/livesIn",pV1.stringValue());
Assert.assertEquals("London", oV1.stringValue());
}
finally {
results.close();
}
con.close();
```

#### boolean examples
```
```

#### graph examples
```
```

#### update examples
```
```

#### add/remove examples
```
```

#### get/clear graph examples
```
```

#### transactions examples
```
```

#### get/export statements examples
```
```

#### pagination examples
```
```