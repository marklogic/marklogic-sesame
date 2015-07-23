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

IMPORTANT- These notes will be purged after initial release.

Latest _develop_ branch Javadocs and test results are accessible here

[http://xquery.github.io/marklogic-sesame/](http://xquery.github.io/marklogic-sesame/)

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
//setup marklogic-sesame-test-content database
curl -v -X PUT --anyauth --user admin:admin --header "Content-Type: application/json" -d'{"collection-lexicon":true,"triple-index":true}' "http://localhost:8002/manage/v2/databases/marklogic-sesame-test-content/properties"

//setup server
curl -v -X POST --anyauth --user admin:admin --header "Content-Type: application/json" -d@src/test/resources/setup/rest.json "http://localhost:8002/manage/v2/servers?server-type=http&group-id=Default"

```

2) manually load test data

```
gradle loadTestData
```

you are not required to run this task as it is run automatically before testing.

(note- this gradle task requires Curl to be installed and available on your system's PATH)

alternately you may load manually
```
//load data triples
curl --anyauth --user admin:admin -i -X POST -d@src/test/resources/setup/test.owl -H "Content-type: application/rdf+xml" http://localhost:8200/v1/graphs?graph=my-graph
```

#### Setup and Test MarkLogic Sesame Repository

1) clone or download marklogic-sesame _develop_ branch

```
https://github.com/marklogic/marklogic-sesame/tree/develop
```

2) test MarkLogic Sesame repository

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

using tupleQuery.evalute();
```
Repository mr = new MarkLogicRepository();

mr.shutDown();
mr.initialize();

RepositoryConnection con = mr.getConnection();

String queryString = "select ?s ?p ?o { ?s ?p ?o } limit 2 ";
TupleQuery tupleQuery =  con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
TupleQueryResult results = tupleQuery.evaluate();

try {
while(results.hasNext()){;
BindingSet bindingSet = results.next();
Value sV = bindingSet.getValue("s");
Value pV = bindingSet.getValue("p");
Value oV = bindingSet.getValue("o");
}
finally {
results.close();
}
con.close();
```

using tupleQuery.evaluate(new TupleQueryResultHandler());
```
Repository mr = new MarkLogicRepository();
mr.shutDown();
mr.initialize();
RepositoryConnection con = mr.getConnection();

String queryString = "select ?s ?p ?o { ?s ?p ?o } limit 10";
TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);

tupleQuery.evaluate(new TupleQueryResultHandler() {
    @Override
    public void startQueryResult(List<String> bindingNames) {
        System.out.println(bindingNames.get(0)); // subject
        System.out.println(bindingNames.get(1)); // predicate
        System.out.println(bindingNames.get(2)); // object
    }

    @Override
    public void handleSolution(BindingSet bindingSet) {
        Assert.assertEquals(bindingSet.getBinding("o").getValue().stringValue(), "0");
    }

    @Override
    public void endQueryResult() {
    }

    @Override
    public void handleBoolean(boolean arg0)
            throws QueryResultHandlerException {
    }

    @Override
    public void handleLinks(List<String> arg0)
            throws QueryResultHandlerException {
    }
});
tupleQuery.evaluate();
```

using prepareQuery;
```
String queryString = "select ?s ?p ?o { ?s ?p ?o } limit 1 ";
Query q = conn.prepareQuery(QueryLanguage.SPARQL, queryString);

if (q instanceof TupleQuery) {
    TupleQueryResult result = ((TupleQuery)q).evaluate();
    while (result.hasNext()) {
        BindingSet tuple = result.next();
        Assert.assertEquals("s",tuple.getBinding("s").getName());
        Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#AlexandriaGeodata",tuple.getBinding("s").getValue().stringValue());
    }
}
```

example with bindings;
```
Repository mr = new MarkLogicRepository();
mr.shutDown();
mr.initialize();
RepositoryConnection con = mr.getConnection();

String queryString = "select ?s ?p ?o { ?s ?p ?o . filter (?s = ?b) filter (?p = ?c) }";
TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);

tupleQuery.setBinding("b", ValueFactoryImpl.getInstance().createURI("http://semanticbible.org/ns/2006/NTNames#Jim"));
tupleQuery.setBinding("c", ValueFactoryImpl.getInstance().createURI("http://semanticbible.org/ns/2006/NTNames#parentOf"));

tupleQuery.removeBinding("c");

tupleQuery.clearBindings();

tupleQuery.setBinding("b", ValueFactoryImpl.getInstance().createURI("http://semanticbible.org/ns/2006/NTNames#Jotham"));
tupleQuery.setBinding("c", ValueFactoryImpl.getInstance().createURI("http://semanticbible.org/ns/2006/NTNames#parentOf"));

TupleQueryResult results = tupleQuery.evaluate();
```

#### boolean examples

simple boolean example;
```
Repository mr = new MarkLogicRepository();
mr.shutDown();
mr.initialize();
RepositoryConnection con = mr.getConnection();

String queryString = "ASK { <http://semanticbible.org/ns/2006/NTNames#Shelah1> ?p ?o}";
BooleanQuery booleanQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, queryString);
boolean results = booleanQuery.evaluate();
Assert.assertEquals(false,results); // should fail
queryString = "ASK { <http://semanticbible.org/ns/2006/NTNames#Shelah> ?p ?o}";
booleanQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, queryString);
results = booleanQuery.evaluate(); // should be true
Assert.assertEquals(true,results);
```

#### graph examples

CONSTRUCT example;
```
Repository mr = new MarkLogicRepository();
mr.shutDown();
mr.initialize();
RepositoryConnection con = mr.getConnection();

String queryString = "PREFIX nn: <http://semanticbible.org/ns/2006/NTNames#>\n" +
        "PREFIX test: <http://marklogic.com#test>\n" +
        "\n" +
        "construct { ?s  test:test \"0\"} where  {?s nn:childOf nn:Eve . }";
GraphQuery graphQuery = conn.prepareGraphQuery(QueryLanguage.SPARQL, queryString);
GraphQueryResult results = graphQuery.evaluate();
Statement st1 = results.next();
Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#Abel",st1.getSubject().stringValue());
Statement st2 = results.next();
Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#Cain", st2.getSubject().stringValue());
```

DESCRIBE example;
```
Repository mr = new MarkLogicRepository();
mr.shutDown();
mr.initialize();
RepositoryConnection con = mr.getConnection();

String queryString = "DESCRIBE <http://semanticbible.org/ns/2006/NTNames#Shelah>";
GraphQuery graphQuery = conn.prepareGraphQuery(QueryLanguage.SPARQL, queryString);
GraphQueryResult results = graphQuery.evaluate();
Statement st1 = results.next();
Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#Shelah",st1.getSubject().stringValue());
Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#childOf",st1.getPredicate().stringValue());
Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#CainanSonOfArphaxad",st1.getObject().stringValue());

```

#### update examples

INSERT update example;
```
Repository mr = new MarkLogicRepository();
mr.shutDown();
mr.initialize();
RepositoryConnection con = mr.getConnection();

// INSERT
String defGraphQuery = "INSERT DATA { GRAPH <g27> { <http://marklogic.com/test> <pp1> <oo1> } }";
String checkQuery = "ASK WHERE { <http://marklogic.com/test> <pp1> <oo1> }";
Update updateQuery = conn.prepareUpdate(QueryLanguage.SPARQL, defGraphQuery);
updateQuery.execute();

//check if the update worked
BooleanQuery booleanQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL, checkQuery);
boolean results = booleanQuery.evaluate();
Assert.assertEquals(true, results);
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
