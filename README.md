# marklogic-sesame-repository v1.0.0

_IMPORTANT_ - NO RELEASE HAS BEEN MADE YET

## Introduction

markLogic-sesame-repository is a [Sesame](http://rdf4j.org/) Repository implementation that exposes MarkLogic semantic capabilities.

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

### Build and Test

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

#### Setup and Test MarkLogic Sesame Repository

1) clone or download marklogic-sesame _develop_ branch

```
https://github.com/marklogic/marklogic-sesame/tree/develop
```

2) test MarkLogic Sesame repository

```
gradle test

```

will load test data, build and run unit tests.

2) optionally load test data

```
gradle loadTestData
```

to be clear, you are not required to run this task as it is run automatically before testing.

(note- this gradle requires Curl to be installed and available on your system's PATH)

alternately you may load directly via curl
```
//load data triples
curl --anyauth --user admin:admin -i -X POST -d@src/test/resources/setup/test.owl -H "Content-type: application/rdf+xml" http://localhost:8200/v1/graphs?graph=my-graph
```

### Usage

To use in your own software, deploy into local maven repo or copy snapshot jars from /build directory.

(TBD - not impl yet)

```
gradle install

```

### Links

[Repo](https://github.com/marklogic/marklogic-sesame)
[Javadocs] (http://marklogic.github.io/marklogic-sesame/build/docs/javadoc/)
[JHM perf results](http://marklogic.github.io/marklogic-sesame/build/reports/jhm/human.txt)

#### Examples

The marklogic-sesame-examples subproject demonstrate idiomatic Sesame usage of the MarkLogic Sesame Repository.

#### query


using tupleQuery.evalute();
```
Repository mr = new MarkLogicRepository();

mr.shutDown();
mr.initialize();

RepositoryConnection con = mr.getConnection();

String queryString = "select ?s ?p ?o { ?s ?p ?o } limit 2 ";
TupleQuery tupleQuery =  con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
tupleQuery.setIncludeInferred(true); // enable default ruleset
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

#### boolean

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

#### graph 

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

#### update 

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

#### add/remove 

add statements with file
```
File inputFile = new File("src/test/resources/testdata/default-graph-1.ttl");
String baseURI = "http://example.org/example1/";
Resource context1 = conn.getValueFactory().createURI("http://marklogic.com/test/context1");
Resource context2 = conn.getValueFactory().createURI("http://marklogic.com/test/context2");
conn.add(inputFile, baseURI, RDFFormat.TURTLE,context1,context2);
conn.clear(context1, context2);
```

add statements with InputStream
```
File inputFile = new File("src/test/resources/testdata/default-graph-1.ttl");
FileInputStream is = new FileInputStream(inputFile);
String baseURI = "http://example.org/example1/";
Resource context3 = conn.getValueFactory().createURI("http://marklogic.com/test/context3");
Resource context4 = conn.getValueFactory().createURI("http://marklogic.com/test/context4");
conn.add(is, baseURI, RDFFormat.TURTLE,context3,context4);
conn.clear(context3, context4);
```

#### get/clear graph 

```
  public void testTransaction3() throws Exception{
        Resource context1 = conn.getValueFactory().createURI("http://marklogic.com/test/my-graph");
        conn.begin();
        conn.clear(context1);
        conn.rollback();
    }
```

#### is active / is open 

close connection, check if open
```
Assert.assertEquals(true, conn.isOpen());
conn.close();
Assert.assertEquals(false, conn.isOpen());
```

start transaction see if connection is active
```
Assert.assertEquals(false, conn.isActive());
conn.begin();
Assert.assertEquals(true, conn.isActive());
```

#### transactions

begin transaction, add statement, then rollback
```
File inputFile = new File("src/test/resources/testdata/default-graph-1.ttl");
String baseURI = "http://example.org/example1/";
Resource context1 = conn.getValueFactory().createURI("http://marklogic.com/test/transactiontest");
conn.begin();
conn.add(inputFile, baseURI, RDFFormat.TURTLE, context1);
conn.rollback();
```

begin transaction, add statement and commit
```
File inputFile = new File("src/test/resources/testdata/default-graph-1.ttl");
String baseURI = "http://example.org/example1/";
Resource context1 = conn.getValueFactory().createURI("http://marklogic.com/test/transactiontest");
conn.begin();
conn.add(inputFile, baseURI, RDFFormat.TURTLE, context1);
conn.commit();
conn.clear(context1);
```

begin transaction, clear context (graph) and rollback
```
Resource context1 = conn.getValueFactory().createURI("http://marklogic.com/test/my-graph");
conn.begin();
conn.clear(context1);
conn.rollback();
```

#### get/export statements 
```
```

#### pagination

return 1 triple, starting from the third triple;
```
String queryString = "select ?s ?p ?o { ?s ?p ?o } limit 100 ";
MarkLogicTupleQuery tupleQuery = (MarkLogicTupleQuery) conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
TupleQueryResult results = tupleQuery.evaluate(3, 1);

Assert.assertEquals(results.getBindingNames().get(0), "s");
Assert.assertEquals(results.getBindingNames().get(1), "p");
Assert.assertEquals(results.getBindingNames().get(2), "o");

BindingSet bindingSet = results.next();

Value sV = bindingSet.getValue("s");
Value pV = bindingSet.getValue("p");
Value oV = bindingSet.getValue("o");

Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#AntiochGeodata", sV.stringValue());
Assert.assertEquals("http://semanticbible.org/ns/2006/NTNames#altitude", pV.stringValue());
Assert.assertEquals("0", oV.stringValue());
```

#### other

### FAQ

* How to contribute ? Pull requests should be made against _develop_ branch.

### Support

TBD

### License

TBD
