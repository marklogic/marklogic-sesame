# marklogic-sesame-repository v1.0.2.1

## Introduction

The MarkLogic Sesame API is a [Sesame](http://rdf4j.org/) Repository implementation exposing [MarkLogic](http://www.marklogic.com) semantic [features](http://www.marklogic.com/what-is-marklogic/features/semantics/).

* Transactions: Fully compliant ACID transactions.
* Variable bindings: Set a binding(s) name, language tag, and value.
* Inference (ruleset configuration): Enable inference rulesets .
* Combination of SPARQL with MarkLogic document query: Constrain SPARQL query with MarkLogic queries.
* Optimized pagination of SPARQL result sets: Efficient paging of results.
* Permissions: Manage permissions on triples.

## Before you start

The MarkLogic Sesame API supports [Sesame v2.8.10](http://rdf4j.org/).

### Setup MarkLogic

Ensure MarkLogic 8.0-6 or greater is installed and running. To use marklogic-sesame applications you will need access to a running MarkLogic server.

## Usage

### Quick Start

The MarkLogic Sesame API is available via [Maven Central](http://mvnrepository.com/artifact/com.marklogic/marklogic-sesame/1.0.2.1).

For gradle projects, include the following dependency in your `build.gradle`:

```
dependencies {
    compile group: 'com.marklogic', name: 'marklogic-sesame-repository', version: '1.0.2.1'
}
```

For maven projects, include in your pom.xml:

```
<dependency>
    <groupId>com.marklogic</groupId>
    <artifactId>marklogic-sesame-repository</artifactId>
    <version>1.0.2.1</version>
</dependency>
```

## Build and Use from develop branch

This section describes how to build and test MarkLogic Sesame API from _develop_ branch.

#### Setup MarkLogic Java API Client (optional)

marklogic-sesame-repository depends on [MarkLogic Java API Client v3.0-6](http://mvnrepository.com/artifact/com.marklogic/java-client-api/3.0.6) and should pull down this version from maven central.

To optionally build marklogic-sesame-repository with _develop_ branch version of MarkLogic Java API Client:

1. Clone or download [MarkLogic Java API client](https://github.com/marklogic/java-client-api/tree/develop) _develop_ branch.
2. Build and deploy Java API client to local maven repo.
```
mvn -Dmaven.test.skip=true -Dmaven.javadoc.skip=true deploy
```
Verify that Java API client has been deployed to your local maven repo.
3. edit marklogic-sesame/build.gradle to use that snapshot
```
    compile('com.marklogic:java-client-api:3.0.6') {
        exclude(group: 'org.slf4j')
        exclude(group: 'ch.qos.logback')
    }

```

#### Setup and Test MarkLogic Sesame API

marklogic-sesame-repository depends on MarkLogic v8.0-6 or greater installed and running;

1. Clone or download [marklogic-sesame](https://github.com/marklogic/marklogic-sesame/tree/develop) _develop_ branch.
2. Review [marklogic-sesame/gradle.properties](marklogic-sesame/gradle.properties) which defines test deployment settings.
3. Run gradle target that provisions MarkLogic with everything required (database,REST server,etc.).

```
gradle :marklogic-sesame:mlDeploy
```
You should be able to test MarkLogic Sesame repository by running:
```
gradle :marklogic-sesame:test
```

#### Build and Deploy

Build and deploy a local maven marklogic-sesame-repository snapshot by runnning;

```
gradle :marklogic-sesame:install

```

optionally you can build the jar without running tests.

```
gradle build -x test
```

and copy resultant build/lib/marklogic-sesame-1.0.2.1.jar.

### Examples

The [marklogic-sesame-examples](marklogic-sesame-examples) folder contains a sample project that demonstrates usage of marklogic-sesame-repository.

### Javadocs

Latest [javadocs are here](http://marklogic.github.io/marklogic-sesame/marklogic-sesame/build/docs/javadoc/index.html)

You may generate javadocs by running;

```
gradle :marklogic-sesame:javadoc

```

### Contributing

Everyone is encouraged to [file bug reports](https://github.com/marklogic/marklogic-sesame/labels/Bug), [feature requests](https://github.com/marklogic/marklogic-sesame/labels/enhancement), and [pull requests](https://github.com/marklogic/marklogic-sesame/pulls) through GitHub. This input is critical and will be carefully considered, though we cannot promise a specific resolution or timeframe for any request.

Learn how to [contribute](CONTRIBUTING.md).

### Support

The marklogic-sesame-repository is maintained by MarkLogic Engineering and distributed under the Apache 2.0 license. In addition, MarkLogic provides technical support for release tags of the MarkLogic Sesame API to licensed customers under the terms outlined in the Support Handbook. For more information or to sign up for support, visit [help.marklogic.com](http://help.marklogic.com).

### License

[Apache License v2.0](LICENSE)
