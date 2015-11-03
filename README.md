# marklogic-sesame v1.0.0

## Introduction

The MarkLogic Sesame API is a [Sesame](http://rdf4j.org/) Repository implementation exposing [MarkLogic](http://www.marklogic.com) semantic [features](http://www.marklogic.com/what-is-marklogic/features/semantics/).

* Transactions
* Variable bindings
* Inference (ruleset configuration)
* Combination of SPARQL with MarkLogic document query
* Optimized pagination of SPARQL result sets
* Permissions

## Before you start

### Setup MarkLogic

Ensure MarkLogic 8.0-4 is installed and running. To use marklogic-sesame applications you will need access to a running MarkLogic server.

## Usage

### Quick Start

_IMPORTANT_ - NO RELEASE TO MAVEN CENTRAL YET, YOU MUST BUILD SOFTWARE

This API will be distributed on Maven Central.

For gradle projects, include following dependency in `build.gradle`:

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

## Build and Use from develop branch

This section describes how to build and test MarkLogic Sesame API from _develop_ branch.

#### Setup Java API Client

marklogic-sesame-repository depends on _develop_ branch version of Java API Client:

1. Clone or download [MarkLogic Java API client](https://github.com/marklogic/java-client-api/tree/develop) _develop_ branch.
2. Build and deploy Java API client to local maven repo.
```
mvn -Dmaven.test.skip=true -Dmaven.javadoc.skip=true deploy
```
Verify that Java API client has been deployed to your local maven repo.

#### Setup and Test MarkLogic Sesame API

marklogic-sesame-repository depends on MarkLogic v8.0-4 or greater installed and running;

1. Clone or download [marklogic-sesame](https://github.com/marklogic/marklogic-sesame/tree/develop) _develop_ branch.
2. Review [marklogic-sesame/gradle.properties](marklogic-sesame/gradle.properties) which defines test deployment settings.
3. Run gradle target that provisions MarkLogic with everything required (database,REST server,etc.).

```
gradle :marklogic-sesame:mlDeploy
```
You should now be able to now test MarkLogic Sesame repository by running:
```
gradle :marklogic-sesame:test
```

#### Deployment

Deploy a local maven marklogic-sesame-repository snapshot by runnning;

```
gradle :marklogic-sesame:install

```

### Examples

The [marklogic-sesame-examples](marklogic-sesame-examples) folder contains a sample project that demonstrates usage of marklogic-sesame-repository.

### Javadocs

Latest [javadocs are here](http://marklogic.github.io/marklogic-sesame/marklogic-sesame/build/docs/javadoc/index.html)

You can generate javadocs by running;

```
gradle :marklogic-sesame:javadoc

```

### Contributing

Everyone is encouraged to [file bug reports](https://github.com/marklogic/marklogic-sesame/labels/Bug), [feature requests](https://github.com/marklogic/marklogic-sesame/labels/enhancement), and [pull requests](https://github.com/marklogic/marklogic-sesame/pulls) through GitHub. This input is critical and will be carefully considered, though we cannot promise a specific resolution or timeframe for any request.

Learn how to [contribute](CONTRIBUTING.md).

### Support

The marklogic-sesame-repository is maintained by MarkLogic Engineering and distributed under the Apache 2.0 license. In addition, MarkLogic provides technical support for release tags of the Java Client API to licensed customers under the terms outlined in the Support Handbook. For more information or to sign up for support, visit [help.marklogic.com](http://help.marklogic.com).

### License

[Apache License v2.0](LICENSE)
