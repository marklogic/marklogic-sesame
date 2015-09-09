# marklogic-sesame-repository v1.0.0

## Introduction

markLogic-sesame-repository is a [Sesame](http://rdf4j.org/) Repository implementation exposing [MarkLogic](http://www.marklogic.com/what-is-marklogic/features/semantics/) semantic features.

## Quick Start / Usage

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
gradle :marklogic-sesame:test

```

#### Deployment

Deploy a local maven marklogic-sesame-repository snapshot by runnning;

```
gradle :marklogic-sesame:install

```

### Examples

The project at [marklogic-sesame-examples](marklogic-sesame-examples) contains a sample project that demonstrates usage of marklogic-sesame-repository.

### Javadocs

Latest [javadocs](http://marklogic.github.io/marklogic-sesame/marklogic-sesame/build/docs/javadoc/)

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