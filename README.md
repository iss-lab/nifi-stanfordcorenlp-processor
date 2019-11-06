# Nifi Stanford CoreNLP Processor

This project contains a custom processor for [Apache Nifi](https://nifi.apache.org/) which implements a subset of the functionality in the [Stanford CoreNLP](https://stanfordnlp.github.io/CoreNLP/) toolkit.

## Installation

TODO: Add Instructions.

## Usage

TODO: Add Instructions.

## Build

### Maven

If you have Maven installed, you can build the package with:

```
mvn -DskipTests package
```

### Docker

You can also build using Docker:

```
docker run -v ~/.m2/repository:/usr/share/maven/ref/repository -v $(PWD):/usr/src/app -w /usr/src/app maven:3.5.2-jdk-8-alpine /usr/src/app/docker_entrypoint.sh
```

### Artifacts

After building, your `nar` file will located at `nifi-stanfordcorenlp-nar/target/nifi-stanfordcorenlp-nar-1.0.nar`.

## Test

### Requirements

In order to run the full suite of tests, an external Stanford CoreNLP server is required. Having an external server is optional when actually using this processor.

```
docker run -p 9000:9000 --name coreNLP --rm -i -t isslab/corenlp:2018-10-05
```

### Running the tests

Initiate the tests with:

```
mvn test
```

