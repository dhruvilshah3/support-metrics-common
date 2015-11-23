# Confluent Proactive Support: Common Library

# Overview

This repository contains common utilities for metrics collection of proactive support.


# Development

## Building

This project uses the standard maven lifecycles such as:

```shell
$ mvn compile
$ mvn test
$ mvn package # creates the jar file
```


## Packaging and releasing

By convention we create release branches of the same name as Kafka _version_ they are integrating with (cf.
`KAFKA_VERSION` in [settings.sh](https://github.com/confluentinc/packaging/blob/master/settings.sh) of
[packaging](https://github.com/confluentinc/packaging/)).

> Note on packaging branches: Unlike other CP projects this project does not require or make use of `archive`, `debian`,
> or `rpm` branches for packaging.

For example, the code of this project for collecting metrics from Apache Kafka version `0.9.0.0` must be maintained
in a shared branch named `0.9.0.0`.  However, this project's maven `<version>` defined [pom.xml](pom.xml) must match
`CONFLUENT_VERSION` in [settings.sh](https://github.com/confluentinc/packaging/blob/master/settings.sh) (like other CP
projects such as [kafka-rest](https://github.com/confluentinc/kafka-rest/)).

```
Branch `0.9.0.0` => code to integrate with Apache Kafka version 0.9.0.0 release
           |
           |
           +-- /pom.xml (top-level)
                   |
                   | defines
                   |
                   V
                <project>
                  ...
                  <version>2.0.0</version>  => for Confluent Platform 2.0.0 release
                  ...
                </project>
```
