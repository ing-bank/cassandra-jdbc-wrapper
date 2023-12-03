# JDBC wrapper of the Java Driver for Apache Cassandra®

[![Apache 2.0 License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0.txt)
![Build Status](https://img.shields.io/github/actions/workflow/status/ing-bank/cassandra-jdbc-wrapper/ci-workflow.yml)
[![Maven Central](https://img.shields.io/maven-central/v/com.ing.data/cassandra-jdbc-wrapper)](https://search.maven.org/search?q=g:com.ing.data%20AND%20cassandra-jdbc-wrapper)
[![Javadoc](https://javadoc.io/badge2/com.ing.data/cassandra-jdbc-wrapper/javadoc.svg)](https://javadoc.io/doc/com.ing.data/cassandra-jdbc-wrapper)
[![Wiki](https://img.shields.io/badge/wiki-documentation-black?logo=github)](https://github.com/ing-bank/cassandra-jdbc-wrapper/wiki)

This is a JDBC wrapper of the Java Driver for Apache Cassandra®, which offers a simple JDBC compliant API to work with 
CQL3.

This JDBC wrapper is based on a fork of the project
[adejanovski/cassandra-jdbc-wrapper](https://github.com/adejanovski/cassandra-jdbc-wrapper/). We would especially like 
to thank its author.

## Features

The JDBC wrapper offers access to most of the core module features:
  - Asynchronous: the driver uses the new CQL binary protocol asynchronous capabilities. Only a relatively low number 
  of connections per nodes needs to be maintained open to achieve good performance.
  - Nodes discovery: the driver automatically discovers and uses all nodes of the C* cluster, including newly 
  bootstrapped ones.
  - Transparent fail-over: if C* nodes fail or become unreachable, the driver automatically and transparently tries 
  other nodes and schedules reconnection to the dead nodes in the background.
  - Convenient schema access: the driver exposes a C* schema in a usable way.

## Getting Started

### Prerequisites

The wrapper uses Java Driver for Apache Cassandra® 4.x. This driver is designed for Apache 
Cassandra® 2.1+ and DataStax Enterprise (5.0+). So, it will throw "unsupported feature" exceptions if used against an 
older version of Cassandra cluster. For more information, please check the 
[compatibility table](https://github.com/ing-bank/cassandra-jdbc-wrapper/wiki/Compatibility).

If you meet issues connecting to the cluster (seeing `NoHostAvailableConnection` exceptions) please check if your
configuration is correct, and you specified a valid local datacenter if you use the default load-balancing policy.

This project requires Java 8 JDK (minimum).

### Installing

This section is mainly aimed at the code contributors.

Clone the repository:
```bash
git clone https://github.com/ing-bank/cassandra-jdbc-wrapper.git
```

To compile and run tests, execute the following Maven command:
```bash
./mvnw clean package
```
Note that running tests requires to install [Docker](https://docs.docker.com/get-docker/) first.

To build a bundled version of the JDBC wrapper, run the following command:
```bash
./mvnw clean package -Pbundle
```

#### Some considerations about running tests

If for some reason the tests using DataStax Enterprise server (`*DseContainerTest`) fail in your local environment, you 
might disable them using the Maven profile `disableDseTests`: 
```bash
./mvnw clean package -PdisableDseTests
```

The test suite also includes integration tests with AstraDB (`DbaasAstraIntegrationTest`). These tests require an
AstraDB token configured in the environment variable `ASTRA_DB_APPLICATION_TOKEN`, otherwise they are skipped.
    
### Integration in Maven projects

You can install the JDBC driver in your application using the following Maven dependency:

```xml
<dependency>
    <groupId>com.ing.data</groupId>
    <artifactId>cassandra-jdbc-wrapper</artifactId>
    <version>${cassandra-jdbc-wrapper.version}</version>
</dependency>
```

### Integration in Gradle projects

You can install the JDBC driver in your application using the following Gradle dependency:

```
implementation 'com.ing.data:cassandra-jdbc-wrapper:${cassandra-jdbc-wrapper.version}'
```

### Other integrations

To use this JDBC wrapper for Apache Cassandra® in database administration tools such as DBeaver Community Edition or 
JetBrains DataGrip, you can have a look to the following links:
* [connecting DBeaver to Cassandra cluster](https://stackoverflow.com/a/77100652/13292108)
* [connecting DataGrip to Cassandra cluster](https://awesome-astra.github.io/docs/pages/data/explore/datagrip/); note 
  this example uses Astra JDBC driver (based on this project), so refer to the "Usage" section below to adapt driver
  class and JDBC URL values.

This JDBC wrapper for Apache Cassandra® is also used to run 
[Liquibase for Cassandra databases](https://github.com/liquibase/liquibase-cassandra) (from Liquibase 4.25.0). To execute Liquibase scripts on
your Cassandra database, specify the following properties in your Liquibase properties file:
```
driver: com.ing.data.cassandra.jdbc.CassandraDriver
url: jdbc:cassandra://<host>:<port>/<keyspaceName>?compliancemode=Liquibase
```
See the [documentation about JDBC connection string](https://github.com/ing-bank/cassandra-jdbc-wrapper/wiki/JDBC-driver-and-connection-string) 
for further details about the allowed parameters in the JDBC URL. 
For further details about Liquibase usage, please check the
[official documentation](https://contribute.liquibase.com/extensions-integrations/directory/database-tutorials/cassandra/).

> _Note:_ The version 4.25.0 of Liquibase extension for Cassandra is currently affected by an issue preventing it
> working correctly. See [this issue](https://github.com/liquibase/liquibase-cassandra/issues/242) for further 
> information.

## Usage

Connect to a Cassandra cluster using the following arguments:
* JDBC driver class: `com.ing.data.cassandra.jdbc.CassandraDriver`
* JDBC URL: `jdbc:cassandra://host1--host2--host3:9042/keyspace?localdatacenter=DC1` 

You can give the driver any number of hosts you want separated by "--". You can optionally specify a port for each host.
If only one port is specified after all the listed hosts, it applies to all hosts. If no port is specified at all, the
default Cassandra port (9042) is used.
They will be used as contact points for the driver to discover the entire cluster.
Give enough hosts taking into account that some nodes may be unavailable upon establishing the JDBC connection.

Find below a basic Java example to establish a connection to a Cassandra database using this driver:
```java
public class HelloCassandra {
    public static void main(final String[] args) {
        // Used driver: com.ing.data.cassandra.cassandra.jdbc.CassandraDriver
        final String url = "jdbc:cassandra://host1--host2--host3:9042/keyspace?localdatacenter=DC1";
        final Connection connection = DriverManager.getConnection(url);
    }
}
```

For further details about configuration and usage of the driver, please refer to the 
[full documentation](https://github.com/ing-bank/cassandra-jdbc-wrapper/wiki/Documentation).

## Contributing

Please read our [contributing guide](https://github.com/ing-bank/cassandra-jdbc-wrapper/blob/master/CONTRIBUTING.md) 
and feel free to improve this library!

## Versioning

We use [SemVer](http://semver.org/) for versioning.

## Authors and contributors

* Maxime Wiewiora - **[@maximevw](https://github.com/maximevw)** 
* Madhavan Sridharan - **[@msmygit](https://github.com/msmygit)**
* Marius Jokubauskas - **[@mjok](https://github.com/mjok)**
* Sualeh Fatehi - **[@sualeh](https://github.com/sualeh)**
* Cedrick Lunven - **[@clun](https://github.com/clun)**
* Stefano Fornari - **[@stefanofornari](https://github.com/stefanofornari)**

And special thanks to the developer of the original project on which is based this one:
* Alexander Dejanovski - **[@adejanovski](https://github.com/adejanovski)**

## Acknowledgments

* [README Template gist](https://gist.github.com/PurpleBooth/84b3d7d6669f77d5a53801a258ed269a) for the redaction of 
what you're reading.
