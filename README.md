# Cassandra JDBC wrapper for the Datastax Java Driver

[![Apache 2.0 License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0.txt)
![Build Status](https://img.shields.io/github/workflow/status/ing-bank/cassandra-jdbc-wrapper/CI%20Workflow)
[![Maven Central](https://img.shields.io/maven-central/v/com.ing.data/cassandra-jdbc-wrapper)](https://search.maven.org/search?q=g:com.ing.data%20AND%20cassandra-jdbc-wrapper)
[![Javadoc](https://javadoc.io/badge2/com.ing.data/cassandra-jdbc-wrapper/javadoc.svg)](https://javadoc.io/doc/com.ing.data/cassandra-jdbc-wrapper)

This is a JDBC wrapper of the DataStax Java Driver for Apache Cassandra (C*), which offers a simple JDBC compliant 
API to work with CQL3.

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

The wrapper uses DataStax Java driver for Apache Cassandra(R) 4.4.0 or greater. This driver is designed for Apache 
Cassandra(R) 2.1+ and DataStax Enterprise (5.0+). So, it will throw "unsupported feature" exceptions if used against an 
older version of Cassandra cluster. For more information, please check the 
[compatibility matrix](https://docs.datastax.com/en/driver-matrix/doc/driver_matrix/javaDrivers.html) and read the 
[driver documentation](https://docs.datastax.com/en/developer/java-driver/latest/).

If you are having issues connecting to the cluster (seeing `NoHostAvailableConnection` exceptions) please check the 
[connection requirements](https://github.com/datastax/java-driver/wiki/Connection-requirements).

This project requires Java 8 JDK (minimum).

### Installing

Clone the repository:
```bash
git clone https://github.com/ing-bank/cassandra-jdbc-wrapper.git
```

To compile and run tests, execute the following Maven command:
```bash
mvn clean package
```
    
### Integration in Maven projects

You can install it in your application using the following Maven dependency:

```xml
<dependency>
    <groupId>com.ing.data</groupId>
    <artifactId>cassandra-jdbc-wrapper</artifactId>
    <version>${cassandra-jdbc-wrapper.version}</version>
</dependency>
```

## Usage

Connect to a Cassandra cluster using the following arguments:
* JDBC driver class: `com.ing.data.cassandra.jdbc.CassandraDriver`
* JDBC URL: `jdbc:cassandra://host1--host2--host3:9042/keyspace?localdatacenter=DC1` (to connect to a DBaaS cluster, 
  please read the section "[Connecting to DBaaS](#connecting-to-dbaas)"; to use a configuration file, please read the 
  section "[Using a configuration file](#using-a-configuration-file)")

You can give the driver any number of hosts you want separated by "--".
They will be used as contact points for the driver to discover the entire cluster.
Give enough hosts taking into account that some nodes may be unavailable upon establishing the JDBC connection.

You also have to specify the name of the local data center to use when the default load balancing policy is defined 
(see paragraph below about load balancing policies) and no configuration file is specified. 

Statements and prepared statements can be executed as with any JDBC driver, but note that queries must be expressed in 
CQL3.

Java example:
```java
public class HelloCassandra {
    public static void main(final String[] args) {
        // Used driver: com.ing.data.cassandra.cassandra.jdbc.CassandraDriver
        final String url = "jdbc:cassandra://host1--host2--host3:9042/keyspace?localdatacenter=DC1";
        final Connection connection = DriverManager.getConnection(url);
    }
}
```

If you want to use a pre-existing session, you can directly build a new `CassandraConnection` with the constructor
`CassandraConnection(Session, String, ConsistencyLevel, debugMode, OptionSet)`. For example:

```java
public class HelloCassandraWithSession {
  public static void main(final String[] args) {
    final CqlSession session = CqlSession.builder()
      .addContactPoint(new InetSocketAddress("localhost", 9042))
      .withLocalDatacenter("DC1")
      .build();
    final Connection connection = new CassandraConnection(session, "keyspace", ConsistencyLevel.ALL, false, new Default());
  }
}
```

### Using a configuration file

If you want to use a 
[configuration file](https://docs.datastax.com/en/developer/java-driver/latest/manual/core/configuration/reference/),
specify the location with the query parameter `configfile`. When this parameter is specified, any other configuration
mentioned into the JDBC URL, except the contact points and the keyspace, will be ignored and overridden by the values
defined in the configuration file. For example:
```
jdbc:cassandra://host1--host2--host3:9042/keyspace?configfile=/path/to/configuration/application.conf
```

### Specifying timeout for queries

By default, the timeout for queries is 2 seconds (see the property `basic.request.timeout` in the
[Configuration reference](https://docs.datastax.com/en/developer/java-driver/latest/manual/core/configuration/reference)
page).

However, if you want to use a non-default timeout value, add a `requesttimeout` argument to the JDBC URL and give the 
expected duration in milliseconds. For example, to define a timeout of 5 seconds:
```
jdbc:cassandra://host1--host2--host3:9042/keyspace?requesttimeout=5000
```

### Specifying load balancing policies

In versions 4+ of DataStax Java driver for Apache Cassandra(R), the load balancing is defined with 
`DefaultLoadBalancingPolicy` by default (see 
[Load balancing](https://docs.datastax.com/en/developer/java-driver/latest/manual/core/load_balancing/) documentation).

The `DefaultLoadBalancingPolicy` requires to specify the local datacenter to use, so don't forget to add a 
`localdatacenter` argument to the JDBC URL:
```
jdbc:cassandra://host1--host2--host3:9042/keyspace?localdatacenter=DC1
```

However, if you want to use a custom policy, add a `loadbalancing` argument to the JDBC URL and give the full package
of the policy's class:
```
jdbc:cassandra://host1--host2--host3:9042/keyspace?loadbalancing=com.company.package.CustomPolicy
```

The custom policy must implement `LoadBalancingPolicy` interface.

### Specifying retry policies

In versions 4+ of DataStax Java driver for Apache Cassandra(R), the retry policy is defined with `DefaultRetryPolicy` by
default (see [Retries](https://docs.datastax.com/en/developer/java-driver/latest/manual/core/retries/) documentation).

However, if you want to use a custom policy, add a `retry` argument to the JDBC URL and give the full package of the 
policy's class:
```
jdbc:cassandra://host1--host2--host3:9042/keyspace?retry=com.company.package.CustomPolicy
```

The custom policy must implement `RetryPolicy` interface.

### Specifying reconnection policies

In versions 4+ of DataStax Java driver for Apache Cassandra(R), the reconnection policy is defined with 
`ExponentialReconnectionPolicy` by default (see 
[Reconnection](https://docs.datastax.com/en/developer/java-driver/latest/manual/core/reconnection/) documentation).

If you want to define a custom base delay (in seconds, by default 1 second) and a custom max delay (in seconds, by
default 60 seconds), specify the arguments as following:
```
jdbc:cassandra://host1--host2--host3:9042/keyspace?reconnection=ExponentialReconnectionPolicy((long)2,(long)120)
```

The first argument is the base delay, the second one is the max delay.

If you want to use the `ConstantReconnectionPolicy` as policy, add a `reconnection` argument to the JDBC URL and give 
the policy's class:
```
jdbc:cassandra://host1--host2--host3:9042/keyspace?reconnection=ConstantReconnectionPolicy()
```

If you want to define a custom base delay (in seconds, by default 1 second), specify an argument as following:
```
jdbc:cassandra://host1--host2--host3:9042/keyspace?reconnection=ConstantReconnectionPolicy((long)10)
```

If you want to use a custom policy, add a `reconnection` argument to the JDBC URL and give the full package of the 
policy's class:
```
jdbc:cassandra://host1--host2--host3:9042/keyspace?reconnection=com.company.package.CustomPolicy()
```

Make sure you cast the policy's arguments appropriately.

### Specifying consistency level

Consistency level can be specified per connection (not per query). To do so, add a `consistency` argument to the JDBC 
URL:
```
jdbc:cassandra://host1--host2--host3:9042/keyspace?consistency=LOCAL_QUORUM
```

Consistency level defaults to `LOCAL_ONE` as defined in the configuration reference if not specified (see 
[Consistency levels](https://docs.datastax.com/en/dse/6.8/dse-arch/datastax_enterprise/dbInternals/dbIntConfigConsistency.html) 
documentation for further details about the valid values for this argument).

### Secure connection with SSL

In order to secure the traffic between the driver and the Cassandra cluster, add at least one of these arguments to the
JDBC URL:
* `enablessl=true`: the `DefaultSslEngineFactory` will be used with the JSSE system properties defined for your 
  application
* `sslenginefactory`: specify the fully-qualified name of a class with a no-args constructor implementing 
  `SslEngineFactory` interface, for instance:
  ```
  jdbc:cassandra://host1--host2--host3:9042/keyspace?sslenginefactory=com.company.package.CustomSslEngineFactory
  ```
  
The argument `sslEngineFactory` will be ignored if the argument `enableSsl` is `false` but SSL will be enabled if
`sslEngineFactory` is present even if `enableSsl` is missing.

For further information about custom implementations of `SslEngineFactory`, see 
[SSL](https://docs.datastax.com/en/developer/java-driver/latest/manual/core/ssl/) documentation.

### Connecting to DBaaS

In order to connect to the cloud [Cassandra DBaaS](www.datastax.com/astra) cluster, one would need to specify:
* `secureconnectbundle`: the fully qualified path of the cloud secure connect bundle file
* `keyspace`: the keyspace to connect to
* `user`: the username
* `password`: the password

For example, using the dedicated protocol `jdbc:cassandra:dbaas:`:
```
jdbc:cassandra:dbaas:///keyspace?consistency=LOCAL_QUORUM&user=user1&password=password1&secureconnectbundle=/path/to/location/secure-connect-bundle-cluster.zip
```

*Note*: whatever the host(s) given here will be ignored and will be fetched from the cloud secure connect bundle.

For further information about connecting to DBaaS, see [cloud documentation](https://docs.datastax.com/en/developer/java-driver/latest/manual/cloud/).

### Compliance modes

For some specific usages, the default behaviour of some JDBC implementations has to be modified. That's why you can
use the argument `compliancemode` in the JDBC URL to cutomize the behaviour of some methods.

The values currently allowed for this argument are:
* `Default`: mode activated by default if not specified in the JDBC URL. It implements the methods detailed below as 
  defined in the JDBC specification (according to the Cassandra driver capabilities). 
* `Liquibase`: compliance mode for a usage of the JDBC driver with Liquibase. 

Here are the behaviours defined by the compliance modes listed above:

| Method | Default mode | Liquibase mode |
|---|---|---|
| `CassandraConnection.getCatalog()` | returns the result of the query`SELECT cluster_name FROM system.local` or `null` if not available | returns `null` |
| `CassandraStatement.executeUpdate(String)` | returns 0 | returns -1 |

### Using simple statements

To issue a simple select and get data from it:
```java
public class HelloCassandra {
    public void selectValuesFromCassandra(final Connection connection) {
        final Statement statement = connection.createStatement();
        final ResultSet result = statement.executeQuery(
            "SELECT bValue, iValue FROM test_table WHERE keyname = 'key0';"
        );
        while (result.next()) {
            System.out.println("bValue = " + result.getBoolean("bValue"));
            System.out.println("iValue = " + result.getInt("iValue"));
        };
    }
}
```

### Using Prepared statements

Considering the following table:
```cql
CREATE TABLE table1 (
    bigint_col bigint PRIMARY KEY, 
    ascii_col ascii, 
    blob_col blob, 
    boolean_col boolean,
    decimal_col decimal, 
    double_col double, 
    float_col float, 
    inet_col inet, 
    int_col int,
    smallint_col smallint,
    text_col text, 
    timestamp_col timestamp, 
    time_col time, 
    date_col date, 
    tinyint_col tinyint,
    duration_col duration,
    uuid_col uuid,
    timeuuid_col timeuuid, 
    varchar_col varchar, 
    varint_col varint,
    string_set_col set<text>,
    string_list_col list<text>, 
    string_map_col map<text, text>
);
```

To insert a record into "table1" using a prepared statement:

```java
import com.datastax.oss.driver.api.core.data.CqlDuration;

import java.io.ByteArrayInputStream;
import java.sql.Date;

public class HelloCassandra {
  public void insertRecordToCassandraTable(final Connection connection) {
    final Statement statement = connection.createStatement();
    final String insertCql = "INSERT INTO table1 (bigint_col, ascii_col, blob_col, boolean_col, decimal_col, "
      + "double_col, float_col, inet_col, int_col, smallint_col, text_col, timestamp_col, time_col, date_col, "
      + "tinyint_col, duration_col, uuid_col, timeuuid_col, varchar_col, varint_col, string_set_col, "
      + "string_list_col, string_map_col) "
      + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now(), ?, ?, ?, ?, ?);";
    final PreparedStatement preparedStatement = connection.prepareStatement(insert);
    preparedStatement.setObject(1, 1L);                             // bigint
    preparedStatement.setObject(2, "test");                         // ascii
    final ByteArrayInputStream baInputStream = new ByteArrayInputStream("test".getBytes(StandardCharsets.UTF_8));
    preparedStatement.setObject(3, baInputStream);                  // blob
    preparedStatement.setObject(4, true);                           // boolean
    preparedStatement.setObject(5, new BigDecimal(5.1));            // decimal
    preparedStatement.setObject(6, (double) 5.1);                   // double
    preparedStatement.setObject(7, (float) 5.1);                    // float
    final InetAddress inet = InetAddress.getLocalHost();
    preparedStatement.setObject(8, inet);                           // inet
    preparedStatement.setObject(9, 1);                              // int
    preparedStatement.setObject(10, 1);                             // smallint
    preparedStatement.setObject(11, "test");                        // text
    final long now = OffsetDateTime.now().toEpochSecond() * 1_000;
    preparedStatement.setObject(12, new Timestamp(now));            // timestamp
    preparedStatement.setObject(13, new Time(now));                 // time
    preparedStatement.setObject(14, new Date(now));                 // date
    preparedStatement.setObject(15, 1);                             // tinyint
    final CqlDuration duration = CqlDuration.from("12h30m15s");
    preparedStatement.setObject(16, duration);                      // duration
    final UUID uuid = UUID.randomUUID();
    preparedStatement.setObject(17, uuid);                          // uuid
    preparedStatement.setObject(18, "test");                        // varchar
    preparedStatement.setObject(19, 1);                             // varint
    final HashSet<String> sampleSet = new HashSet<String>();
    sampleSet.add("test1");
    sampleSet.add("test2");
    preparedStatement.setObject(20, sampleSet);                     // set
    ArrayList<String> sampleList = new ArrayList<String>();
    sampleList.add("test1");
    sampleList.add("test2");
    preparedStatement.setObject(21, sampleList);                    // list
    HashMap<String, String> sampleMap = new HashMap<String, String>();
    sampleMap.put("1", "test1");
    sampleMap.put("2", "test2");
    preparedStatement.setObject(22, sampleMap);                     // map
    // Execute the prepare statement.
    preparedStatement.execute();
  }
}
```

### Using Async Queries

#### Insert/update

There are 2 ways to insert/update data using asynchronous queries. The first is to use JDBC batches (we're not talking 
about Cassandra atomic batches here).

With simple statements:
```java
public class HelloCassandra {
    public void insertUsingJdbcBatches(final Connection connection) {
        final Statement statement = connection.createStatement();
        
        for(int i = 0; i < 10; i++){
            statement.addBatch("INSERT INTO testCollection (keyValue, lValue) VALUES (" + i + ", [1, 3, 12345])");
        }
        
        final int[] counts = statement.executeBatch();
        statement.close();
    }
}
```

With prepared statements:
```java
public class HelloCassandra {
    public void insertUsingJdbcBatches(final Connection connection) {
        final PreparedStatement statement = connection.prepareStatement(
            "INSERT INTO testCollection (keyValue, lValue) VALUES (?, ?)"
        );
        
        for(int i = 0; i < 10; i++){
            statement.setInt(1, i);
            statement.setObject(2, Arrays.asList(1, 3, 12345));
            statement.addBatch();
        }
        
        final int[] counts = statement.executeBatch();
        statement.close();
    }
}
```

The second one is to put all the queries in a single CQL statement, each ended with a semicolon (`;`):
```java
public class HelloCassandra {
    public void insertUsingSingleCqlStatement(final Connection connection) {
        final Statement statement = connection.createStatement();

        final StringBuilder queryBuilder = new StringBuilder();
        for(int i = 0; i < 10; i++){
            queryBuilder.append("INSERT INTO testCollection (keyValue, lValue) VALUES(")
                        .append(i)
                        .append(", [1, 3, 12345]);");
        }
              
        statement.execute(queryBuilder.toString());
        statement.close();
    }
}
```

#### Select

As JDBC batches do not support returning result sets, there is only one way to send asynchronous select queries through 
the JDBC driver:
```java
public class HelloCassandra {
    public void multipleSelectQueries(final Connection connection) {
        final StringBuilder queries = new StringBuilder();
        for(int i = 0; i < 10; i++){
            queries.append("SELECT * FROM testCollection where keyValue = ").append(i).append(";");
        }

        // Send all the select queries at once.
        final Statement statement = connection.createStatement();
        final ResultSet result = statement.executeQuery(queries.toString());

        // Get all the results from all the select queries in a single result set.
        final ArrayList<Integer> ids = new ArrayList<>();
        while (result.next()){
            ids.add(result.getInt("keyValue"));
        }
    }
}
```

Make sure you send select queries that return the exact same columns or you might get pretty unpredictable results.

### Working with Tuples and UDTs

To create a new `Tuple` object in Java (see 
[Tuple](https://docs.datastax.com/en/developer/java-driver/latest/manual/core/tuples/) documentation), use the 
`TupleType.of().newValue()` method.
Note that the UDT ([User-Defined Types](https://docs.datastax.com/en/developer/java-driver/latest/manual/core/udts/)) 
fields cannot be instantiated outside of the Datastax Java driver core. If you want to use prepared statements, you 
must proceed as in the following example:
```java
public class HelloCassandra {
    public void insertTuples(final Connection connection) {
        final Statement statement = connection.createStatement();
        final String createUDT = "CREATE TYPE IF NOT EXISTS fieldmap (key text, value text)";
        final String createTbl = "CREATE TABLE t_udt (id bigint PRIMARY KEY, field_values frozen<fieldmap>, " 
                                + "the_tuple frozen<tuple<int, text, float>>, " 
                                + "the_other_tuple frozen<tuple<int, text, float>>);";
        statement.execute(createUDT);
        statement.execute(createCF);
        statement.close();
        
        final String insertCql = "INSERT INTO t_udt (id, field_values, the_tuple, the_other_tuple) "
                                 + "VALUES (?, {key : ?, value : ?}, (?, ?, ?), ?);";
        final TupleValue tuple = TupleType.of(DataType.cint(), DataType.text(), DataType.cfloat()).newValue();
        tuple.setInt(0, 1).setString(1, "midVal").setFloat(2, (float)2.0);
        
        final PreparedStatement preparedStatement = con.prepareStatement(insert);
        preparedStatement.setLong(1, 1L);
        preparedStatement.setString(2, "key1");
        preparedStatement.setString(3, "value1");
        preparedStatement.setInt(4, 1);
        preparedStatement.setString(5, "midVal");
        preparedStatement.setFloat(6, (float)2.0);
        preparedStatement.setObject(7, (Object)tuple);
        // Execute the prepared statement.
        preparedStatement.execute();
        preparedStatement.close();
    }
}
```

When working on collections of UDTs, it is not possible to use prepared statements. You then have to use simple 
statements as follows:
```java
public class HelloCassandra {
    public void insertCollectionsOfUDT(final Connection connection) {
        final Statement statement = connection.createStatement();
        final String createUDT = "CREATE TYPE IF NOT EXISTS fieldmap (key text, value text)";
        final String createTbl = "CREATE TABLE t_udt_tuple_coll (id bigint PRIMARY KEY, "
                                + "field_values set<frozen<fieldmap>>, "
                                + "the_tuple list<frozen<tuple<int, text, float>>>, "
                                + "field_values_map map<text,frozen<fieldmap>>, "
                                + "tuple_map map<text,frozen<tuple<int,int>>>);";
        statement.execute(createUDT);
        statement.execute(createCF);
        statement.close();
        
        final Statement insertStatement = con.createStatement();
        final String insertCql = "INSERT INTO t_udt_tuple_coll "
                                 + "(id,field_values, the_tuple, field_values_map, tuple_map) "
                                 + "VALUES (1, {{key : 'key1', value : 'value1'}, {key : 'key2', value : 'value2'}}, "
                                 + "[(1, 'midVal1', 1.0), (2, 'midVal2', 2.0)], "
                                 + "{'map_key1' : {key : 'key1', value : 'value1'},"
                                 + "'map_key2' : {key : 'key2', value : 'value2'}}, " 
                                 + "{'tuple1' : (1, 2), 'tuple2' : (2, 3)});";
        insertStatement.execute(insert);
        insertStatement.close();
    }
}
```

## Contributing

Please read our [contributing guide](https://github.com/ing-bank/cassandra-jdbc-wrapper/blob/master/CONTRIBUTING.md) 
and feel free to improve this library!

## Versioning

We use [SemVer](http://semver.org/) for versioning.

## Authors

* Maxime Wiewiora - **[@maximevw](https://github.com/maximevw)** 
* Madhavan Sridharan - **[@msmygit](https://github.com/msmygit)**
* Marius Jokubauskas - **[@mjok](https://github.com/mjok)**

And special thanks to the developer of the original project on which is based this one:
* Alexander Dejanovski - **[@adejanovski](https://github.com/adejanovski)**

## Acknowledgments

* [README Template gist](https://gist.github.com/PurpleBooth/84b3d7d6669f77d5a53801a258ed269a) for the redaction of 
what you're reading.
