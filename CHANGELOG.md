# Release notes
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) and this project adheres to 
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased
### Added
- Add a parameter `fetchsize` to specify a default fetch size for all the queries returning result sets. This value is 
  the number of rows the server will return in each network frame (see 
  [paging documentation](https://docs.datastax.com/en/developer/java-driver/latest/manual/core/paging/)).
- Add support for Kerberos authentication provider.
### Changed
- Modify the types of some columns in the result sets of the following methods of `CassandraDatabaseMetadata` to respect
  the JDBC API specifications:
  - `getAttributes(String, String, String, String)`
  - `getBestRowIdentifier(String, String, String, int, boolean)`
  - `getColumns(String, String, String, String)` 
  - `getFunctions(String, String, String)` 
  - `getFunctionColumns(String, String, String, String)` 
  - `getIndexInfo(String, String, String, boolean, boolean)`
  - `getPrimaryKeys(String, String, String)`
  - `getTypeInfo()`
  - `getUDTs(String, String, String, int[])` 
- In prepared statements, force the page size to the configured one (from JDBC URL or configuration file if present), or 
  the default page size. 
- Update Apache Commons IO to version 2.15.1.
### Removed
- Remove the parameter `version` (CQL version) in JDBC URL and the deprecated constructors of `CassandraDataSource`
  using this parameter.
### Fixed
- Fix issue preventing a correct usage of `DCInferringLoadBalancingPolicy` (see PR 
  [#49](https://github.com/ing-bank/cassandra-jdbc-wrapper/pull/49)).

## [4.11.1] - 2023-12-28
### Fixed
- Fix issue [#50](https://github.com/ing-bank/cassandra-jdbc-wrapper/issues/50) preventing a correct execution of
  multiple statements separated by semicolon characters (`;`) when at least one of the CQL queries contains a semicolon
  character which is not a query separator.

## [4.11.0] - 2023-12-03
### Added
- Add support for connections with multiple contact points using different ports (see feature request 
  [#41](https://github.com/ing-bank/cassandra-jdbc-wrapper/issues/41)).
- Handle additional types and conversions in the methods `CassandraPreparedStatement.setObject()`:
  - JDBC types `BLOB`, `CLOB`, `NCLOB` and Java types `java.sql.Blob`, `java.sql.Clob`, and `java.sql.NClob` handled as
    arrays of bytes (CQL type `blob`)
  - JDBC types `LONGVARCHAR`, `NCHAR`, `NVARCHAR`, `LONGNVARCHAR` and `DATALINK` and Java type `java.net.URL` handled
    as string (CQL types `text`, `varchar` and `ascii`)
  - JDBC type `TIME_WITH_TIMEZONE` and Java types `java.time.OffsetTime` and `java.time.LocalTime` handled as
    `LocalTime` (CQL type `time`)
  - JDBC type `TIMESTAMP_WITH_TIMEZONE` and Java types `java.util.OffsetDateTime`, `java.time.LocalDateTime`,
    `java.util.Date` and `java.util.Calendar` handled as `Instant` (CQL type `timestamp`)
  - Java type `java.time.LocalDate` (CQL type `date`)
  - JDBC type `BIT` handled as boolean (CQL type `boolean`)
  - JDBC type `NUMERIC` handled as `BigDecimal` (CQL type `decimal`)
  - JDBC type `REAL` handled as float number (CQL type `float`)
- Handle `java.util.Calendar` in the methods `CassandraResultSet.getObject(int | String, Class)`.
- Implement the following methods in `CassandraResultSet`: `getAsciiStream(int | String)`, 
  `getCharacterStream(int | String)`, `getClob(int | String)`, `getNClob(int | String)`.
### Changed
- Deprecate the parameter `version` (CQL version) in JDBC URL because this one is purely informational and has no 
  effect. This will be removed in the next release.
- The index type returned by `CassandraDatabaseMetaData.getIndexInfo(String, String, String, boolean, boolean)` is
  now always `tableIndexOther`.
- Improve the accuracy of the JDBC metadata of the collection types (`list`, `map`, `set` and `vector`).
- Update the following methods of `CassandraDatabaseMetaData`: `getNumericFunctions()`, `getSQLKeywords()`,
  `getSystemFunctions()`, `getTimeDateFunctions()` and `getTypeInfo()` to add the new math, date/time and
  [data masking](https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-20%3A+Dynamic+Data+Masking) 
  functions introduced in Cassandra 5.0 and take into account the version of the database the driver in connected to.
- Update Apache Commons IO to version 2.15.0.
- Update Apache Commons Lang to version 3.14.0.
- Update Jackson dependencies to version 2.16.0.
- Use Apache Cassandra® 5.0 image to run tests.
- Replace references to "DataStax Java driver" by "Java Driver for Apache Cassandra®" following the transfer of the 
  codebase to Apache Software Foundation (see: 
  [IP clearance status](https://incubator.apache.org/ip-clearance/cassandra-java-driver.html) and
  [CEP-8](https://cwiki.apache.org/confluence/x/5Y1rDQ))
### Fixed
- Fix `NullPointerException` issue [#38](https://github.com/ing-bank/cassandra-jdbc-wrapper/issues/38) when a null 
  type name pattern is specified in a call to `CassandraDatabaseMetaData.getUDTs(String, String, String, int[])`.
- Fix issue [#39](https://github.com/ing-bank/cassandra-jdbc-wrapper/issues/39): return `false` when the method 
  `isSearchable(int)` is called on the metadata of a result set without table or schema name (typically on 
  `CassandraMetadataResultSet`s).
- Fix incorrect consistency level used to execute simple prepared statements.
- Fix issue preventing to retrieve the metadata of an empty `CassandraMetadataResultSet`.
- Add null safety on some methods of `CassandraResultSet` and `CassandraMetadataResultSet`.

## [4.10.2] - 2023-11-01
### Fixed
- Fix issue [#33](https://github.com/ing-bank/cassandra-jdbc-wrapper/issues/33) to handle `VARBINARY` and 
  `LONGVARBINARY` types with either `ByteArrayInputStream` or `byte[]` in the methods
  `CassandraPreparedStatement.setObject()`.
- Fix issue [#35](https://github.com/ing-bank/cassandra-jdbc-wrapper/issues/35): configuration of the local
  datacenter using the one from the configuration file when such a file is used.

## [4.10.1] - 2023-10-07
### Changed
- Update Apache Commons IO to version 2.14.0.
- Harmonize logging.
### Fixed
- Fix multiple issues related to the method `findColumn(String)` of `CassandraResultSet` and 
  `CassandraMetadataResultSet`:
  - Fix issue [#31](https://github.com/ing-bank/cassandra-jdbc-wrapper/issues/31) to return a 1-based index value.
  - Return a result even if there's no row in the result set but the column exist in the statement.
  - Fix the exception thrown by the method when the given column name does not exist in the result set (was an
    `IllegalArgumentException` instead of an `SQLException`.

## [4.10.0] - 2023-09-30
### Added
- Add support for new [`vector` CQL type](https://datastax-oss.atlassian.net/browse/JAVA-3060)
  defined in [CEP-30](https://cwiki.apache.org/confluence/x/OQ40Dw). 
  Also see PR [#27](https://github.com/ing-bank/cassandra-jdbc-wrapper/pull/27).
- Implement the method `getWarnings()` in `CassandraResultSet`.
- Implement the following methods of `CassandraDatabaseMetaData`: 
  `getBestRowIdentifier(String, String, String, int, boolean)` and `getAttributes(String, String, String, String)`.
### Changed
- Update DataStax Java Driver for Apache Cassandra® to version 4.17.0.
- Update Apache Commons IO to version 2.13.0.
- Update Apache Commons Lang to version 3.13.0.
- Update Jackson dependencies to version 2.15.2.
- Packages refactoring: utility classes, types and database metadata management have been moved to dedicated packages.
### Removed
- Remove the legacy package `org.apache.cassandra2.cql.jdbc`: only `com.ing.data.cassandra.jdbc.CassandraDriver` should
  be used now as `java.sql.Driver` implementation.

## [4.9.1] - 2023-09-03
### Fixed
- Fix issue [#25](https://github.com/ing-bank/cassandra-jdbc-wrapper/issues/25) causing failure when running with
  Liquibase. The fix includes several changes:
  - fixes result sets and statements closing.
  - introduces a new behaviour in Liquibase compliance mode to run multiple queries in the same statement synchronously
    (by default, they are executed asynchronously).
  - returns the schema name instead of `null` when the method `CassandraConnection.getCatalog()` is called in Liquibase
    compliance mode.
  - does not throw `SQLFeatureNotSupportedException` when `CassandraConnection.rollback()` is called in Liquibase
    compliance mode.

## [4.9.0] - 2023-04-15
### Added
- Add non-JDBC standard [JSON support](https://cassandra.apache.org/doc/latest/cassandra/cql/json.html) with the 
  methods `getObjectFromJson(int | String, Class)` and `getObjectFromJson(Class)` in `CassandraResultSet` and
  `setJson(int, Object)` in `CassandraPreparedStatement`.
- Add query parameter `hostnameverification` to specify whether the hostname verification must be enabled or not when 
  SSL connection is used. See the discussion [#20](https://github.com/ing-bank/cassandra-jdbc-wrapper/discussions/20).
- Add some socket options thanks to the additional query parameters: `connecttimeout`, `tcpnodelay` and `keepalive`. 
  It fixes the issue [#16](https://github.com/adejanovski/cassandra-jdbc-wrapper/issues/16) of the [original project].
- Implement the methods `isSigned()` and `isSearchable()` in the different `ResultSetMetaData` implementations.
- Implement the method `isValid(int)` in `CassandraConnection`.
- Implement the following methods of `CassandraDatabaseMetaData`: `getFunctions(String, String, String)`,
  `getFunctionColumns(String, String, String, String)`, `getNumericFunctions()`, `getSQLKeywords()`, 
  `getStringFunctions()`, `getSystemFunctions()`, `getTimeDateFunctions()`, `getTypeInfo()` and 
  `getUDTs(String, String, String, int[])`.
### Changed
- Harmonize the implementations of `Wrapper` interface.
- Rewrite the tests using Testcontainers with Apache Cassandra® 4.1.0 image.
- Modify the implementation of `setQueryTimeout(int)` and `getQueryTimeout()` in `CassandraStatement` to update the
  request timeout on a specific statement.
### Removed
- Remove vulnerable Guava compile dependency and replace it by standard Java, Apache Commons libraries and Caffeine
  for sessions caching.
### Fixed
- Fix the JDBC driver version returned by the methods of the classes `CassandraDriver` and `CassandraDatabaseMetaData` 
  to be consistent with the version of the JDBC wrapper artifact (see issue 
  [#19](https://github.com/ing-bank/cassandra-jdbc-wrapper/issues/19)).
- Fix an issue on collections of tuples and UDTs that threw `NullPointerException` in result sets when calling methods
  such as `getList()`, `getSet()` and `getMap()`.

## [4.8.0] - 2023-01-12
### Added
- Implement the methods `getMetaData()` and `getParameterMetaData()` into the implementation class
  `CassandraPreparedStatement` of `PreparedStatement` interface. It fixes the issue
  [#19](https://github.com/adejanovski/cassandra-jdbc-wrapper/issues/19) of the [original project].
### Changed
- Update DataStax Java Driver for Apache Cassandra® to version 4.15.0.
- Fully implement methods from `Wrapper` interface for Cassandra connections, results sets and statements (see pull 
  request [#14](https://github.com/ing-bank/cassandra-jdbc-wrapper/pull/14)).
### Fixed
- Fix the implementations of the methods `getInt(int)` and `getLong(int)` in `MetadataRow` and used in the class
  `CassandraMetadataResultSet` to be compliant with the JDBC API specifications (see pull request
  [#12](https://github.com/ing-bank/cassandra-jdbc-wrapper/pull/12), issue 
  [#10](https://github.com/ing-bank/cassandra-jdbc-wrapper/issues/10)).

## [4.7.0] - 2022-09-23
### Added
- Add a system of compliance mode with the query parameter `compliancemode`: for some usages (for example with 
  Liquibase), some default behaviours of the JDBC implementation have to be adapted. See the readme file for details 
  about the overridable behaviours and the available compliance modes. See pull request
  [#8](https://github.com/ing-bank/cassandra-jdbc-wrapper/pull/8).
- Add an additional `CassandraConnection` constructor using a pre-existing session (see pull request
  [#8](https://github.com/ing-bank/cassandra-jdbc-wrapper/pull/8)).
### Changed
- Update DataStax Java Driver for Apache Cassandra® to version 4.14.1.

## [4.6.0] - 2022-03-20
### Added
- Add support for connecting to Cassandra DBaaS cluster with secure connect bundle. 
  See the feature request [#1](https://github.com/ing-bank/cassandra-jdbc-wrapper/discussions/1).
- Add query parameter `configfile` to use a configuration file instead of the settings defined in the JDBC URL. 
- Add query parameter `requesttimeout` to specify a non-default timeout for queries. 
  See the feature request [#5](https://github.com/ing-bank/cassandra-jdbc-wrapper/discussions/5).
### Changed
- Update DataStax Java Driver for Apache Cassandra® to version 4.14.0.
- Update Apache Commons Lang to version 3.12.0.
### Removed
- Remove `cassandra-all` and `libthrift` dependencies to limit exposure to vulnerable libraries (see pull request
  [#6](https://github.com/ing-bank/cassandra-jdbc-wrapper/pull/6)).
### Fixed
- User credentials parameters in the connection string were ignored.

## [4.5.0] - 2021-04-13
### Added
- Manage additional CQL types: `duration`, `smallint` and `tinyint`.
- Add support for SSL between the driver and the Cassandra cluster.
- Implement the methods `getSchema()`, `setSchema(String)` and `getTypeMap()` in the class `ManagedConnection`.
- Implement the methods `getPrecision(int)` and `getScale(int)` into the implementations of the interface 
  `ResultSetMetaData` for the classes `CassandraResultSet` and `CassandraMetadataResultSet`.
- Implement the methods `getURL(int|String)` in the classes `CassandraResultSet` and `CassandraMetadataResultSet`. The 
  URL values are handled as `String` values.
- Add codecs for conversions between `Integer` and CQL types `varint`, `smallint` and `tinyint`. It also fixes the issue
  [#33](https://github.com/adejanovski/cassandra-jdbc-wrapper/issues/33) of the [original project].
### Changed
- Update DataStax Java Driver for Apache Cassandra® to version 4.10.0.
- Update `cassandra-all` to version 3.11.9.
- Improve documentation and code quality (refactoring, removing dead code, adding tests, ...).
- Improve the implementation of the metadata precision/size for the columns.
### Fixed
- Fix values returned by some methods in `CassandraDatabaseMetaData` according to the capabilities described into the
  CQL3 documentation.
- Fix issue [#24](https://github.com/adejanovski/cassandra-jdbc-wrapper/issues/24) of the [original project]
  by correctly implementing JDBC API regarding the result returned by the methods `CassandraStatement.execute(String)`
  and `CassandraPreparedStatement.execute()`.
- Fix `CassandraResultSet.getLong(int | String)` implementations to return 0 when the stored value is SQL `NULL`.  
- Add null-safety into the methods `CassandraResultSet.getString(int | String)`,
  `CassandraMetadataResultSet.getBigDecimal(int | String, int)`, 
  `CassandraMetadataResultSet.getBinaryStream(int | String)`, `CassandraMetadataResultSet.getBlob(int | String)`, 
  `CassandraMetadataResultSet.getByte(int | String)`, `CassandraMetadataResultSet.getBytes(int | String)` and 
  `CassandraMetadataResultSet.getString(int | String)`.
- Validate column existence when calling the methods `CassandraResultSet.getBytes(int | String)`.

## 4.4.0 - 2020-12-23
For this version, the changelog lists the main changes comparatively to the latest version of the [original project].
### Changed
- Update DataStax Java Driver for Apache Cassandra® to version 4.9.0.
- Update `cassandra-all` to version 3.11.8.
- Force using `libthrift` 0.13.0 instead of the vulnerable version included into `cassandra-all`.
- Manage separately the type `LocalDate` in `CassandraResultSet`.
### Removed
- Remove deprecated load balancing policy (`DCAwareRoundRobinPolicy`).
### Fixed
- Fix issue [#27](https://github.com/adejanovski/cassandra-jdbc-wrapper/issues/27) of the [original project] 
  by implementing the method `CassandraResultSet.getObject(String, Class<>)`.
- Fix logs in `CassandraConnection` constructor.

[original project]: https://github.com/adejanovski/cassandra-jdbc-wrapper/
[4.11.1]: https://github.com/ing-bank/cassandra-jdbc-wrapper/compare/v4.11.0...v4.11.1
[4.11.0]: https://github.com/ing-bank/cassandra-jdbc-wrapper/compare/v4.10.2...v4.11.0
[4.10.2]: https://github.com/ing-bank/cassandra-jdbc-wrapper/compare/v4.10.1...v4.10.2
[4.10.1]: https://github.com/ing-bank/cassandra-jdbc-wrapper/compare/v4.10.0...v4.10.1
[4.10.0]: https://github.com/ing-bank/cassandra-jdbc-wrapper/compare/v4.9.1...v4.10.0
[4.9.1]: https://github.com/ing-bank/cassandra-jdbc-wrapper/compare/v4.9.0...v4.9.1
[4.9.0]: https://github.com/ing-bank/cassandra-jdbc-wrapper/compare/v4.8.0...v4.9.0
[4.8.0]: https://github.com/ing-bank/cassandra-jdbc-wrapper/compare/v4.7.0...v4.8.0
[4.7.0]: https://github.com/ing-bank/cassandra-jdbc-wrapper/compare/v4.6.0...v4.7.0
[4.6.0]: https://github.com/ing-bank/cassandra-jdbc-wrapper/compare/v4.5.0...v4.6.0
[4.5.0]: https://github.com/ing-bank/cassandra-jdbc-wrapper/compare/v4.4.0...v4.5.0
