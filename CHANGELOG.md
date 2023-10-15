# Release notes
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) and this project adheres to 
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased
### Changed
- Deprecate the parameter `version` (CQL version) in JDBC URL because this one is purely informational and has no 
  effect. This will be removed in the next release.

## [4.10.1] - 2023-10-07
### Changed
- Update Apache Commons IO to version 2.14.0.
- Harmonize logging.
### Fixed
- Fix multiple issues related to the method `findColumn(String)` of `CassandraResultSet` and `CassandraMetadataResultSet`:
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
- Update DataStax Java Driver for Apache Cassandra(R) to version 4.17.0.
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
- Rewrite the tests using Testcontainers with Apache Cassandra(R) 4.1.0 image.
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
- Update DataStax Java Driver for Apache Cassandra(R) to version 4.15.0.
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
- Update DataStax Java Driver for Apache Cassandra(R) to version 4.14.1.

## [4.6.0] - 2022-03-20
### Added
- Add support for connecting to Cassandra DBaaS cluster with secure connect bundle. 
  See the feature request [#1](https://github.com/ing-bank/cassandra-jdbc-wrapper/discussions/1).
- Add query parameter `configfile` to use a configuration file instead of the settings defined in the JDBC URL. 
- Add query parameter `requesttimeout` to specify a non-default timeout for queries. 
  See the feature request [#5](https://github.com/ing-bank/cassandra-jdbc-wrapper/discussions/5).
### Changed
- Update DataStax Java Driver for Apache Cassandra(R) to version 4.14.0.
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
- Update DataStax Java Driver for Apache Cassandra(R) to version 4.10.0.
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
- Update DataStax Java Driver for Apache Cassandra(R) to version 4.9.0.
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
[4.10.1]: https://github.com/ing-bank/cassandra-jdbc-wrapper/compare/v4.10.0...v4.10.1
[4.10.0]: https://github.com/ing-bank/cassandra-jdbc-wrapper/compare/v4.9.1...v4.10.0
[4.9.1]: https://github.com/ing-bank/cassandra-jdbc-wrapper/compare/v4.9.0...v4.9.1
[4.9.0]: https://github.com/ing-bank/cassandra-jdbc-wrapper/compare/v4.8.0...v4.9.0
[4.8.0]: https://github.com/ing-bank/cassandra-jdbc-wrapper/compare/v4.7.0...v4.8.0
[4.7.0]: https://github.com/ing-bank/cassandra-jdbc-wrapper/compare/v4.6.0...v4.7.0
[4.6.0]: https://github.com/ing-bank/cassandra-jdbc-wrapper/compare/v4.5.0...v4.6.0
[4.5.0]: https://github.com/ing-bank/cassandra-jdbc-wrapper/compare/v4.4.0...v4.5.0
