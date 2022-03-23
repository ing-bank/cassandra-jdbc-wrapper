# Release notes
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) and this project adheres to 
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 4.6.0 - 2022-03-20
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

## 4.5.0 - 2021-04-13
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
