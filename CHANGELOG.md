# Release notes
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) and this project adheres to 
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Next release
### Added
- Add implementations for the methods `getSchema()`, `setSchema(String)` and `getTypeMap()` of the class 
  `ManagedConnection`.
- Add implementations for the methods `getPrecision(int)` and `getScale(int)` into the implementations of the interface 
  `ResultSetMetaData` for the classes `CassandraResultSet` and `CassandraMetadataResultSet`.
### Changed
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
