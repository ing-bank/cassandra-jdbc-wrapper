/*
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.ing.data.cassandra.jdbc;

import com.datastax.oss.driver.api.core.data.UdtValue;
import com.ing.data.cassandra.jdbc.metadata.CatalogMetadataResultSetBuilder;
import com.ing.data.cassandra.jdbc.metadata.SchemaMetadataResultSetBuilder;
import com.ing.data.cassandra.jdbc.metadata.TableMetadataResultSetBuilder;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;

import static com.ing.data.cassandra.jdbc.utils.Utils.NOT_SUPPORTED;
import static com.ing.data.cassandra.jdbc.utils.Utils.NO_INTERFACE;
import static com.ing.data.cassandra.jdbc.utils.Utils.getDriverProperty;
import static com.ing.data.cassandra.jdbc.utils.Utils.parseVersion;

/**
 * Cassandra database metadata: implementation class for {@link DatabaseMetaData}.
 */
public class CassandraDatabaseMetaData implements DatabaseMetaData {

    static final int UNKNOWN_MAX_VALUE = 0;
    static final int KEYSPACE_NAME_MAX_LENGTH = 48;
    static final int TABLE_NAME_MAX_LENGTH = 48;
    static final String CATALOG_VENDOR_TERM = "Cluster";
    static final String SCHEMA_VENDOR_TERM = "Keyspace";

    private final CassandraConnection connection;
    private CassandraStatement statement;

    /**
     * Constructor.
     *
     * @param connection The connection to a Cassandra database.
     * @throws SQLException when something went wrong during the initialisation of the
     *                      {@code CassandraDatabaseMetaData}.
     */
    public CassandraDatabaseMetaData(final CassandraConnection connection) throws SQLException {
        this.connection = connection;
        this.statement = new CassandraStatement(this.connection);
    }

    @Override
    public boolean isWrapperFor(final Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(this.getClass());
    }

    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        if (isWrapperFor(iface)) {
            return iface.cast(this);
        } else {
            throw new SQLException(String.format(NO_INTERFACE, iface.getSimpleName()));
        }
    }

    @Override
    public boolean allProceduresAreCallable() {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() {
        return true;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() {
        return false;
    }

    @Override
    public boolean deletesAreDetected(final int type) {
        return false;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() {
        return false;
    }

    @Override
    public boolean generatedKeyAlwaysReturned() {
        return false;
    }

    @Override
    public ResultSet getAttributes(final String catalog, final String schemaPattern, final String typeNamePattern,
                                   final String attributeNamePattern) throws SQLException {
        // TODO: method to implement
        checkStatementClosed();
        return CassandraResultSet.EMPTY_RESULT_SET;
    }

    @Override
    public ResultSet getBestRowIdentifier(final String catalog, final String schema, final String table,
                                          final int scope, final boolean nullable) throws SQLException {
        // TODO: method to implement
        checkStatementClosed();
        return CassandraResultSet.EMPTY_RESULT_SET;
    }

    @Override
    public String getCatalogSeparator() {
        return StringUtils.EMPTY;
    }

    @Override
    public String getCatalogTerm() {
        return CATALOG_VENDOR_TERM;
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        checkStatementClosed();
        return new CatalogMetadataResultSetBuilder(this.statement).buildCatalogs();
    }

    /**
     * Retrieves a list of the client info properties that the driver supports.
     * <p>
     * Cassandra database does not support client info properties, so this method will throw a
     * {@link SQLFeatureNotSupportedException}.
     * </p>
     *
     * @return Always throw a {@link SQLFeatureNotSupportedException}.
     */
    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    /**
     * Retrieves a description of the access rights for a table's columns.
     * <p>
     * Datastax Java driver for Apache Cassandra(R) currently does not provide information about permissions and only
     * super users can access to such information through {@code LIST ALL PERMISSIONS ON <tableName>}, so it cannot
     * be implemented safely for any connection to the database, that's why this method will throw a
     * {@link SQLFeatureNotSupportedException}.
     * </p>
     *
     * @param catalog           A catalog name; must match the catalog name as it is stored in the database;
     *                          {@code ""} retrieves those without a catalog; {@code null} means that the catalog name
     *                          should not be used to narrow the search.
     * @param schema            A schema name pattern; must match the schema name as it is stored in the database;
     *                          {@code ""} retrieves those without a schema; {@code null} means that the schema name
     *                          should not be used to narrow the search.
     * @param table             A table name pattern; must match the table name as it is stored in the database.
     * @param columnNamePattern A column name pattern; must match the column name as it is stored in the database.
     * @return Always throw a {@link SQLFeatureNotSupportedException}.
     */
    @Override
    public ResultSet getColumnPrivileges(final String catalog, final String schema, final String table,
                                         final String columnNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public ResultSet getColumns(final String catalog, final String schemaPattern, final String tableNamePattern,
                                final String columnNamePattern) throws SQLException {
        checkStatementClosed();
        if (catalog == null || catalog.equals(this.connection.getCatalog())) {
            this.statement.connection = connection;
            String schemaNamePattern = schemaPattern;
            if (schemaPattern == null) {
                schemaNamePattern = this.connection.getSchema(); // limit to current schema if defined.
            }
            return MetadataResultSets.INSTANCE.makeColumns(statement, schemaNamePattern, tableNamePattern,
                columnNamePattern);
        }
        return CassandraResultSet.EMPTY_RESULT_SET;
    }

    @Override
    public Connection getConnection() {
        return this.connection;
    }

    /**
     * Retrieves a description of the foreign key columns in the given foreign key table that reference the primary key
     * or the columns representing a unique constraint of the parent table (could be the same or a different table).
     * <p>
     * Cassandra database does not support foreign keys, so this method will throw a
     * {@link SQLFeatureNotSupportedException}.
     * </p>
     *
     * @param parentCatalog  A catalog name; must match the catalog name as it is stored in the database; {@code ""}
     *                       retrieves those without a catalog; {@code null} means drop catalog name from the
     *                       selection criteria.
     * @param parentSchema   A schema name; must match the schema name as it is stored in the database; {@code ""}
     *                       retrieves those without a schema; {@code null} means drop schema name from the
     *                       selection criteria.
     * @param parentTable    The name of the table that exports the key; must match the table name as it is stored
     *                       in the database.
     * @param foreignCatalog A catalog name; must match the catalog name as it is stored in the database; {@code ""}
     *                       retrieves those without a catalog; {@code null} means drop catalog name from the
     *                       selection criteria.
     * @param foreignSchema  A schema name; must match the schema name as it is stored in the database; {@code ""}
     *                       retrieves those without a schema; null means drop schema name from the selection
     *                       criteria.
     * @param foreignTable   The name of the table that imports the key; must match the table name as it is stored
     *                       in the database.
     * @return Always throw a {@link SQLFeatureNotSupportedException}.
     */
    @Override
    public ResultSet getCrossReference(final String parentCatalog, final String parentSchema, final String parentTable,
                                       final String foreignCatalog, final String foreignSchema,
                                       final String foreignTable) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public int getDatabaseMajorVersion() {
        return CassandraConnection.dbMajorVersion;
    }

    @Override
    public int getDatabaseMinorVersion() {
        return CassandraConnection.dbMinorVersion;
    }

    @Override
    public String getDatabaseProductName() {
        return getDriverProperty("database.productName");
    }

    /**
     * Retrieves the version number of this database product.
     * <p>
     * The version number returned by this method is the minimal version of Apache Cassandra supported by this JDBC
     * implementation (see Datastax Java driver version embedded into this JDBC wrapper and
     * <a href="https://docs.datastax.com/en/driver-matrix/doc/driver_matrix/javaDrivers.html">
     * compatibility matrix</a> for further details.
     * </p>
     *
     * @return The database version number.
     */
    @Override
    public String getDatabaseProductVersion() {
        return String.format("%d.%d.%d", CassandraConnection.dbMajorVersion, CassandraConnection.dbMinorVersion,
            CassandraConnection.dbPatchVersion);
    }

    @Override
    public int getDefaultTransactionIsolation() {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public int getDriverMajorVersion() {
        return parseVersion(getDriverVersion(), 0);
    }

    @Override
    public int getDriverMinorVersion() {
        return parseVersion(getDriverVersion(), 1);
    }

    @Override
    public String getDriverName() {
        return getDriverProperty("driver.name");
    }

    @Override
    public String getDriverVersion() {
        return getDriverProperty("driver.version");
    }

    /**
     * Retrieves a description of the foreign key columns that reference the given table's primary key columns (the
     * foreign keys exported by a table).
     * <p>
     * Cassandra database does not support foreign keys, so this method will throw a
     * {@link SQLFeatureNotSupportedException}.
     * </p>
     *
     * @param catalog A catalog name; must match the catalog name as it is stored in this database; {@code ""}
     *                retrieves those without a catalog; {@code null} means that the catalog name should not be used
     *                to narrow the search.
     * @param schema  A schema name; must match the schema name as it is stored in the database; {@code ""} retrieves
     *                those without a schema; {@code null} means that the schema name should not be used to narrow
     *                the search.
     * @param table   A table name; must match the table name as it is stored in this database.
     * @return Always throw a {@link SQLFeatureNotSupportedException}.
     */
    @Override
    public ResultSet getExportedKeys(final String catalog, final String schema, final String table)
        throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public String getExtraNameCharacters() {
        return StringUtils.EMPTY;
    }

    @Override
    public ResultSet getFunctionColumns(final String catalog, final String schemaPattern,
                                        final String functionNamePattern, final String columnNamePattern)
        throws SQLException {
        checkStatementClosed();
        if (catalog == null || catalog.equals(this.connection.getCatalog())) {
            String schemaName = schemaPattern;
            if (schemaPattern == null) {
                schemaName = this.connection.getSchema(); // limit to current schema if defined.
            }
            return MetadataResultSets.INSTANCE.makeFunctionColumns(this.statement, schemaName, functionNamePattern,
                columnNamePattern);
        }
        return CassandraResultSet.EMPTY_RESULT_SET;
    }

    @Override
    public ResultSet getFunctions(final String catalog, final String schemaPattern, final String functionNamePattern)
        throws SQLException {
        checkStatementClosed();
        if (catalog == null || catalog.equals(this.connection.getCatalog())) {
            String schemaName = schemaPattern;
            if (schemaPattern == null) {
                schemaName = this.connection.getSchema(); // limit to current schema if defined.
            }
            return MetadataResultSets.INSTANCE.makeFunctions(this.statement, schemaName, functionNamePattern);
        }
        return CassandraResultSet.EMPTY_RESULT_SET;
    }

    @Override
    public String getIdentifierQuoteString() {
        return StringUtils.SPACE;
    }

    /**
     * Retrieves a description of the primary key columns that are referenced by the given table's foreign key columns
     * (the primary keys imported by a table).
     * <p>
     * Cassandra database does not support foreign keys, so this method will throw a
     * {@link SQLFeatureNotSupportedException}.
     * </p>
     *
     * @param catalog A catalog name; must match the catalog name as it is stored in this database; {@code ""}
     *                retrieves those without a catalog; {@code null} means that the catalog name should not be used
     *                to narrow the search.
     * @param schema  A schema name; must match the schema name as it is stored in the database; {@code ""} retrieves
     *                those without a schema; {@code null} means that the schema name should not be used to narrow
     *                the search.
     * @param table   A table name; must match the table name as it is stored in this database.
     * @return Always throw a {@link SQLFeatureNotSupportedException}.
     */
    @Override
    public ResultSet getImportedKeys(final String catalog, final String schema, final String table)
        throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public ResultSet getIndexInfo(final String catalog, final String schema, final String table, final boolean unique,
                                  final boolean approximate) throws SQLException {
        checkStatementClosed();
        if (catalog == null || catalog.equals(this.connection.getCatalog())) {
            String schemaName = schema;
            if (schema == null) {
                schemaName = this.connection.getSchema(); // limit to current schema if defined.
            }
            return MetadataResultSets.INSTANCE.makeIndexes(this.statement, schemaName, table, unique, approximate);
        }
        return CassandraResultSet.EMPTY_RESULT_SET;
    }

    @Override
    public int getJDBCMajorVersion() {
        return parseVersion(getDriverProperty("driver.jdbcVersion"), 0);
    }

    @Override
    public int getJDBCMinorVersion() {
        return parseVersion(getDriverProperty("driver.jdbcVersion"), 1);
    }

    @Override
    public int getMaxBinaryLiteralLength() {
        // Cassandra can represent a 2GB value, but CQL has to encode it in hexadecimal.
        return Integer.MAX_VALUE / 2;
    }

    @Override
    public int getMaxCatalogNameLength() {
        return Byte.MAX_VALUE;
    }

    @Override
    public int getMaxCharLiteralLength() {
        return Character.MAX_VALUE;
    }

    @Override
    public int getMaxColumnNameLength() {
        return Character.MAX_VALUE;
    }

    @Override
    public int getMaxColumnsInGroupBy() {
        return UNKNOWN_MAX_VALUE;
    }

    @Override
    public int getMaxColumnsInIndex() {
        return UNKNOWN_MAX_VALUE;
    }

    @Override
    public int getMaxColumnsInOrderBy() {
        return UNKNOWN_MAX_VALUE;
    }

    @Override
    public int getMaxColumnsInSelect() {
        return UNKNOWN_MAX_VALUE;
    }

    @Override
    public int getMaxColumnsInTable() {
        return UNKNOWN_MAX_VALUE;
    }

    @Override
    public int getMaxConnections() {
        return UNKNOWN_MAX_VALUE;
    }

    @Override
    public int getMaxCursorNameLength() {
        return UNKNOWN_MAX_VALUE;
    }

    @Override
    public int getMaxIndexLength() {
        return UNKNOWN_MAX_VALUE;
    }

    @Override
    public int getMaxProcedureNameLength() {
        return UNKNOWN_MAX_VALUE;
    }

    @Override
    public int getMaxRowSize() {
        return UNKNOWN_MAX_VALUE;
    }

    @Override
    public int getMaxSchemaNameLength() {
        return KEYSPACE_NAME_MAX_LENGTH;
    }

    @Override
    public int getMaxStatementLength() {
        return UNKNOWN_MAX_VALUE;
    }

    @Override
    public int getMaxStatements() {
        return UNKNOWN_MAX_VALUE;
    }

    @Override
    public int getMaxTableNameLength() {
        return TABLE_NAME_MAX_LENGTH;
    }

    @Override
    public int getMaxTablesInSelect() {
        return UNKNOWN_MAX_VALUE;
    }

    @Override
    public int getMaxUserNameLength() {
        return UNKNOWN_MAX_VALUE;
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        checkStatementClosed();
        // Cassandra does not implement natively numeric functions.
        return StringUtils.EMPTY;
    }

    @Override
    public ResultSet getPrimaryKeys(final String catalog, final String schema, final String table) throws SQLException {
        checkStatementClosed();
        if (catalog == null || catalog.equals(this.connection.getCatalog())) {
            String schemaName = schema;
            if (schema == null) {
                schemaName = this.connection.getSchema(); // limit to current schema if defined.
            }
            return MetadataResultSets.INSTANCE.makePrimaryKeys(this.statement, schemaName, table);
        }
        return CassandraResultSet.EMPTY_RESULT_SET;
    }

    /**
     * Retrieves a description of the given catalog's stored procedure parameter and result columns.
     * <p>
     * Cassandra database does not support procedures, so this method will throw a
     * {@link SQLFeatureNotSupportedException}.
     * </p>
     *
     * @param catalog              A catalog name; must match the catalog name as it is stored in the database;
     *                             {@code ""} retrieves those without a catalog; {@code null} means that the catalog
     *                             name should not be used to narrow the search.
     * @param schemaPattern        A schema name pattern; must match the schema name as it is stored in the database;
     *                             {@code ""} retrieves those without a schema; {@code null} means that the schema
     *                             name should not be used to narrow the search.
     * @param procedureNamePattern A procedure name pattern; must match the procedure name as it is stored in the
     *                             database.
     * @param columnNamePattern    A column name pattern; must match the column name as it is stored in the database.
     * @return Always throw a {@link SQLFeatureNotSupportedException}.
     */
    @Override
    public ResultSet getProcedureColumns(final String catalog, final String schemaPattern,
                                         final String procedureNamePattern, final String columnNamePattern)
        throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    /**
     * Retrieves the database vendor's preferred term for "procedure". Since "procedures" are not supported in Cassandra
     * database, this method always returns an empty string.
     *
     * @return An empty string.
     */
    @Override
    public String getProcedureTerm() {
        return StringUtils.EMPTY;
    }

    /**
     * Retrieves a description of the stored procedures available in the given catalog.
     * <p>
     * Cassandra database does not support procedures, so this method will throw a
     * {@link SQLFeatureNotSupportedException}.
     * </p>
     *
     * @param catalog              A catalog name; must match the catalog name as it is stored in the database;
     *                             {@code ""} retrieves those without a catalog; {@code null} means that the catalog
     *                             name should not be used to narrow the search.
     * @param schemaPattern        A schema name pattern; must match the schema name as it is stored in the database;
     *                             {@code ""} retrieves those without a schema; {@code null} means that the schema
     *                             name should not be used to narrow the search.
     * @param procedureNamePattern A procedure name pattern; must match the procedure name as it is stored in the
     *                             database.
     * @return Always throw a {@link SQLFeatureNotSupportedException}.
     */
    @Override
    public ResultSet getProcedures(final String catalog, final String schemaPattern,
                                   final String procedureNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    /**
     * Retrieves a description of the pseudo or hidden columns available in a given table within the specified catalog
     * and schema.
     * <p>
     * Cassandra database does not support pseudo or hidden columns, so this method will throw a
     * {@link SQLFeatureNotSupportedException}.
     * </p>
     *
     * @param catalog           A catalog name; must match the catalog name as it is stored in the database;
     *                          {@code ""} retrieves those without a catalog; {@code null} means that the catalog name
     *                          should not be used to narrow the search.
     * @param schemaPattern     A schema name pattern; must match the schema name as it is stored in the database;
     *                          {@code ""} retrieves those without a schema; {@code null} means that the schema name
     *                          should not be used to narrow the search.
     * @param tableNamePattern  A table name pattern; must match the table name as it is stored in the database.
     * @param columnNamePattern A column name pattern; must match the column name as it is stored in the database.
     * @return Always throw a {@link SQLFeatureNotSupportedException}.
     */
    @Override
    public ResultSet getPseudoColumns(final String catalog, final String schemaPattern, final String tableNamePattern,
                                      final String columnNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public int getResultSetHoldability() {
        return CassandraResultSet.DEFAULT_HOLDABILITY;
    }


    @Override
    public RowIdLifetime getRowIdLifetime() {
        // ROWID type is not supported by Cassandra.
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    /**
     * Retrieves a comma-separated list of all the Cassandra CQL keywords that are NOT also SQL:2003 keywords.
     *
     * @return The list of Cassandra's keywords that are not also SQL:2003 keywords.
     * @throws SQLException if a database access error occurs.
     */
    @Override
    public String getSQLKeywords() throws SQLException {
        checkStatementClosed();
        // Cassandra does not use SQL but CQL. Here are list all the keywords used by Cassandra and not in common with
        // SQL:2003 standard keywords (see: https://ronsavage.github.io/SQL/sql-2003-2.bnf.html#xref-keywords).
        // The CQL keywords are listed here:
        // https://cassandra.apache.org/doc/latest/cassandra/cql/appendices.html#appendix-A
        // Also add new keywords relative to vector type introduced by CEP-30:
        // https://cwiki.apache.org/confluence/x/OQ40Dw
        final List<String> cqlKeywords = Arrays.asList("AGGREGATE", "ALLOW", "ANN OF", "APPLY", "ASCII", "AUTHORIZE",
            "BATCH", "CLUSTERING", "COLUMNFAMILY", "COMPACT", "COUNTER", "CUSTOM", "ENTRIES", "FILTERING", "FINALFUNC",
            "FROZEN", "FUNCTIONS", "IF", "INDEX", "INET", "INFINITY", "INITCOND", "JSON", "KEYS", "KEYSPACE",
            "KEYSPACES", "LIMIT", "LIST", "LOGIN", "MODIFY", "NAN", "NOLOGIN", "NORECURSIVE", "NOSUPERUSER", "PASSWORD",
            "PERMISSION", "PERMISSIONS", "RENAME", "REPLACE", "RETURNS", "ROLES", "SFUNC", "SMALLINT", "STORAGE",
            "STYPE", "SUPERUSER", "TEXT", "TIMEUUID", "TINYINT", "TOKEN", "TRUNCATE", "TTL", "TUPLE", "UNLOGGED", "USE",
            "USERS", "UUID", "VARINT", "VECTOR", "WRITETIME");
        return String.join(",", cqlKeywords);
    }

    @Override
    public int getSQLStateType() {
        return sqlStateSQL;
    }

    @Override
    public String getSchemaTerm() {
        return SCHEMA_VENDOR_TERM;
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        checkStatementClosed();
        return new SchemaMetadataResultSetBuilder(this.statement).buildSchemas(null);
    }

    @Override
    public ResultSet getSchemas(final String catalog, final String schemaPattern) throws SQLException {
        checkStatementClosed();
        if (!(catalog == null || catalog.equals(this.statement.connection.getCatalog()))) {
            throw new SQLSyntaxErrorException("Catalog name must exactly match or be null.");
        }
        return new SchemaMetadataResultSetBuilder(this.statement).buildSchemas(schemaPattern);
    }

    @Override
    public String getSearchStringEscape() {
        return "\\";
    }

    @Override
    public String getStringFunctions() throws SQLException {
        checkStatementClosed();
        // Cassandra does not implement natively string functions.
        return StringUtils.EMPTY;
    }

    /**
     * Retrieves a description of the table hierarchies defined in a particular schema in this database.
     * <p>
     * Cassandra database does not support super tables, so this method will throw a
     * {@link SQLFeatureNotSupportedException}.
     * </p>
     *
     * @param catalog          A catalog name; {@code ""} retrieves those without a catalog; {@code null} means drop
     *                         catalog name from the selection criteria.
     * @param schemaPattern    A schema name pattern; {@code ""} retrieves those without a schema.
     * @param tableNamePattern A table name pattern; may be a fully-qualified name.
     * @return Always throw a {@link SQLFeatureNotSupportedException}.
     */
    @Override
    public ResultSet getSuperTables(final String catalog, final String schemaPattern, final String tableNamePattern)
        throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    /**
     * Retrieves a description of the user-defined type (UDT) hierarchies defined in a particular schema in this
     * database.
     * <p>
     * Cassandra database does not support super types, so this method will throw a
     * {@link SQLFeatureNotSupportedException}.
     * </p>
     *
     * @param catalog         A catalog name; {@code ""} retrieves those without a catalog; {@code null} means drop
     *                        catalog name from the selection criteria.
     * @param schemaPattern   A schema name pattern; {@code ""} retrieves those without a schema.
     * @param typeNamePattern A UDT name pattern; may be a fully-qualified name.
     * @return Always throw a {@link SQLFeatureNotSupportedException}.
     */
    @Override
    public ResultSet getSuperTypes(final String catalog, final String schemaPattern, final String typeNamePattern)
        throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        checkStatementClosed();
        return "TOKEN,TTL,WRITETIME";
    }

    /**
     * Retrieves a description of the access rights for each table available in a catalog (Cassandra cluster).
     * <p>
     * Datastax Java driver for Apache Cassandra(R) currently does not provide information about permissions and only
     * super users can access to such information through {@code LIST ALL PERMISSIONS ON <tableName>}, so it cannot
     * be implemented safely for any connection to the database, that's why this method will throw a
     * {@link SQLFeatureNotSupportedException}.
     * </p>
     *
     * @param catalog           A catalog name; must match the catalog name as it is stored in the database;
     *                          {@code ""} retrieves those without a catalog; {@code null} means that the catalog name
     *                          should not be used to narrow the search.
     * @param schemaPattern     A schema name pattern; must match the schema name as it is stored in the database;
     *                          {@code ""} retrieves those without a schema; {@code null} means that the schema name
     *                          should not be used to narrow the search.
     * @param tableNamePattern  A table name pattern; must match the table name as it is stored in the database.
     * @return Always throw a {@link SQLFeatureNotSupportedException}.
     */
    @Override
    public ResultSet getTablePrivileges(final String catalog, final String schemaPattern,
                                        final String tableNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        checkStatementClosed();
        return new TableMetadataResultSetBuilder(this.statement).buildTableTypes();
    }

    @Override
    public ResultSet getTables(final String catalog, final String schemaPattern, final String tableNamePattern,
                               final String[] types) throws SQLException {
        // Note: only TABLE or null are taken into account for types parameter here since Cassandra only supports
        // TABLE type.
        boolean askingForTable = types == null;
        if (types != null) {
            askingForTable = Arrays.asList(types).contains(MetadataResultSets.TABLE);
        }
        // Only null or the current catalog (i.e. cluster) name are supported.
        if ((catalog == null || catalog.equals(this.connection.getCatalog())) && askingForTable) {
            checkStatementClosed();
            return new TableMetadataResultSetBuilder(this.statement).buildTables(schemaPattern, tableNamePattern);
        }
        return CassandraResultSet.EMPTY_RESULT_SET;
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        checkStatementClosed();
        // See: https://cassandra.apache.org/doc/latest/cassandra/cql/functions.html
        return "dateOf,now,minTimeuuid,maxTimeuuid,unixTimestampOf,toDate,toTimestamp,toUnixTimestamp,currentTimestamp,"
            + "currentDate,currentTime,currentTimeUUID,";
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        checkStatementClosed();
        return MetadataResultSets.INSTANCE.makeTypes(this.statement);
    }

    /**
     * Retrieves a description of the user-defined types (UDTs) defined in a particular schema.
     * <p>
     * Schema-specific UDTs in a Cassandra database will be considered as having type {@code JAVA_OBJECT}.
     * </p>
     * <p>
     * Only types matching the catalog, schema, type name and type criteria are returned. They are ordered by
     * {@code DATA_TYPE}, {@code TYPE_CAT}, {@code TYPE_SCHEM} and {@code TYPE_NAME}. The type name parameter may be
     * a fully-qualified name (it should respect the format {@code <SCHEMA_NAME>.<TYPE_NAME>}). In this case, the
     * {@code catalog} and {@code schemaPattern} parameters are ignored.
     * </p>
     * <p>Each type description has the following columns:
     * <ol>
     *     <li><b>TYPE_CAT</b> String => type's catalog, may be {@code null}: here is the Cassandra cluster name
     *     (if available).</li>
     *     <li><b>TYPE_SCHEM</b> String => type's schema, may be {@code null}: here is the keyspace the type is
     *     member of.</li>
     *     <li><b>TYPE_NAME</b> String => user-defined type name</li>
     *     <li><b>CLASS_NAME</b> String => Java class name, always {@link UdtValue} in the current implementation</li>
     *     <li><b>DATA_TYPE</b> int => type value defined in {@link Types}. One of {@link Types#JAVA_OBJECT},
     *     {@link Types#STRUCT}, or {@link Types#DISTINCT}. Always {@link Types#JAVA_OBJECT} in the current
     *     implementation.</li>
     *     <li><b>REMARKS</b> String => explanatory comment on the type, always empty in the current implementation</li>
     *     <li><b>BASE_TYPE</b> short => type code of the source type of a {@code DISTINCT} type or the type that
     *     implements the user-generated reference type of the {@code SELF_REFERENCING_COLUMN} of a structured type
     *     as defined in {@link Types} ({@code null} if {@code DATA_TYPE} is not {@code DISTINCT} or not
     *     {@code STRUCT} with {@code REFERENCE_GENERATION = USER_DEFINED}). Always {@code null} in the current
     *     implementation.</li>
     * </ol>
     * </p>
     *
     * @param catalog         A catalog name; must match the catalog name as it is stored in the database; {@code ""}
     *                        retrieves those without a catalog (will always return an empty set); {@code null} means
     *                        that the catalog name should not be used to narrow the search and in this case the
     *                        Cassandra cluster of the current connection is used.
     * @param schemaPattern   A schema pattern name; must match the schema name as it is stored in the database;
     *                        {@code ""} retrieves those without a schema (will always return an empty set);
     *                        {@code null} means that the schema name should not be used to narrow the search and in
     *                        this case the search is restricted to the current schema (if available).
     * @param typeNamePattern A type name pattern; must match the type name as it is stored in the database (not
     *                        case-sensitive); may be a fully qualified name.
     * @param types           A list of user-defined types ({@link Types#JAVA_OBJECT}, {@link Types#STRUCT}, or
     *                        {@link Types#DISTINCT}) to include; {@code null} returns all types. All the UDTs defined
     *                        in a Cassandra database are considered as {@link Types#JAVA_OBJECT}, so other values will
     *                        return an empty result set.
     * @return {@link ResultSet} object in which each row describes a UDT.
     * @throws SQLException if a database access error occurs.
     */
    @Override
    public ResultSet getUDTs(final String catalog, final String schemaPattern, final String typeNamePattern,
                             final int[] types) throws SQLException {
        checkStatementClosed();
        if (catalog == null || catalog.equals(this.connection.getCatalog())) {
            this.statement.connection = connection;
            String schemaNamePattern = schemaPattern;
            if (schemaPattern == null) {
                schemaNamePattern = this.connection.getSchema(); // limit to current schema if defined.
            }
            return MetadataResultSets.INSTANCE.makeUDTs(this.statement, schemaNamePattern, typeNamePattern, types);
        }
        return CassandraResultSet.EMPTY_RESULT_SET;
    }

    @Override
    public String getURL() {
        return this.connection.url;
    }

    /**
     * Retrieves the username as known to this database.
     *
     * @return The database username or an empty string if not defined.
     */
    @Override
    public String getUserName() {
        if (this.connection.username == null) {
            return StringUtils.EMPTY;
        } else {
            return this.connection.username;
        }
    }

    /**
     * Retrieves a description of a table's columns that are automatically updated when any value in a row is updated.
     * <p>
     * Cassandra database does not support any mechanism of automatic column update when any value in a row is updated,
     * so this method will throw a {@link SQLFeatureNotSupportedException}.
     * </p>
     *
     * @param catalog A catalog name; must match the catalog name as it is stored in the database; {@code ""} retrieves
     *                those without a catalog; {@code null} means that the catalog name should not be used to narrow
     *                the search.
     * @param schema  A schema name; must match the schema name as it is stored in the database; {@code ""} retrieves
     *                those without a schema; {@code null} means that the schema name should not be used to narrow the
     *                search.
     * @param table   A table name; must match the table name as it is stored in the database.
     * @return Always throw a {@link SQLFeatureNotSupportedException}.
     */
    @Override
    public ResultSet getVersionColumns(final String catalog, final String schema, final String table)
        throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public boolean insertsAreDetected(final int type) {
        return false;
    }

    @Override
    public boolean isCatalogAtStart() {
        return false;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public boolean locatorsUpdateCopy() {
        return false;
    }

    @Override
    public boolean nullPlusNonNullIsNull() {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart() {
        return true;
    }

    @Override
    public boolean nullsAreSortedHigh() {
        return true;
    }

    @Override
    public boolean nullsAreSortedLow() {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(final int type) {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(final int type) {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(final int type) {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(final int type) {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(final int type) {
        return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(final int type) {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() {
        return true;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() {
        return true;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() {
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() {
        return true;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() {
        return true;
    }

    @Override
    public boolean supportsBatchUpdates() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing() {
        return true;
    }

    @Override
    public boolean supportsConvert() {
        return false;
    }

    @Override
    public boolean supportsConvert(final int fromType, final int toType) {
        return false;
    }

    @Override
    public boolean supportsCoreSQLGrammar() {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() {
        return false;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() {
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() {
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() {
        return false;
    }

    @Override
    public boolean supportsFullOuterJoins() {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() {
        return false;
    }

    @Override
    public boolean supportsGroupBy() {
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() {
        return false;
    }

    @Override
    public boolean supportsGroupByUnrelated() {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() {
        return false;
    }

    @Override
    public boolean supportsLikeEscapeClause() {
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() {
        return false;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() {
        return true;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() {
        return true;
    }

    @Override
    public boolean supportsMultipleOpenResults() {
        return false;
    }

    @Override
    public boolean supportsMultipleResultSets() {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() {
        return false;
    }

    @Override
    public boolean supportsOrderByUnrelated() {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() {
        return false;
    }

    @Override
    public boolean supportsResultSetConcurrency(final int type, final int concurrency) {
        return false;
    }

    @Override
    public boolean supportsResultSetHoldability(final int holdability) {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT == holdability;
    }

    @Override
    public boolean supportsResultSetType(final int type) {
        return ResultSet.TYPE_FORWARD_ONLY == type;
    }

    @Override
    public boolean supportsSavepoints() {
        return false;
    }

    @Override
    public boolean supportsSchemasInDataManipulation() {
        return true;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() {
        return true;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() {
        return true;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() {
        return true;
    }

    @Override
    public boolean supportsSelectForUpdate() {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() {
        return false;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(final int level) {
        return Connection.TRANSACTION_NONE == level;
    }

    @Override
    public boolean supportsTransactions() {
        return false;
    }

    @Override
    public boolean supportsUnion() {
        return false;
    }

    @Override
    public boolean supportsUnionAll() {
        return false;
    }

    @Override
    public boolean updatesAreDetected(final int type) {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() {
        return false;
    }

    @Override
    public boolean usesLocalFiles() {
        return false;
    }

    private void checkStatementClosed() throws SQLException {
        if (this.statement.isClosed()) {
            this.statement = new CassandraStatement(this.connection);
        }
    }
}
