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

import com.datastax.oss.driver.api.core.metadata.Metadata;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLSyntaxErrorException;

import static com.ing.data.cassandra.jdbc.Utils.NOT_SUPPORTED;

class CassandraDatabaseMetaData implements DatabaseMetaData {
    private final CassandraConnection connection;
    private CassandraStatement statement;
    private final Metadata metadata;

    public CassandraDatabaseMetaData(final CassandraConnection connection) throws SQLException {
        this.connection = connection;
        this.statement = new CassandraStatement(this.connection);
        this.metadata = this.connection.getClusterMetadata();
    }

    @Override
    public boolean isWrapperFor(final Class<?> iface) {
        return iface.isAssignableFrom(getClass());
    }

    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) return iface.cast(this);
        throw new SQLFeatureNotSupportedException(String.format(Utils.NO_INTERFACE, iface.getSimpleName()));
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
    public boolean deletesAreDetected(final int arg0) {
        return false;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() {
        return false;
    }

    @Override
    public ResultSet getAttributes(final String arg0, final String arg1, final String arg2, final String arg3) {
        return new CassandraResultSet();
    }

    @Override
    public ResultSet getBestRowIdentifier(final String arg0, final String arg1, final String arg2, final int arg3,
                                          final boolean arg4) {
        return new CassandraResultSet();
    }

    @Override
    public String getCatalogSeparator() {
        return StringUtils.EMPTY;
    }

    @Override
    public String getCatalogTerm() {
        return "Cluster";
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        if (statement.isClosed()) {
            statement = new CassandraStatement(this.connection);
        }
        return MetadataResultSets.instance.makeCatalogs(statement);
    }

    @Override
    public ResultSet getClientInfoProperties() {
        return new CassandraResultSet();
    }

    @Override
    public ResultSet getColumnPrivileges(final String arg0, final String arg1, final String arg2, final String arg3) {
        return new CassandraResultSet();
    }

    @Override
    public ResultSet getColumns(final String catalog, String schemaPattern, final String tableNamePattern,
                                final String columnNamePattern) throws SQLException {
        if (statement.isClosed()) {
            statement = new CassandraStatement(this.connection);
        }
        if (catalog == null || connection.getCatalog().equals(catalog)) {
            statement.connection = connection;
            if (schemaPattern == null) schemaPattern = connection.getSchema(); //limit to current schema if set
            return MetadataResultSets.instance.makeColumns(statement, schemaPattern, tableNamePattern,
                columnNamePattern);
        }
        return new CassandraResultSet();
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public ResultSet getCrossReference(final String arg0, final String arg1, final String arg2, final String arg3,
                                       final String arg4, final String arg5) {
        return new CassandraResultSet();
    }

    @Override
    public int getDatabaseMajorVersion() {
        return CassandraConnection.DB_MAJOR_VERSION;
    }

    @Override
    public int getDatabaseMinorVersion() {
        return CassandraConnection.DB_MINOR_VERSION;
    }

    @Override
    public String getDatabaseProductName() {
        return CassandraConnection.DB_PRODUCT_NAME;
    }

    @Override
    public String getDatabaseProductVersion() {
        return String.format("%d.%d.%d", CassandraConnection.DB_MAJOR_VERSION, CassandraConnection.DB_MINOR_VERSION,
            CassandraConnection.DB_REVISION);
    }

    @Override
    public int getDefaultTransactionIsolation() {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public int getDriverMajorVersion() {
        return CassandraDriver.DVR_MAJOR_VERSION;
    }

    @Override
    public int getDriverMinorVersion() {
        return CassandraDriver.DVR_MINOR_VERSION;
    }

    @Override
    public String getDriverName() {
        return CassandraDriver.DVR_NAME;
    }

    @Override
    public String getDriverVersion() {
        return String.format("%d.%d.%d", CassandraDriver.DVR_MAJOR_VERSION, CassandraDriver.DVR_MINOR_VERSION,
            CassandraDriver.DVR_PATCH_VERSION);
    }

    @Override
    public ResultSet getExportedKeys(final String arg0, final String arg1, final String arg2) {
        return new CassandraResultSet();
    }

    @Override
    public String getExtraNameCharacters() {
        return StringUtils.EMPTY;
    }

    @Override
    public ResultSet getFunctionColumns(final String arg0, final String arg1, final String arg2, final String arg3) {
        return new CassandraResultSet();
    }

    @Override
    public ResultSet getFunctions(final String arg0, final String arg1, final String arg2) {
        return new CassandraResultSet();
    }

    @Override
    public String getIdentifierQuoteString() {
        return StringUtils.SPACE;
    }

    @Override
    public ResultSet getImportedKeys(final String arg0, final String arg1, final String arg2) {
        return new CassandraResultSet();
    }

    @Override
    public ResultSet getIndexInfo(final String catalog, String schema, final String table, final boolean unique,
                                  final boolean approximate) throws SQLException {
        if (statement.isClosed()) {
            statement = new CassandraStatement(this.connection);
        }
        if (catalog == null || connection.getCatalog().equals(catalog)) {
            if (schema == null) schema = connection.getSchema(); //limit to current schema if set
            return MetadataResultSets.instance.makeIndexes(statement, schema, table, unique, approximate);
        }
        return new CassandraResultSet();
    }

    @Override
    public int getJDBCMajorVersion() {
        return 4;
    }

    @Override
    public int getJDBCMinorVersion() {
        return 0;
    }

    @Override
    public int getMaxBinaryLiteralLength() {
        // Cassandra can represent a 2GB value, but CQL has to encode it in hexadecimal.
        return Integer.MAX_VALUE / 2;
    }

    @Override
    public int getMaxCatalogNameLength() {
        return Short.MAX_VALUE;
    }

    @Override
    public int getMaxCharLiteralLength() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getMaxColumnNameLength() {
        return Short.MAX_VALUE;
    }

    @Override
    public int getMaxColumnsInGroupBy() {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() {
        return 0;
    }

    @Override
    public int getMaxConnections() {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() {
        return 0;
    }

    @Override
    public int getMaxIndexLength() {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() {
        return 0;
    }

    @Override
    public int getMaxRowSize() {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() {
        return 0;
    }

    @Override
    public int getMaxStatementLength() {
        return 0;
    }

    @Override
    public int getMaxStatements() {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() {
        return 0;
    }

    @Override
    public String getNumericFunctions() {
        return StringUtils.EMPTY;
    }

    @Override
    public ResultSet getPrimaryKeys(final String catalog, String schema, final String table) throws SQLException {
        if (statement.isClosed()) {
            statement = new CassandraStatement(this.connection);
        }
        if (catalog == null || connection.getCatalog().equals(catalog)) {
            if (schema == null) {
                schema = connection.getSchema(); //limit to current schema if set
            }
            return MetadataResultSets.instance.makePrimaryKeys(statement, schema, table);
        }
        return new CassandraResultSet();
    }

    @Override
    public ResultSet getProcedureColumns(final String arg0, final String arg1, final String arg2, final String arg3) {
        return new CassandraResultSet();
    }

    @Override
    public String getProcedureTerm() {
        return StringUtils.EMPTY;
    }

    @Override
    public ResultSet getProcedures(final String arg0, final String arg1, final String arg2) {
        return new CassandraResultSet();
    }

    @Override
    public int getResultSetHoldability() {
        return CassandraResultSet.DEFAULT_HOLDABILITY;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() {
        return RowIdLifetime.ROWID_VALID_FOREVER;
    }

    @Override
    public String getSQLKeywords() {
        return StringUtils.EMPTY;
    }

    @Override
    public int getSQLStateType() {
        return sqlStateSQL;
    }

    @Override
    public String getSchemaTerm() {
        return "Column Family";
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        if (statement.isClosed()) {
            statement = new CassandraStatement(this.connection);
        }
        return MetadataResultSets.instance.makeSchemas(statement, null);
    }

    @Override
    public ResultSet getSchemas(final String catalog, final String schemaPattern) throws SQLException {
        if (statement.isClosed()) {
            statement = new CassandraStatement(this.connection);
        }
        if (!(catalog == null || catalog.equals(statement.connection.getCatalog()))) {
            throw new SQLSyntaxErrorException("Catalog name must exactly match or be null.");
        }

        return MetadataResultSets.instance.makeSchemas(statement, schemaPattern);
    }

    @Override
    public String getSearchStringEscape() {
        return "\\";
    }

    @Override
    public String getStringFunctions() {
        return StringUtils.EMPTY;
    }

    @Override
    public ResultSet getSuperTables(final String arg0, final String arg1, final String arg2) {
        return new CassandraResultSet();
    }

    @Override
    public ResultSet getSuperTypes(final String arg0, final String arg1, final String arg2) {
        return new CassandraResultSet();
    }

    @Override
    public String getSystemFunctions() {
        return StringUtils.EMPTY;
    }

    @Override
    public ResultSet getTablePrivileges(final String arg0, final String arg1, final String arg2) {
        return new CassandraResultSet();
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        if (statement.isClosed()) {
            statement = new CassandraStatement(this.connection);
        }
        return MetadataResultSets.instance.makeTableTypes(statement);
    }

    @Override
    public ResultSet getTables(final String catalog, final String schemaPattern, final String tableNamePattern,
                               final String[] types) throws SQLException {
        boolean askingForTable = (types == null);
        if (types != null) {
            for (final String t : types) {
                if (MetadataResultSets.TABLE_CONSTANT.equals(t)) {
                    askingForTable = true;
                    break;
                }
            }
        }
        if ((catalog == null || connection.getCatalog().equals(catalog)) && askingForTable) {
            if (statement.isClosed()) {
                statement = new CassandraStatement(this.connection);
            }
            return MetadataResultSets.instance.makeTables(statement, schemaPattern, tableNamePattern);
        }

        return new CassandraResultSet();
    }

    @Override
    public String getTimeDateFunctions() {
        return StringUtils.EMPTY;
    }

    @Override
    public ResultSet getTypeInfo() {
        return new CassandraResultSet();
    }

    @Override
    public ResultSet getUDTs(final String arg0, final String arg1, final String arg2, final int[] arg3) {
        return new CassandraResultSet();
    }

    @Override
    public String getURL() {
        return connection.url;
    }

    @Override
    public String getUserName() {
        if (connection.username == null) {
            return StringUtils.EMPTY;
        } else {
            return connection.username;
        }
    }

    @Override
    public ResultSet getVersionColumns(final String arg0, final String arg1, final String arg2) {
        return new CassandraResultSet();
    }

    @Override
    public boolean insertsAreDetected(final int arg0) {
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
    public boolean othersDeletesAreVisible(final int arg0) {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(final int arg0) {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(final int arg0) {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(final int arg0) {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(final int arg0) {
        return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(final int arg0) {
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
        return false;
    }

    @Override
    public boolean supportsConvert() {
        return false;
    }

    @Override
    public boolean supportsConvert(final int arg0, final int arg1) {
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
        return false;
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
    public boolean supportsResultSetConcurrency(final int arg0, final int arg1) {
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
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() {
        return false;
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
    public boolean updatesAreDetected(final int arg0) {
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

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public ResultSet getPseudoColumns(final String catalog, final String schemaPattern, final String tableNamePattern,
                                      final String columnNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }
}
