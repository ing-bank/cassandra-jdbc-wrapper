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

package com.ing.data.cassandra.jdbc.commands;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.ing.data.cassandra.jdbc.CassandraConnection;
import com.ing.data.cassandra.jdbc.CassandraStatement;
import com.opencsv.CSVWriterBuilder;
import com.opencsv.ICSVWriter;
import com.opencsv.ResultSetHelperService;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Stream;

import static com.ing.data.cassandra.jdbc.commands.SpecialCommandsUtil.LOG;
import static com.ing.data.cassandra.jdbc.commands.SpecialCommandsUtil.translateFilename;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.CANNOT_WRITE_CSV_FILE;
import static com.ing.data.cassandra.jdbc.utils.WarningConstants.COUNTING_EXPORTED_ROWS_FAILED;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.time.TimeZones.GMT;

/**
 * Executor for special command copying table content to a CSV file.
 * <p>
 *     {@code COPY <tableName>[(<columns>)] TO <target>[ WITH <options>[ AND <options> ...]]}: where {@code tableName}
 *     is the name of the table to copy (it may be prefixed with the keyspace name), {@code columns} is a subset of
 *     columns to copy specified by adding a comma-separated list of column names, {@code target} is a string literal
 *     (with single quotes) representing the path to the destination file; and {@code options} are the options among
 *     the following:
 *     <ul>
 *         <li>{@code DECIMALSEP}: the character that is used as the decimal point separator.
 *         Defaults to {@value #DEFAULT_DECIMAL_SEPARATOR}.</li>
 *         <li>{@code DELIMITER}: the character that is used to separate fields.
 *         Defaults to {@value #DEFAULT_DELIMITER_CHAR}.</li>
 *         <li>{@code ESCAPE}: the character that is used to escape the literal uses of the {@code QUOTE} character.
 *         Defaults to {@value #DEFAULT_ESCAPE_CHAR}.</li>
 *         <li>{@code HEADER}: whether the first line in the CSV output file will contain the column names.
 *         Defaults to {@code false}.</li>
 *         <li>{@code NULLVAL}: the string placeholder for null values. Defaults to {@value #DEFAULT_NULL_FORMAT}.</li>
 *         <li>{@code PAGESIZE}: the number of rows to fetch in a single page.
 *         Defaults to {@value #DEFAULT_FETCH_SIZE} if not specified or if the given value is not a valid integer.</li>
 *         <li>{@code QUOTE}: the character that is used to enclose field values.
 *         Defaults to {@value #DEFAULT_QUOTE_CHAR}.</li>
 *         <li>{@code THOUSANDSSEP}: the character that is used to separate thousands.
 *         Defaults to the empty string.</li>
 *     </ul>
 * </p>
 * <p>
 *     If the path to the destination file is not absolute, it is interpreted relative to the current working
 *     directory. The tilde shorthand notation ({@code '~/dir'}) is supported for referring to the home directory.
 * </p>
 * <p>
 *     The format to define the value of an option is: {@code <optionName> = <value>}. The value must be quoted if it
 *     is a string literal.
 * </p>
 * <p>
 *     The following options are not supported:
 *     <ul>
 *         <li>{@code BEGINTOKEN}</li>
 *         <li>{@code BOOLSTYLE}</li>
 *         <li>{@code CONFIGFILE}</li>
 *         <li>{@code DATETIMEFORMAT}</li>
 *         <li>{@code ENCODING}</li>
 *         <li>{@code ENDTOKEN}</li>
 *         <li>{@code MAXATTEMPTS}</li>
 *         <li>{@code MAXOUTPUTSIZE}</li>
 *         <li>{@code MAXREQUESTS}</li>
 *         <li>{@code NULL} (use {@code NULLVAL} instead)</li>
 *         <li>{@code NUMPROCESSES}</li>
 *         <li>{@code PAGETIMEOUT}</li>
 *         <li>{@code RATEFILE}</li>
 *         <li>{@code REPORTFREQUENCY}</li>
 *     </ul>
 *     Using unknown options will throw a {@link SQLSyntaxErrorException}.
 * </p>
 * <p>
 *     A successful command execution will return a result set with a single row containing some information about the
 *     export process in a column {@code result}.
 * </p>
 * <p>
 *     The documentation of the original {@code COPY TO} command is available:
 *     <ul>
 *         <li><a href="https://cassandra.apache.org/doc/latest/cassandra/managing/tools/cqlsh.html#copy-to">
 *             in the Apache Cassandra® documentation</a></li>
 *         <li><a href="https://docs.datastax.com/en/cql-oss/3.3/cql/cql_reference/cqlshCopy.html">
 *             in the DataStax CQL reference documentation</a></li>
 *     </ul>
 * </p>
 * @implNote <p>
 *     The used encoding is always UTF-8.
 * </p>
 * <p>
 *     The special value {@code STDOUT} for the {@code target} parameter, used to print the CSV to the standard output,
 *     is not supported.
 * </p>
 */
public class CopyToCommandExecutor extends AbstractCopyCommandExecutor {

    private static final int DEFAULT_FETCH_SIZE = 1000;

    // Supported options (not in common with COPY FROM)
    private static final String OPTION_PAGESIZE = "PAGESIZE";

    private final String target;

    /**
     * Constructor.
     *
     * @param tableName The parameter {@code tableName} of the command.
     * @param columns   The optional parameter {@code columns} of the command.
     * @param target    The parameter {@code target} of the command.
     * @param options   The optional parameter {@code options} of the command already parsed into a {@link Properties}
     *                  instance.
     * @throws SQLSyntaxErrorException if an unknown option is used.
     */
    public CopyToCommandExecutor(@Nonnull final String tableName, final String columns, @Nonnull final String target,
                                 @Nonnull final Properties options) throws SQLSyntaxErrorException {
        this.tableName = tableName;
        this.columns = StringUtils.defaultIfBlank(columns, "*");
        this.target = target;
        this.options = options;

        SUPPORTED_OPTIONS.add(OPTION_PAGESIZE);
        checkOptions();

        configureFormatters();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet execute(final CassandraStatement statement, final String cql) throws SQLException {
        final CassandraConnection connection = statement.getCassandraConnection();
        final Statement selectStatement = connection.createStatement();
        selectStatement.setFetchSize(getOptionValueAsInt(OPTION_PAGESIZE, DEFAULT_FETCH_SIZE));
        final java.sql.ResultSet rs = selectStatement.executeQuery(
            String.format("SELECT %s FROM %s", this.columns, this.tableName)
        );

        final Path targetPath = Paths.get(translateFilename(this.target));
        ICSVWriter csvWriter = null;
        long exportedRows = 0;
        try {
            final CSVWriterBuilder builder = new CSVWriterBuilder(
                new OutputStreamWriter(
                    Files.newOutputStream(targetPath),
                    StandardCharsets.UTF_8.newEncoder() // Use UTF-8 encoding by default
                )
            );

            csvWriter = builder
                .withResultSetHelper(configureResultSetHelperService())
                .withQuoteChar(getOptionValueAsChar(OPTION_QUOTE, DEFAULT_QUOTE_CHAR))
                .withSeparator(getOptionValueAsChar(OPTION_DELIMITER, DEFAULT_DELIMITER_CHAR))
                .withEscapeChar(getOptionValueAsChar(OPTION_ESCAPE, DEFAULT_ESCAPE_CHAR))
                .build();

            final boolean includeHeaders = Boolean.parseBoolean(this.options.getProperty(OPTION_HEADER));
            if (includeHeaders) {
                exportedRows -= 1;
            }
            csvWriter.writeAll(rs, includeHeaders);
        } catch (final IOException e) {
            throw new SQLException(String.format(CANNOT_WRITE_CSV_FILE, this.target, e.getMessage()), e);
        } finally {
            rs.close();
            selectStatement.close();
            IOUtils.closeQuietly(csvWriter);
        }

        try (Stream<String> csvLines = Files.lines(targetPath)) {
            exportedRows += csvLines.count();
        } catch (final IOException e) {
            LOG.warn(COUNTING_EXPORTED_ROWS_FAILED);
        }
        return buildCopyCommandResultSet("exported to", exportedRows, 1, -1);
    }

    private ResultSetHelperService configureResultSetHelperService() {
        final EnhancedResultSetHelperService rsHelperService = new EnhancedResultSetHelperService();
        rsHelperService.setDateFormat(this.dateFormat);
        rsHelperService.setDateTimeFormat(this.dateTimeFormat);
        rsHelperService.setFloatingPointFormat(this.decimalFormat);
        rsHelperService.setIntegerFormat(this.decimalFormat);
        rsHelperService.setNullFormat(getOptionValueAsString(OPTION_NULLVAL, DEFAULT_NULL_FORMAT));
        return rsHelperService;
    }

    private static final class EnhancedResultSetHelperService extends ResultSetHelperService {
        private String nullFormat = DEFAULT_NULL_FORMAT;

        /**
         * Set a default format for {@code null} values that will be used by the service.
         *
         * @param nullFormat The desired format for {@code null} values.
         */
        public void setNullFormat(final String nullFormat) {
            this.nullFormat = nullFormat;
        }

        @Override
        public String[] getColumnValues(final java.sql.ResultSet rs, final boolean trim,
                                        final String dateFormatString, final String timeFormatString)
            throws SQLException, IOException {
            // Keep the same logic as the parent class, but use the enhanced getColumnValue method.
            final ResultSetMetaData metadata = rs.getMetaData();
            final String[] valueArray = new String[metadata.getColumnCount()];
            for (int i = 1; i <= metadata.getColumnCount(); i++) {
                valueArray[i - 1] = getColumnValueFromResultSet(rs, metadata.getColumnType(i), i,
                    trim, dateFormatString, timeFormatString);
            }
            return valueArray;
        }

        private String applyNumberFormatter(final NumberFormat formatter, final Number value) {
            // Keep the same logic as in the parent class here.
            if (formatter != null && value != null) {
                return formatter.format(value);
            }
            return Objects.toString(value, EMPTY);
        }

        @Override
        protected String handleTimestamp(final Timestamp timestamp, final String timestampFormatString) {
            if (timestamp == null) {
                return null;
            }
            final SimpleDateFormat timeFormat = new SimpleDateFormat(timestampFormatString);
            // Use GMT time zone for the timestamp values to get consistent and machine-independent results.
            timeFormat.setTimeZone(GMT);
            return timeFormat.format(timestamp);
        }

        private String getColumnValueFromResultSet(final java.sql.ResultSet rs, final int colType, final int colIndex,
                                                   final boolean trim, final String dateFormatString,
                                                   final String timestampFormatString)
            throws SQLException, IOException {
            String value;

            switch (colType) {
                case Types.BOOLEAN:
                    value = Objects.toString(rs.getBoolean(colIndex));
                    break;
                case Types.NCLOB:
                    value = handleNClob(rs, colIndex);
                    break;
                case Types.CLOB:
                    value = handleClob(rs, colIndex);
                    break;
                case Types.DECIMAL:
                case Types.REAL:
                case Types.NUMERIC:
                    value = applyNumberFormatter(this.floatingPointFormat, rs.getBigDecimal(colIndex));
                    break;
                case Types.DOUBLE:
                    value = applyNumberFormatter(this.floatingPointFormat, rs.getDouble(colIndex));
                    break;
                case Types.FLOAT:
                    value = applyNumberFormatter(this.floatingPointFormat, rs.getFloat(colIndex));
                    break;
                case Types.BIGINT:
                    value = applyNumberFormatter(this.integerFormat, rs.getLong(colIndex));
                    break;
                case Types.INTEGER:
                    value = applyNumberFormatter(this.integerFormat, rs.getInt(colIndex));
                    break;
                case Types.TINYINT:
                    value = applyNumberFormatter(this.integerFormat, rs.getByte(colIndex));
                    break;
                case Types.SMALLINT:
                    value = applyNumberFormatter(this.integerFormat, rs.getShort(colIndex));
                    break;
                case Types.DATE:
                    value = handleDate(rs, colIndex, dateFormatString);
                    break;
                case Types.TIME:
                    value = Objects.toString(rs.getTime(colIndex), EMPTY);
                    break;
                case Types.TIMESTAMP:
                    value = handleTimestamp(rs.getTimestamp(colIndex), timestampFormatString);
                    break;
                case Types.NVARCHAR:
                case Types.NCHAR:
                case Types.LONGNVARCHAR:
                    value = handleNVarChar(rs, colIndex, trim);
                    break;
                case Types.LONGVARCHAR:
                case Types.VARCHAR:
                case Types.CHAR:
                    value = handleVarChar(rs, colIndex, trim);
                    break;
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    value = new String(rs.getBytes(colIndex), StandardCharsets.UTF_8);
                    break;
                default:
                    // This takes care of any types not previously handled.
                    // For tuples, use the method "getFormattedContents()" to get the string representation of the
                    // tuple.
                    final Object objValue = rs.getObject(colIndex);
                    if (TupleValue.class.isAssignableFrom(objValue.getClass())) {
                        return ((TupleValue) objValue).getFormattedContents();
                    }
                    value = Objects.toString(objValue, EMPTY);
            }

            if (rs.wasNull() || value == null) {
                value = this.nullFormat;
            }

            return value;
        }
    }

}
