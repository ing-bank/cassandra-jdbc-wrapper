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
import java.nio.file.Paths;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.sql.Types;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import static com.ing.data.cassandra.jdbc.commands.SpecialCommandsUtil.LOG;
import static com.ing.data.cassandra.jdbc.commands.SpecialCommandsUtil.buildEmptyResultSet;
import static com.ing.data.cassandra.jdbc.commands.SpecialCommandsUtil.translateFilename;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.CANNOT_WRITE_CSV_FILE;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.UNSUPPORTED_COPY_OPTIONS;
import static org.apache.commons.lang3.StringUtils.EMPTY;

/**
 * Executor for copy to CSV special command.
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
public class CopyToCommandExecutor implements SpecialCommandExecutor {

    private static final int DEFAULT_FETCH_SIZE = 1000;
    private static final char DEFAULT_DECIMAL_SEPARATOR = '.';
    private static final String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ssZ";
    private static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
    private static final String DEFAULT_NULL_FORMAT = "null";
    private static final char DEFAULT_QUOTE_CHAR = '"';
    private static final char DEFAULT_DELIMITER_CHAR = ',';
    private static final char DEFAULT_ESCAPE_CHAR = '\\';

    // Supported options
    private static final String OPTION_DECIMALSEP = "DECIMALSEP";
    private static final String OPTION_DELIMITER = "DELIMITER";
    private static final String OPTION_ESCAPE = "ESCAPE";
    private static final String OPTION_HEADER = "HEADER";
    private static final String OPTION_NULLVAL = "NULLVAL";
    private static final String OPTION_PAGESIZE = "PAGESIZE";
    private static final String OPTION_QUOTE = "QUOTE";
    private static final String OPTION_THOUSANDSSEP = "THOUSANDSSEP";
    private static final Set<String> SUPPORTED_OPTIONS = Collections.unmodifiableSet(
        new HashSet<String>() {
            {
                add(OPTION_DECIMALSEP);
                add(OPTION_DELIMITER);
                add(OPTION_ESCAPE);
                add(OPTION_HEADER);
                add(OPTION_NULLVAL);
                add(OPTION_PAGESIZE);
                add(OPTION_QUOTE);
                add(OPTION_THOUSANDSSEP);
            }
        }
    );

    private final String tableName;
    private final String columns;
    private final String target;
    private final Properties options;

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
        checkOptions();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet execute(final CassandraStatement statement, final String cql) throws SQLException {
        final CassandraConnection connection = statement.getCassandraConnection();
        final Statement selectStatement = connection.createStatement();
        selectStatement.setFetchSize(parsePageSizeFromOptions());
        final java.sql.ResultSet rs = selectStatement.executeQuery(
            String.format("SELECT %s FROM %s", this.columns, this.tableName)
        );

        ICSVWriter csvWriter = null;
        try {
            final CSVWriterBuilder builder = new CSVWriterBuilder(
                new OutputStreamWriter(
                    Files.newOutputStream(Paths.get(translateFilename(this.target))),
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
            csvWriter.writeAll(rs, includeHeaders);
        } catch (final IOException e) {
            throw new SQLException(String.format(CANNOT_WRITE_CSV_FILE, this.target, e), e);
        } finally {
            rs.close();
            selectStatement.close();
            IOUtils.closeQuietly(csvWriter);
        }

        return buildEmptyResultSet();
    }

    private void checkOptions() throws SQLSyntaxErrorException {
        // Remove the supported options from the set of options found in the command, if the result set is not empty,
        // this means there are unsupported options.
        final Set<String> invalidKeys = new HashSet<>(this.options.stringPropertyNames());
        invalidKeys.removeAll(SUPPORTED_OPTIONS);
        if (!invalidKeys.isEmpty()) {
            throw new SQLSyntaxErrorException(String.format(UNSUPPORTED_COPY_OPTIONS, invalidKeys));
        }
    }

    private int parsePageSizeFromOptions() {
        int configuredPageSize = DEFAULT_FETCH_SIZE;
        final String pageSizeOption = this.options.getProperty(OPTION_PAGESIZE);
        if (pageSizeOption != null) {
            try {
                configuredPageSize = Integer.parseInt(pageSizeOption);
            } catch (final NumberFormatException e) {
                LOG.warn("Invalid value for option PAGESIZE: {}. Will use the default value: {}.",
                    pageSizeOption, DEFAULT_FETCH_SIZE);
            }
        }
        return configuredPageSize;
    }

    private char getOptionValueAsChar(final String optionName, final char defaultValue) {
        final String optionValue = this.options.getProperty(optionName);
        if (StringUtils.isNotEmpty(optionValue)) {
            return optionValue.charAt(0);
        }
        return defaultValue;
    }

    private ResultSetHelperService configureResultSetHelperService() {
        final ResultSetHelperService rsHelperService = new EnhancedResultSetHelperService();

        rsHelperService.setDateFormat(DEFAULT_DATE_FORMAT);
        rsHelperService.setDateTimeFormat(DEFAULT_DATETIME_FORMAT);

        final char thousandsSeparator = getOptionValueAsChar(OPTION_THOUSANDSSEP, Character.MIN_VALUE);
        final DecimalFormatSymbols decimalSymbols = new DecimalFormatSymbols(Locale.getDefault());
        decimalSymbols.setDecimalSeparator(getOptionValueAsChar(OPTION_DECIMALSEP, DEFAULT_DECIMAL_SEPARATOR));
        if (thousandsSeparator != Character.MIN_VALUE) {
            decimalSymbols.setGroupingSeparator(thousandsSeparator);
        }

        final DecimalFormat decimalFormat = (DecimalFormat) NumberFormat.getNumberInstance();
        decimalFormat.setGroupingUsed(thousandsSeparator != Character.MIN_VALUE);
        decimalFormat.setDecimalFormatSymbols(decimalSymbols);
        rsHelperService.setFloatingPointFormat(decimalFormat);
        rsHelperService.setIntegerFormat(decimalFormat);

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
                valueArray[i - 1] = getColumnValue(rs, metadata.getColumnType(i), i,
                    trim, dateFormatString, timeFormatString);
            }
            return valueArray;
        }

        private String applyFormatter(final NumberFormat formatter, final Number value) {
            // Keep the same logic as the parent class here.
            if (formatter != null && value != null) {
                return formatter.format(value);
            }
            return Objects.toString(value, EMPTY);
        }

        private String getColumnValue(final java.sql.ResultSet rs, final int colType, final int colIndex,
                                      final boolean trim, final String dateFormatString,
                                      final String timestampFormatString) throws SQLException, IOException {
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
                case Types.BIGINT:
                    value = applyFormatter(this.integerFormat, rs.getBigDecimal(colIndex));
                    break;
                case Types.DECIMAL:
                case Types.REAL:
                case Types.NUMERIC:
                    value = applyFormatter(this.floatingPointFormat, rs.getBigDecimal(colIndex));
                    break;
                case Types.DOUBLE:
                    value = applyFormatter(this.floatingPointFormat, rs.getDouble(colIndex));
                    break;
                case Types.FLOAT:
                    value = applyFormatter(this.floatingPointFormat, rs.getFloat(colIndex));
                    break;
                case Types.INTEGER:
                case Types.TINYINT:
                case Types.SMALLINT:
                    value = applyFormatter(this.integerFormat, rs.getInt(colIndex));
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
                default:
                    // This takes care of Types.BIT, Types.JAVA_OBJECT, and anything unknown.
                    value = Objects.toString(rs.getObject(colIndex), EMPTY);
            }

            if (rs.wasNull() || value == null) {
                value = this.nullFormat;
            }

            return value;
        }
    }

}
