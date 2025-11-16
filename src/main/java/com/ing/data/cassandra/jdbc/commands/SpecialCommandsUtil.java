package com.ing.data.cassandra.jdbc.commands;

import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.internal.core.cql.DefaultColumnDefinition;
import com.datastax.oss.driver.internal.core.cql.DefaultColumnDefinitions;
import com.datastax.oss.driver.internal.core.cql.DefaultRow;
import com.ing.data.cassandra.jdbc.ColumnDefinitions;
import jakarta.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.nio.ByteBuffer;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.ing.data.cassandra.jdbc.utils.DriverUtil.SINGLE_QUOTE;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.MISSING_SOURCE_FILENAME;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.unwrap;

/**
 * Utility methods for execution of
 * <a href="https://cassandra.apache.org/doc/stable/cassandra/tools/cqlsh.html#special-commands">special CQL
 * commands</a>.
 */
@Slf4j
public final class SpecialCommandsUtil {

    // Regex for CQL identifiers such as table or keyspace name is specified in the Cassandra documentation here:
    // https://cassandra.apache.org/doc/5.0/cassandra/developing/cql/ddl.html#common-definitions
    // NB: \\w is equivalent to [a-zA-Z_0-9]
    static final String CQL_IDENTIFIER_PATTERN = "\"?\\w{1,48}\"?";

    static final String CMD_CONSISTENCY_PATTERN = "(?<consistencyCmd>CONSISTENCY(?<consistencyLvl>\\s+\\w+)?)";
    static final String CMD_SERIAL_CONSISTENCY_PATTERN =
        "(?<serialConsistencyCmd>SERIAL CONSISTENCY(?<serialConsistencyLvl>\\s+\\w+)?)";
    static final String CMD_SOURCE_PATTERN = "(?<sourceCmd>SOURCE(?<sourceFile>\\s+'[^']*')?)";

    static final String FIRST_OPTION_PATTERN =
        "\\s+WITH\\s+(?<firstOptName>[A-Z]+)\\s*=\\s*(?<firstOptValue>'[^']*'|[^\\s']+)";
    static final String ADDITIONAL_OPTIONS_PATTERN =
        "\\s+AND\\s+(?<otherOptName>[A-Z]+)\\s*=\\s*(?<otherOptValue>'[^']*'|[^\\s']+)";
    static final String CMD_COPY_PATTERN =
        "(?<copyCmd>COPY(?<tableName>\\s+(" + CQL_IDENTIFIER_PATTERN + "\\.)?" + CQL_IDENTIFIER_PATTERN + ")"
            + "(\\s*\\((?<columns>\"?\\w+\"?(,\\s*\"?\\w+\"?)*)\\))?"
            + "\\s+(?<copyDirection>TO|FROM)\\s+(?<copyTargetOrOrigin>'[^']*')"
            + "(" + FIRST_OPTION_PATTERN + "(" + ADDITIONAL_OPTIONS_PATTERN + ")*)?)";

    private static final Pattern SUPPORTED_COMMANDS_PATTERN = Pattern.compile(
        CMD_CONSISTENCY_PATTERN
            + "|" + CMD_SERIAL_CONSISTENCY_PATTERN
            + "|" + CMD_SOURCE_PATTERN
            + "|" + CMD_COPY_PATTERN,
        Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);

    private SpecialCommandsUtil() {
        // Private constructor to hide the public one.
    }

    /**
     * Checks whether the CQL statement contains at least one special command (supported by this JDBC driver)
     * to execute.
     *
     * @param cql The CQL statement.
     * @return {@code true} if the given statement contains at least one special command to execute, {@code false}
     * otherwise.
     */
    public static boolean containsSpecialCommands(final String cql) {
        return SUPPORTED_COMMANDS_PATTERN.matcher(cql).find();
    }

    /**
     * Gets the appropriate executor for special command.
     *
     * @param cql The CQL statement.
     * @return The special command executor instance or {@code null} if the statement is not a special command or not
     * supported.
     * @throws SQLSyntaxErrorException if an error occurs while parsing the special command.
     */
    public static SpecialCommandExecutor getCommandExecutor(final String cql)
        throws SQLSyntaxErrorException {
        final String trimmedCql = cql.trim();
        final Matcher matcher = SUPPORTED_COMMANDS_PATTERN.matcher(trimmedCql);
        if (!matcher.matches()) {
            log.trace("CQL statement is not a supported special command: {}", cql);
            return null;
        } else {
            return handleConsistencyLevelCommand(matcher)
                .orElse(handleSerialConsistencyLevelCommand(matcher)
                    .orElse(handleSourceCommand(matcher)
                        .orElse(handleCopyCommand(matcher, trimmedCql)
                            .orElse(new NoOpExecutor()))));
        }
    }

    /**
     * Builds an empty result set.
     *
     * @return The empty result set.
     */
    public static ResultSet buildEmptyResultSet() {
        return buildSpecialCommandResultSet(new ColumnDefinitions.Definition[]{}, List.of());
    }

    /**
     * Builds a result set returned by a special command.
     *
     * @param colDefinitions The list of columns to include in the result set.
     * @param rows           The list of rows to include in the result set. Each row is a list of {@link ByteBuffer}
     *                       representation of the data, each item of the list being the value of the n-th column.
     * @return The result set.
     */
    public static ResultSet buildSpecialCommandResultSet(final ColumnDefinitions.Definition[] colDefinitions,
                                                         final List<List<ByteBuffer>> rows) {
        // Build columns definitions.
        final List<ColumnDefinition> columnDefinitions = new ArrayList<>();
        for (int i = 0; i < colDefinitions.length; i++) {
            columnDefinitions.add(new DefaultColumnDefinition(colDefinitions[i].toColumnSpec(i), AttachmentPoint.NONE));
        }
        final com.datastax.oss.driver.api.core.cql.ColumnDefinitions rsColumns =
            DefaultColumnDefinitions.valueOf(columnDefinitions);

        // Populate rows.
        final List<Row> rsRows = rows.stream()
            .map(rowData -> new DefaultRow(rsColumns, rowData))
            .collect(toList());

        return new ResultSet() {
            @Override
            public boolean wasApplied() {
                return true;
            }

            @Nonnull
            @Override
            public com.datastax.oss.driver.api.core.cql.ColumnDefinitions getColumnDefinitions() {
                return rsColumns;
            }

            @Nonnull
            @Override
            public List<ExecutionInfo> getExecutionInfos() {
                return new ArrayList<>();
            }

            @Override
            public boolean isFullyFetched() {
                return true;
            }

            @Override
            public int getAvailableWithoutFetching() {
                return 0;
            }

            @Nonnull
            @Override
            public Iterator<Row> iterator() {
                return rsRows.iterator();
            }
        };
    }

    /**
     * Translates a specified filename to support the tilde shorthand notation (for home directory) and to re-build the
     * absolute path of a filename relative to the current directory.
     *
     * @param originalFilename The original filename.
     * @return The translated filename or the original one if it does neither use the tilde shorthand notation nor is
     * relative.
     */
    public static String translateFilename(final String originalFilename) {
        String enhancedFilename = originalFilename;
        if (originalFilename.startsWith("~")) {
            enhancedFilename = originalFilename.replace("~", System.getProperty("user.home"));
        } else if (!new File(originalFilename).isAbsolute()) {
            enhancedFilename = System.getProperty("user.dir") + File.separator + originalFilename;
        }
        return enhancedFilename;
    }

    private static Optional<SpecialCommandExecutor> handleConsistencyLevelCommand(final Matcher matcher) {
        // If the 'consistencyCmd' matching group is not null, this means the command matched
        // CMD_CONSISTENCY_PATTERN, and if the 'consistencyLvl' matching group is not null, this means the command
        // specifies a consistency level to set.
        String levelParameter = null;
        if (matcher.group("consistencyCmd") != null) {
            final String consistencyLevelValue = matcher.group("consistencyLvl");
            if (consistencyLevelValue != null) {
                levelParameter = consistencyLevelValue.trim();
            }
            return Optional.of(new ConsistencyLevelExecutor(levelParameter));
        }
        return Optional.empty();
    }

    private static Optional<SpecialCommandExecutor> handleSerialConsistencyLevelCommand(final Matcher matcher) {
        // If the 'serialConsistencyCmd' matching group is not null, this means the command matched
        // CMD_SERIAL_CONSISTENCY_PATTERN, and if the 'serialConsistencyLvl' matching group is not null, this means
        // the command specifies a serial consistency level to set.
        String levelParameter = null;
        if (matcher.group("serialConsistencyCmd") != null) {
            final String consistencyLevelValue = matcher.group("serialConsistencyLvl");
            if (consistencyLevelValue != null) {
                levelParameter = consistencyLevelValue.trim();
            }
            return Optional.of(new SerialConsistencyLevelExecutor(levelParameter));
        }
        return Optional.empty();
    }

    private static Optional<SpecialCommandExecutor> handleSourceCommand(final Matcher matcher)
        throws SQLSyntaxErrorException {
        // If the 'sourceCmd' and 'sourceFile' matching group are not null, this means the command matched
        // CMD_SOURCE_PATTERN with a file name.
        if (matcher.group("sourceCmd") != null) {
            final String sourceFile = matcher.group("sourceFile");
            if (sourceFile != null) {
                return Optional.of(new SourceCommandExecutor(unwrap(sourceFile.trim(), SINGLE_QUOTE)));
            } else {
                throw new SQLSyntaxErrorException(MISSING_SOURCE_FILENAME);
            }
        }
        return Optional.empty();
    }

    private static Optional<SpecialCommandExecutor> handleCopyCommand(final Matcher matcher, final String cql)
        throws SQLSyntaxErrorException {
        // If the 'copyCmd' matching group is not null, this means the command matched CMD_COPY_PATTERN.
        if (matcher.group("copyCmd") != null) {
            final String tableName = matcher.group("tableName").trim();
            final String columns = Optional.ofNullable(matcher.group("columns")).orElse(EMPTY).trim();
            final String targetOrOrigin = unwrap(matcher.group("copyTargetOrOrigin"), SINGLE_QUOTE);

            // Extract options from the command if at least one is present.
            final Properties options = new Properties();
            final String firstOptionName = matcher.group("firstOptName");
            if (firstOptionName != null) {
                final String firstOptionValue = unwrap(matcher.group("firstOptValue"), SINGLE_QUOTE);
                options.put(firstOptionName, firstOptionValue);

                // Extract additional options from the repeating group, this requires to re-apply a new matcher only
                // using the additional options pattern to the whole CQL statement.
                final Matcher additionalOptionsMatcher = Pattern.compile(ADDITIONAL_OPTIONS_PATTERN).matcher(cql);
                while (additionalOptionsMatcher.find()) {
                    final String optionName = additionalOptionsMatcher.group("otherOptName");
                    final String optionValue = unwrap(additionalOptionsMatcher.group("otherOptValue"), SINGLE_QUOTE);
                    options.put(optionName, optionValue);
                }
            }

            if ("TO".equalsIgnoreCase(matcher.group("copyDirection"))) {
                return Optional.of(new CopyToCommandExecutor(tableName, columns, targetOrOrigin, options));
            } else {
                return Optional.of(new CopyFromCommandExecutor(tableName, columns, targetOrOrigin, options));
            }
        }
        return Optional.empty();
    }

}
