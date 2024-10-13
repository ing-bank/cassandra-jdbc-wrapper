package com.ing.data.cassandra.jdbc.utils;

import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.internal.core.cql.DefaultColumnDefinition;
import com.datastax.oss.driver.internal.core.cql.DefaultColumnDefinitions;
import com.datastax.oss.driver.internal.core.cql.DefaultRow;
import com.ing.data.cassandra.jdbc.ColumnDefinitions;
import com.ing.data.cassandra.jdbc.SpecialCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Utility methods for execution of
 * <a href="https://cassandra.apache.org/doc/stable/cassandra/tools/cqlsh.html#special-commands">special CQL
 * commands</a>.
 */
public final class SpecialCommandsUtil {

    static final Logger LOG = LoggerFactory.getLogger(SpecialCommandsUtil.class);

    static final String CMD_CONSISTENCY_PATTERN = "(?<consistencyCmd>CONSISTENCY(?<consistencyLvl> \\w+)?)";
    static final String CMD_SERIAL_CONSISTENCY_PATTERN =
        "(?<serialConsistencyCmd>SERIAL CONSISTENCY(?<serialConsistencyLvl> \\w+)?)";

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
        final Pattern pattern = Pattern.compile(CMD_CONSISTENCY_PATTERN + "|" + CMD_SERIAL_CONSISTENCY_PATTERN,
            Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
        return pattern.matcher(cql).find();
    }

    /**
     * Gets the appropriate executor for special command.
     *
     * @param cql The CQL statement.
     * @return The special command executor instance or {@code null} if the statement is not a special command or not
     * supported.
     */
    public static SpecialCommands.SpecialCommandExecutor getCommandExecutor(final String cql) {
        final Matcher matcher = Pattern.compile(CMD_CONSISTENCY_PATTERN + "|" + CMD_SERIAL_CONSISTENCY_PATTERN,
                Pattern.CASE_INSENSITIVE | Pattern.MULTILINE)
            .matcher(cql.trim());
        if (!matcher.matches()) {
            LOG.trace("CQL statement is not a supported special command: {}", cql);
            return null;
        } else {
            // If the first matching group is not null, this means the command matched CMD_CONSISTENCY_PATTERN, and if
            // the second matching group is not null, this means the command specifies a consistency level to set.
            String levelParameter = null;
            if (matcher.group("consistencyCmd") != null) {
                final String consistencyLevelValue = matcher.group("consistencyLvl");
                if (consistencyLevelValue != null) {
                    levelParameter = consistencyLevelValue.trim();
                }
                return new SpecialCommands.ConsistencyLevelExecutor(levelParameter);
            }
            // If the third matching group is not null, this means the command matched CMD_SERIAL_CONSISTENCY_PATTERN,
            // and if the fourth matching group is not null, this means the command specifies a serial consistency
            // level to set.
            if (matcher.group("serialConsistencyCmd") != null) {
                final String consistencyLevelValue = matcher.group("serialConsistencyLvl");
                if (consistencyLevelValue != null) {
                    levelParameter = consistencyLevelValue.trim();
                }
                return new SpecialCommands.SerialConsistencyLevelExecutor(levelParameter);
            }
            return new SpecialCommands.NoOpExecutor();
        }
    }

    /**
     * Builds an empty result set.
     *
     * @return The empty result set.
     */
    public static ResultSet buildEmptyResultSet() {
        return buildSpecialCommandResultSet(new ColumnDefinitions.Definition[]{}, Collections.emptyList());
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
            .collect(Collectors.toList());

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

}
