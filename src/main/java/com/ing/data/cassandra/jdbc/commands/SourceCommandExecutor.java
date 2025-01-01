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
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;

import static com.ing.data.cassandra.jdbc.commands.SpecialCommandsUtil.buildEmptyResultSet;
import static com.ing.data.cassandra.jdbc.commands.SpecialCommandsUtil.translateFilename;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.CANNOT_OPEN_SOURCE_FILE;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.MISSING_SOURCE_FILENAME;

/**
 * Executor for source special command.
 * <p>
 *     {@code SOURCE <filename>}: where {@code filename} is the path of the file containing the CQL statements to
 *     execute. If not absolute, the path is interpreted relative to the current working directory. The tilde shorthand
 *     notation ({@code '~/dir'}) is supported for referring to the home directory.
 * </p>
 * <p>
 *     The documentation of the original {@code SOURCE} command is available:
 *     <ul>
 *         <li><a href="https://cassandra.apache.org/doc/latest/cassandra/managing/tools/cqlsh.html#source">
 *             in the Apache Cassandra® documentation</a></li>
 *         <li><a href="https://docs.datastax.com/en/cql-oss/3.3/cql/cql_reference/cqlshSource.html">
 *             in the DataStax CQL reference documentation</a></li>
 *     </ul>
 * </p>
 * @implNote <p>
 *     If some of CQL statements included in the executed script return a result set or warnings, both will be
 *     ignored. In this implementation, the {@code SOURCE} command always returns an empty result set without
 *     warnings.
 * </p>
 * <p>
 *     Contrary to the original {@code SOURCE} command, any error in the CQL script will abort the execution.
 * </p>
 */
public class SourceCommandExecutor implements SpecialCommandExecutor {

    private final String filename;

    /**
     * Constructor.
     *
     * @param filename The parameter {@code filename} of the command.
     */
    public SourceCommandExecutor(@Nonnull final String filename) {
        this.filename = filename;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet execute(final CassandraStatement statement, final String cql) throws SQLException {
        if (StringUtils.isBlank(this.filename)) {
            throw new SQLSyntaxErrorException(MISSING_SOURCE_FILENAME);
        }

        final CassandraConnection connection = (CassandraConnection) statement.getConnection();
        final File sourceFile = new File(translateFilename(this.filename));
        final String cqlStatement;
        try {
            cqlStatement = FileUtils.readFileToString(sourceFile, StandardCharsets.UTF_8);
        } catch (final IOException e) {
            throw new SQLException(String.format(CANNOT_OPEN_SOURCE_FILE, this.filename, e), e);
        }
        connection.createStatement().execute(cqlStatement);
        return buildEmptyResultSet();
    }

}
