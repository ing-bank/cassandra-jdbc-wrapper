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

package com.ing.data.cassandra.jdbc.metadata;

import com.ing.data.cassandra.jdbc.CassandraDatabaseMetaData;
import org.apache.commons.lang3.StringUtils;

import java.sql.DatabaseMetaData;
import java.util.Arrays;
import java.util.List;

import static com.ing.data.cassandra.jdbc.utils.DriverUtil.CASSANDRA_5;
import static com.ing.data.cassandra.jdbc.utils.DriverUtil.buildMetadataList;

/**
 * Utility class building list of Cassandra built-in functions returned in {@link CassandraDatabaseMetaData}.
 */
public class BuiltInFunctionsMetadataBuilder {

    private final String databaseVersion;

    /**
     * Constructor.
     *
     * @param databaseVersion The database version the driver is currently connected to.
     */
    public BuiltInFunctionsMetadataBuilder(final String databaseVersion) {
        this.databaseVersion = databaseVersion;
    }

    /**
     * Builds the comma-separated list of the math functions available in this Cassandra database.
     * This method is used to implement the method {@link DatabaseMetaData#getNumericFunctions()}.
     *
     * @return A valid result for the implementation of {@link DatabaseMetaData#getNumericFunctions()}.
     */
    public String buildNumericFunctionsList() {
        final List<VersionedMetadata> numericFunctions = Arrays.asList(
            // Math functions introduced in Cassandra 5.0 (see https://issues.apache.org/jira/browse/CASSANDRA-17221)
            new BasicVersionedMetadata("abs", CASSANDRA_5),
            new BasicVersionedMetadata("exp", CASSANDRA_5),
            new BasicVersionedMetadata("log", CASSANDRA_5),
            new BasicVersionedMetadata("log10", CASSANDRA_5),
            new BasicVersionedMetadata("round", CASSANDRA_5),
            // We consider here the vectors similarity functions introduced by CEP-30 as numeric functions (see
            // https://issues.apache.org/jira/browse/CASSANDRA-18640).
            new BasicVersionedMetadata("similarity_cosine", CASSANDRA_5),
            new BasicVersionedMetadata("similarity_euclidean", CASSANDRA_5),
            new BasicVersionedMetadata("similarity_dot_product", CASSANDRA_5)
        );
        return buildMetadataList(numericFunctions, databaseVersion);
    }

    /**
     * Builds the comma-separated list of the time and date functions available in this Cassandra database.
     * This method is used to implement the method {@link DatabaseMetaData#getTimeDateFunctions()}.
     *
     * @return A valid result for the implementation of {@link DatabaseMetaData#getTimeDateFunctions()}.
     */
    public String buildTimeDateFunctionsList() {
        // See: https://cassandra.apache.org/doc/latest/cassandra/cql/functions.html
        // In Cassandra 5.0, functions named using camel cased have been renamed to use snake case
        // (see https://issues.apache.org/jira/browse/CASSANDRA-18037)
        // and deprecated functions dateOf and unixTimestampOf have been removed (see
        // https://issues.apache.org/jira/browse/CASSANDRA-18328).
        final List<VersionedMetadata> timeDateFunctions = Arrays.asList(
            new BasicVersionedMetadata("dateOf", null, CASSANDRA_5),
            new BasicVersionedMetadata("now"),
            new BasicVersionedMetadata("minTimeuuid"),
            new BasicVersionedMetadata("min_timeuuid", CASSANDRA_5),
            new BasicVersionedMetadata("maxTimeuuid"),
            new BasicVersionedMetadata("max_timeuuid", CASSANDRA_5),
            new BasicVersionedMetadata("unixTimestampOf", null, CASSANDRA_5),
            new BasicVersionedMetadata("toDate", null, CASSANDRA_5),
            new BasicVersionedMetadata("to_date", CASSANDRA_5),
            new BasicVersionedMetadata("toTimestamp", null, CASSANDRA_5),
            new BasicVersionedMetadata("to_timestamp", CASSANDRA_5),
            new BasicVersionedMetadata("toUnixTimestamp", null, CASSANDRA_5),
            new BasicVersionedMetadata("to_unix_timestamp", CASSANDRA_5),
            new BasicVersionedMetadata("currentTimestamp", null, CASSANDRA_5),
            new BasicVersionedMetadata("current_timestamp", CASSANDRA_5),
            new BasicVersionedMetadata("currentDate", null, CASSANDRA_5),
            new BasicVersionedMetadata("current_date", CASSANDRA_5),
            new BasicVersionedMetadata("currentTime", null, CASSANDRA_5),
            new BasicVersionedMetadata("current_time", CASSANDRA_5),
            new BasicVersionedMetadata("currentTimeUUID", null, CASSANDRA_5),
            new BasicVersionedMetadata("current_timeuuid", CASSANDRA_5)
        );
        return buildMetadataList(timeDateFunctions, databaseVersion);
    }

    /**
     * Builds the comma-separated list of the system functions available in this Cassandra database.
     * This method is used to implement the method {@link DatabaseMetaData#getSystemFunctions()}.
     *
     * @return A valid result for the implementation of {@link DatabaseMetaData#getSystemFunctions()}.
     */
    public String buildSystemFunctionsList() {
        final List<VersionedMetadata> systemFunctions = Arrays.asList(
            new BasicVersionedMetadata("token"),
            new BasicVersionedMetadata("ttl"),
            new BasicVersionedMetadata("writetime"),
            // Masking functions introduced by CEP-20 (see
            // https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-20%3A+Dynamic+Data+Masking)
            new BasicVersionedMetadata("mask_default", CASSANDRA_5),
            new BasicVersionedMetadata("mask_hash", CASSANDRA_5),
            new BasicVersionedMetadata("mask_inner", CASSANDRA_5),
            new BasicVersionedMetadata("mask_null", CASSANDRA_5),
            new BasicVersionedMetadata("mask_outer", CASSANDRA_5),
            new BasicVersionedMetadata("mask_replace", CASSANDRA_5)
        );
        return buildMetadataList(systemFunctions, this.databaseVersion);
    }

    /**
     * Builds the comma-separated list of the string functions available in this Cassandra database.
     * This method is used to implement the method {@link DatabaseMetaData#getStringFunctions()}.
     *
     * @return A valid result for the implementation of {@link DatabaseMetaData#getStringFunctions()}.
     */
    public String buildStringFunctionsList() {
        // Cassandra does not implement natively string functions.
        return StringUtils.EMPTY;
    }

}
