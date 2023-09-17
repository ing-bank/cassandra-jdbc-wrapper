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

package com.ing.data.cassandra.jdbc.codec;

import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.apache.commons.lang3.StringUtils;

import static com.ing.data.cassandra.jdbc.utils.DriverUtil.NULL_KEYWORD;

/**
 * Provides a minimal implementation for the methods {@link TypeCodec#parse(String)} and
 * {@link TypeCodec#format(Object)}.
 * The {@code null} and empty values are automatically managed, so only implement {@link #parseNonNull(String)} and
 * {@link #formatNonNull(Object)} for non-null values to get the full implementation of the parsing and formatting
 * methods.
 *
 * @param <JavaTypeT> The Java type managed by the codec implementation.
 */
public abstract class AbstractCodec<JavaTypeT> {

    /**
     * Parses the given CQL literal into an instance of the Java type handled by this codec.
     *
     * @param value The value to parse.
     * @return The parsed value or {@code null} if the value to parse is {@code NULL} CQL keyword or blank.
     */
    public JavaTypeT parse(final String value) {
        if (StringUtils.isBlank(value) || NULL_KEYWORD.equals(value)) {
            return null;
        }
        return parseNonNull(value);
    }

    /**
     * Parses the given non-null CQL literal into an instance of the Java type handled by this codec.
     *
     * @param value The value to parse.
     * @return The parsed value.
     */
    abstract JavaTypeT parseNonNull(@NonNull String value);

    /**
     * Formats the given value as a valid CQL literal according to the CQL type handled by this codec.
     *
     * @param value The value to format.
     * @return The formatted value or {@code NULL} CQL keyword if the value to format is {@code null}.
     */
    @NonNull
    public String format(final JavaTypeT value) {
        if (value == null) {
            return NULL_KEYWORD;
        }
        return formatNonNull(value);
    }

    /**
     * Formats the given non-null value as a valid CQL literal according to the CQL type handled by this codec.
     *
     * @param value The value to format.
     * @return The formatted value.
     */
    abstract String formatNonNull(@NonNull JavaTypeT value);
}
