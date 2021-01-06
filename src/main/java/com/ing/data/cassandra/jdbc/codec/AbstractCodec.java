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

import static com.ing.data.cassandra.jdbc.Utils.NULL_KEYWORD;

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

    public JavaTypeT parse(final String value) {
        if (StringUtils.isBlank(value) || NULL_KEYWORD.equals(value)) {
            return null;
        }
        return parseNonNull(value);
    }

    abstract JavaTypeT parseNonNull(@NonNull String value);

    @NonNull
    public String format(final JavaTypeT value) {
        if (value == null) {
            return NULL_KEYWORD;
        }
        return formatNonNull(value);
    }

    abstract String formatNonNull(@NonNull JavaTypeT value);
}
