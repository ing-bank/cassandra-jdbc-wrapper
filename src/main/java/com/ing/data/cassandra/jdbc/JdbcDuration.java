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

import com.datastax.oss.driver.api.core.data.CqlDuration;

/**
 * JDBC description of {@code DURATION} CQL type (corresponding Java type: {@link CqlDuration}).
 * <p>CQL type description: a duration with nanosecond precision.</p>
 */
public class JdbcDuration extends JdbcOther {

    /**
     * Gets a {@code JdbcDuration} instance.
     */
    public static final JdbcDuration INSTANCE = new JdbcDuration();

    @Override
    public boolean isCaseSensitive() {
        return false;
    }

    @Override
    public boolean needsQuotes() {
        return false;
    }

    @Override
    public String compose(final Object value) {
        if (value == null) {
            return null;
        } else {
            return value.toString();
        }
    }

    @Override
    public Object decompose(final String value) {
        return value;
    }

}
