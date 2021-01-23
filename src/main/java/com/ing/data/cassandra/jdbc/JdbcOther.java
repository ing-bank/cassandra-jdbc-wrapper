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

import java.sql.Types;

/**
 * JDBC description of unknown or other CQL types (corresponding Java type: {@link String}).
 */
public class JdbcOther extends AbstractJdbcType<String> {

    /**
     * Gets a {@code JdbcOther} instance.
     */
    public static final JdbcOther instance = new JdbcOther();

    JdbcOther() {
    }

    public boolean isCaseSensitive() {
        return true;
    }

    public int getScale(final String obj) {
        return DEFAULT_SCALE;
    }

    public int getPrecision(final String obj) {
        return DEFAULT_PRECISION;
    }

    public boolean isCurrency() {
        return false;
    }

    public boolean isSigned() {
        return false;
    }

    public String toString(final String obj) {
        return obj;
    }

    public boolean needsQuotes() {
        return true;
    }

    public String getString(final Object obj) {
        if (obj != null) {
            return obj.toString();
        } else {
            return null;
        }
    }

    public Class<String> getType() {
        return String.class;
    }

    public int getJdbcType() {
        return Types.OTHER;
    }

    public String compose(final Object obj) {
        return getString(obj);
    }

    public Object decompose(final String value) {
        return value;
    }

}
