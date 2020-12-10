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

public class JdbcDouble extends AbstractJdbcType<Double> {
    public static final JdbcDouble instance = new JdbcDouble();

    JdbcDouble() {
    }

    public boolean isCaseSensitive() {
        return false;
    }

    public int getScale(final Double obj) {
        return 300;
    }

    public int getPrecision(final Double obj) {
        return 15;
    }

    public boolean isCurrency() {
        return false;
    }

    public boolean isSigned() {
        return true;
    }

    public String toString(final Double obj) {
        return obj.toString();
    }

    public boolean needsQuotes() {
        return false;
    }

    public Class<Double> getType() {
        return Double.class;
    }

    public int getJdbcType() {
        return Types.DOUBLE;
    }

    public Double compose(final Object value) {
        return (Double) value;
    }

    public Object decompose(final Double value) {
        return value;
    }
}
