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

package com.ing.data.cassandra.jdbc.types;

import com.datastax.oss.driver.api.core.data.CqlVector;

/**
 * JDBC description of {@code VECTOR} CQL type (corresponding Java type: {@link CqlVector}).
 * <p>CQL type description: a n-dimensional vector.</p>
 */
@SuppressWarnings("rawtypes")
public class JdbcVector extends AbstractJdbcCollection<CqlVector> {

    /**
     * Gets a {@code JdbcVector} instance.
     */
    public static final JdbcVector INSTANCE = new JdbcVector();

    JdbcVector() {
    }

    @Override
    public String toString(final CqlVector obj) {
        return obj.toString();
    }

    @Override
    public Class<CqlVector> getType() {
        return CqlVector.class;
    }

    @Override
    public CqlVector compose(final Object obj) {
        if (obj != null && obj.getClass().isAssignableFrom(CqlVector.class)) {
            return (CqlVector) obj;
        }
        return null;
    }

    @Override
    public Object decompose(final CqlVector value) {
        return value;
    }

}
