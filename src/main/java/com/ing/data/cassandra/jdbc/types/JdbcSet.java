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

import com.datastax.oss.driver.shaded.guava.common.collect.Iterables;

import java.util.Set;

/**
 * JDBC description of {@code SET} CQL type (corresponding Java type: {@link Set}).
 * <p>CQL type description: a collection of one or more elements.</p>
 */
@SuppressWarnings("rawtypes")
public class JdbcSet extends AbstractJdbcCollection<Set> {

    /**
     * Gets a {@code JdbcSet} instance.
     */
    public static final JdbcSet INSTANCE = new JdbcSet();

    JdbcSet() {
    }

    @Override
    public String toString(final Set obj) {
        return Iterables.toString(obj);
    }

    @Override
    public Class<Set> getType() {
        return Set.class;
    }

    @Override
    public Set compose(final Object obj) {
        if (obj != null && obj.getClass().isAssignableFrom(Set.class)) {
            return (Set) obj;
        }
        return null;
    }

    @Override
    public Object decompose(final Set value) {
        return value;
    }

}
