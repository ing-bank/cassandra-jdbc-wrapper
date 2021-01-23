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

/**
 * Abstract class providing description about the JDBC equivalent of a CQL type.
 *
 * @param <T> The Java type corresponding to the CQL type.
 */
public abstract class AbstractJdbcType<T> {

    static final int DEFAULT_SCALE = 0;
    static final int DEFAULT_PRECISION = -1;

    /**
     * Gets whether the values of this type are case-sensitive (if applicable).
     * <p>The implementation should return {@code false} if not applicable.</p>
     *
     * @return {@code true} if the values are case-sensitive, {@code false} otherwise.
     */
    public abstract boolean isCaseSensitive();

    /**
     * Gets the scale of the given value (if applicable).
     * <p>The implementation should return {@value #DEFAULT_SCALE} if not applicable.</p>
     *
     * @param obj The value.
     * @return The scale.
     */
    public abstract int getScale(T obj);

    /**
     * Gets the precision of the given value (if applicable).
     * <p>The implementation should return {@value #DEFAULT_PRECISION} if not applicable.</p>
     *
     * @param obj The value.
     * @return The precision.
     */
    public abstract int getPrecision(T obj);

    /**
     * Gets whether the values of this type are currencies (if applicable).
     * <p>The implementation should return {@code false} if not applicable.</p>
     *
     * @return {@code true} if the values are currencies, {@code false} otherwise.
     */
    public abstract boolean isCurrency();

    /**
     * Gets whether the values of this type are signed (if applicable).
     * <p>The implementation should return {@code false} if not applicable.</p>
     *
     * @return {@code true} if the values are signed, {@code false} otherwise.
     */
    public abstract boolean isSigned();

    /**
     * Gets a string representation of the given value.
     *
     * @param obj The value.
     * @return The string representation of the value.
     */
    public abstract String toString(T obj);

    /**
     * Gets whether the values of this type require quotes.
     *
     * @return {@code true} if the values need to be surrounded by quotes, {@code false} otherwise.
     */
    public abstract boolean needsQuotes();

    /**
     * Gets the Java type corresponding to this type.
     *
     * @return The Java type corresponding to this type.
     */
    public abstract Class<T> getType();

    /**
     * Gets the JDBC type constant.
     *
     * @return The JDBC type constant.
     * @see java.sql.Types
     */
    public abstract int getJdbcType();

    /**
     * Transforms the given object to an instance of the Java type corresponding to this type.
     *
     * @param obj The object.
     * @return An instance of {@code T}.
     */
    public abstract T compose(Object obj);

    /**
     * Transforms the given instance of the Java type corresponding to this type to a generic object.
     *
     * @param obj The value.
     * @return The transformed object.
     */
    public abstract Object decompose(T obj);

}
