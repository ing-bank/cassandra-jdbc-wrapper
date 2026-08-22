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

package com.ing.data.cassandra.jdbc.utils;

import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.data.DefaultTupleValue;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toCollection;

/**
 * Utility methods used for tuples handling.
 */
public final class TupleUtil {

    private TupleUtil() {
        // Private constructor to hide the public one.
    }

    /**
     * Returns a list of tuple values using an implementation of {@link TupleValue} using the result of the method
     * {@link TupleValue#getFormattedContents()} as string representation of the tuple object.
     *
     * @param list The original list of tuple values.
     * @return An instance of {@link List} using {@link WithFormattedContentsDefaultTupleValue} for the items of the
     * given list, or {@code null} if the original list was {@code null}.
     */
    public static List<TupleValue> tupleValuesUsingFormattedContents(final List<?> list) {
        if (list != null) {
            return list.stream()
                .map(item -> tupleValueUsingFormattedContents((TupleValue) item))
                .toList();
        }
        return null;
    }

    /**
     * Returns a set of tuple values using an implementation of {@link TupleValue} using the result of the method
     * {@link TupleValue#getFormattedContents()} as string representation of the tuple object.
     *
     * @param set The original set of tuple values.
     * @return An instance of {@link Set} using {@link WithFormattedContentsDefaultTupleValue} for the items of the
     * given set, or {@code null} if the original set was {@code null}.
     */
    public static Set<TupleValue> tupleValuesUsingFormattedContents(final Set<?> set) {
        if (set != null) {
            return set.stream()
                .map(item -> tupleValueUsingFormattedContents((TupleValue) item))
                .collect(toCollection(LinkedHashSet::new));
        }
        return null;
    }

    /**
     * Returns a map where the keys and/or values of type {@link TupleValue} are replaced by an implementation of
     * {@link TupleValue} using the result of the method {@link TupleValue#getFormattedContents()} as string
     * representation of the tuple object.
     *
     * @param map The original map containing some tuple values as keys and/or values.
     * @return An instance of {@link Map} using {@link WithFormattedContentsDefaultTupleValue} for the keys and/or
     * values of the given map which were tuple values, or {@code null} if the original map was {@code null}.
     */
    public static Map<?, ?> tupleValuesUsingFormattedContents(final Map<?, ?> map) {
        if (map != null) {
            return map.entrySet().stream()
                .collect(Collectors.toMap(
                    entry -> {
                        final Object key = entry.getKey();
                        if (TupleValue.class.isAssignableFrom(key.getClass())) {
                            return tupleValueUsingFormattedContents((TupleValue) key);
                        }
                        return key;
                    },
                    entry -> {
                        final Object value = entry.getValue();
                        if (TupleValue.class.isAssignableFrom(value.getClass())) {
                            return tupleValueUsingFormattedContents((TupleValue) value);
                        }
                        return value;
                    },
                    (k, v) -> v, LinkedHashMap::new)
                );
        }
        return null;
    }

    /**
     * Returns an implementation of {@link TupleValue} using the result of the method
     * {@link TupleValue#getFormattedContents()} as string representation of the tuple object.
     *
     * @param tupleValue The original tuple value.
     * @return An instance of {@link WithFormattedContentsDefaultTupleValue} for the given tuple value or {@code null}
     * if the original value was {@code null}.
     */
    public static TupleValue tupleValueUsingFormattedContents(final TupleValue tupleValue) {
        if (tupleValue == null) {
            return null;
        }
        return new WithFormattedContentsDefaultTupleValue(tupleValue);
    }

    /**
     * Extended implementation of {@link DefaultTupleValue} overriding {@link DefaultTupleValue#toString()} method to
     * include the representation of the tuple contents.
     * <p>
     *     Be careful, using this implementation may result in a data leak (e.g. in application logs) if the method
     *     {@link #toString()} is called carelessly.
     * </p>
     */
    public static final class WithFormattedContentsDefaultTupleValue extends DefaultTupleValue {
        /**
         * Instantiates a {@code WithFormattedContentsDefaultTupleValue} based on an original {@link TupleValue}
         * instance.
         *
         * @param original The original tuple value.
         */
        @SuppressWarnings("ResultOfMethodCallIgnored")
        WithFormattedContentsDefaultTupleValue(final TupleValue original) {
            super(original.getType());
            // Copy original values into this new instance.
            for (int i = 0; i < original.size(); i++) {
                if (!original.isNull(i)) {
                    final DataType fieldDataType = original.getType().getComponentTypes().get(i);
                    final GenericType<Object> fieldGenericType = codecRegistry().codecFor(fieldDataType).getJavaType();
                    this.set(i, original.get(i, fieldGenericType), fieldGenericType);
                }
            }
        }

        /**
         * Gets the string representation of the tuple contents for this object.
         *
         * @return The string representation of the contents of this tuple value.
         * @see #getFormattedContents()
         */
        @Override
        public String toString() {
            return this.getFormattedContents();
        }
    }
}
