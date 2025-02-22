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

import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.data.DefaultUdtValue;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility methods used for User-defined types (UDTs) handling.
 */
public final class UdtUtil {

    private UdtUtil() {
        // Private constructor to hide the public one.
    }

    /**
     * Returns a list of UDT values using an implementation of {@link UdtValue} using the result of the method
     * {@link UdtValue#getFormattedContents()} as string representation of the UDT object.
     *
     * @param list The original list of UDT values.
     * @return An instance of {@link List} using {@link WithFormattedContentsDefaultUdtValue} for the items of the given
     * list, or {@code null} if the original list was {@code null}.
     */
    public static List<UdtValue> udtValuesUsingFormattedContents(final List<?> list) {
        if (list != null) {
            return list.stream()
                .map(item -> udtValueUsingFormattedContents((UdtValue) item))
                .collect(Collectors.toList());
        }
        return null;
    }

    /**
     * Returns a set of UDT values using an implementation of {@link UdtValue} using the result of the method
     * {@link UdtValue#getFormattedContents()} as string representation of the UDT object.
     *
     * @param set The original set of UDT values.
     * @return An instance of {@link Set} using {@link WithFormattedContentsDefaultUdtValue} for the items of the given
     * set, or {@code null} if the original set was {@code null}.
     */
    public static Set<UdtValue> udtValuesUsingFormattedContents(final Set<?> set) {
        if (set != null) {
            return set.stream()
                .map(item -> udtValueUsingFormattedContents((UdtValue) item))
                .collect(Collectors.toCollection(LinkedHashSet::new));
        }
        return null;
    }

    /**
     * Returns a map where the keys and/or values of type {@link UdtValue} are replaced by an implementation of
     * {@link UdtValue} using the result of the method {@link UdtValue#getFormattedContents()} as string representation
     * of the UDT object.
     *
     * @param map The original map containing some UDT values as keys and/or values.
     * @return An instance of {@link Map} using {@link WithFormattedContentsDefaultUdtValue} for the keys and/or values
     * of the given map which were UDT values, or {@code null} if the original map was {@code null}.
     */
    public static Map<?, ?> udtValuesUsingFormattedContents(final Map<?, ?> map) {
        if (map != null) {
            return map.entrySet().stream()
                .collect(Collectors.toMap(
                    entry -> {
                        final Object key = entry.getKey();
                        if (UdtValue.class.isAssignableFrom(key.getClass())) {
                            return udtValueUsingFormattedContents((UdtValue) key);
                        }
                        return key;
                    },
                    entry -> {
                        final Object value = entry.getValue();
                        if (UdtValue.class.isAssignableFrom(value.getClass())) {
                            return udtValueUsingFormattedContents((UdtValue) value);
                        }
                        return value;
                    },
                    (k, v) -> v, LinkedHashMap::new)
                );
        }
        return null;
    }

    /**
     * Returns an implementation of {@link UdtValue} using the result of the method
     * {@link UdtValue#getFormattedContents()} as string representation of the UDT object.
     *
     * @param udtValue The original UDT value.
     * @return An instance of {@link WithFormattedContentsDefaultUdtValue} for the given UDT value or {@code null} if
     * the original value was {@code null}.
     */
    public static UdtValue udtValueUsingFormattedContents(final UdtValue udtValue) {
        if (udtValue == null) {
            return null;
        }
        return new WithFormattedContentsDefaultUdtValue(udtValue);
    }

    /**
     * Extended implementation of {@link DefaultUdtValue} overriding {@link DefaultUdtValue#toString()} method to
     * include the representation of the UDT contents.
     * <p>
     *     Be careful, using this implementation may result in a data leak (e.g. in application logs) if the method
     *     {@link #toString()} is called carelessly.
     * </p>
     */
    public static final class WithFormattedContentsDefaultUdtValue extends DefaultUdtValue {
        /**
         * Instantiates a {@code WithFormattedContentsDefaultUdtValue} based on an original {@link UdtValue} instance.
         *
         * @param original The original UDT value.
         */
        @SuppressWarnings("ResultOfMethodCallIgnored")
        WithFormattedContentsDefaultUdtValue(final UdtValue original) {
            super(original.getType());
            // Copy original values into this new instance.
            for (int i = 0; i < original.size(); i++) {
                if (!original.isNull(i)) {
                    final DataType fieldDataType = original.getType().getFieldTypes().get(i);
                    final GenericType<Object> fieldGenericType = codecRegistry().codecFor(fieldDataType).getJavaType();
                    this.set(i, original.get(i, fieldGenericType), fieldGenericType);
                }
            }
        }

        /**
         * Gets the string representation of the UDT contents for this object.
         *
         * @return The string representation of the contents of this UDT value.
         * @see #getFormattedContents()
         */
        @Override
        public String toString() {
            return this.getFormattedContents();
        }
    }
}
