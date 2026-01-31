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
 * An exception thrown when driver class is not present in the classpath.
 */
public class DriverNotFoundException extends RuntimeException {
    /**
     * Constructs a {@code DriverNotFoundException} from a {@link Throwable}.
     *
     * @param t The underlying throwable.
     */
    public DriverNotFoundException(final Throwable t) {
        super(t);
    }
}
