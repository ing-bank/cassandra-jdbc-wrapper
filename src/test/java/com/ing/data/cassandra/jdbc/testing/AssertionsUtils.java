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

package com.ing.data.cassandra.jdbc.testing;

import com.ing.data.cassandra.jdbc.utils.ErrorConstants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.function.Executable;

import java.sql.SQLFeatureNotSupportedException;

import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.NOT_SUPPORTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Utilities methods for assertions specific to the JDBC driver tests.
 */
public final class AssertionsUtils {

    /**
     * Assert that execution of the supplied executable throws a {@link SQLFeatureNotSupportedException} or a subtype
     * thereof, with the message {@value ErrorConstants#NOT_SUPPORTED }.
     * If no exception is thrown, or if an exception of a different type is thrown, this method will fail.
     *
     * @param executable The verified executable.
     * @see Assertions#assertThrows(Class, Executable)
     */
    public static void assertNotImplemented(final Executable executable) {
        final SQLFeatureNotSupportedException sqlEx = assertThrows(SQLFeatureNotSupportedException.class, executable);
        assertEquals(NOT_SUPPORTED, sqlEx.getMessage());
    }

}
