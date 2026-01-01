/*
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BlobImplTest {

    private static final Blob EMPTY_BLOB = new BlobImpl();
    private static final byte[] EMPTY_BYTEARRAY = new byte[0];
    private static final byte[] SAMPLE_BYTES = new byte[]{1, 2, 3, 4, 5};
    private static final byte[] ANOTHER_SAMPLE_BYTES = new byte[]{6, 7, 8, 9, 0};
    private static final byte[] PATTERN_SEARCH_TEST_BYTES = new byte[]{1, 2, 1, 2, 2, 3, 1, 3, 2, 2, 3, 3, 2, 2, 1};

    private static int initSampleBlob(final Blob sut) throws SQLException {
        return sut.setBytes(1, SAMPLE_BYTES);
    }

    @Test
    void givenEmptyBlob_whenGetLength_returnExpectedValue() throws SQLException {
        assertEquals(0, EMPTY_BLOB.length());
    }

    @Test
    void givenBlob_whenSetBytesWithNull_throwException() {
        final Blob sut = new BlobImpl();
        SQLException sqlEx = assertThrows(SQLException.class, () -> sut.setBytes(0, null));
        assertEquals("Can't write null array of bytes.", sqlEx.getMessage());

        sqlEx = assertThrows(SQLException.class, () -> sut.setBytes(0, null, 1, 1));
        assertEquals("Can't write null array of bytes.", sqlEx.getMessage());
    }

    @Test
    void givenBlob_whenFree_emptyBlobContent() throws SQLException {
        final Blob sut = new BlobImpl();
        sut.setBytes(1, SAMPLE_BYTES);
        assertEquals(SAMPLE_BYTES.length, sut.length());
        sut.free();
        assertArrayEquals(EMPTY_BYTEARRAY, sut.getBytes(1, SAMPLE_BYTES.length));
        assertEquals(0, sut.length());
    }

    @Test
    void givenBlob_whenSetBytesAtInvalidPosition_throwException() throws SQLException {
        final Blob sut = new BlobImpl();
        initSampleBlob(sut);
        SQLException sqlEx = assertThrows(SQLException.class, () -> sut.setBytes(0, SAMPLE_BYTES));
        assertEquals("Invalid position: 0. It must be included between 1 and the length of existing blob + 1.",
            sqlEx.getMessage());

        sqlEx = assertThrows(SQLException.class, () -> sut.setBytes(SAMPLE_BYTES.length + 2, SAMPLE_BYTES));
        assertEquals("Invalid position: 7. It must be included between 1 and the length of existing blob + 1.",
            sqlEx.getMessage());
    }

    @Test
    void givenBlob_whenSetBytesAtInvalidOffset_throwException() {
        final Blob sut = new BlobImpl();
        SQLException sqlEx = assertThrows(SQLException.class, () -> sut.setBytes(1, SAMPLE_BYTES, 6, 4));
        assertEquals("Invalid offset: 6. It must be within incoming bytes boundaries.",
            sqlEx.getMessage());

        sqlEx = assertThrows(SQLException.class, () -> sut.setBytes(1, SAMPLE_BYTES, -1, 4));
        assertEquals("Invalid offset: -1. It must be within incoming bytes boundaries.",
            sqlEx.getMessage());
    }

    @Test
    void givenBlob_whenSetBytesWithInvalidLength_throwException() {
        final Blob sut = new BlobImpl();
        SQLException sqlEx = assertThrows(SQLException.class, () -> sut.setBytes(1, SAMPLE_BYTES, 0, -1));
        assertEquals("Invalid length: -1. It must be within incoming bytes boundaries.",
            sqlEx.getMessage());

        sqlEx = assertThrows(SQLException.class, () -> sut.setBytes(1, SAMPLE_BYTES, 0, 10));
        assertEquals("Invalid length: 10. It must be within incoming bytes boundaries.",
            sqlEx.getMessage());
    }

    @Test
    void givenBlob_whenSetBytesOnEmptyBlob_returnWrittenBytesLength() throws SQLException {
        final Blob sut = new BlobImpl();
        final int writtenBytes = initSampleBlob(sut);
        assertEquals(SAMPLE_BYTES.length, writtenBytes);
        assertEquals(SAMPLE_BYTES.length, sut.length());
    }

    static Stream<Arguments> buildSetBytesTestCases() {
        return Stream.of(
            Arguments.of(1, 0, 0, SAMPLE_BYTES),
            Arguments.of(1, 0, ANOTHER_SAMPLE_BYTES.length, ANOTHER_SAMPLE_BYTES),
            Arguments.of(3, 1, 4, new byte[]{1, 2, 7, 8, 9, 0}),
            Arguments.of(3, 2, 2, new byte[]{1, 2, 8, 9, 5})
        );
    }

    @ParameterizedTest
    @MethodSource("buildSetBytesTestCases")
    void givenBlob_whenSetBytes_returnExpectedBytes(final long pos,
                                                    final int offset,
                                                    final int len,
                                                    final byte[] expectedArray) throws SQLException {
        final Blob sut = new BlobImpl();
        initSampleBlob(sut);
        final int writtenBytes = sut.setBytes(pos, ANOTHER_SAMPLE_BYTES, offset, len);
        assertEquals(len, writtenBytes);
        assertArrayEquals(expectedArray, sut.getBytes(1, 10));
    }

    static boolean hasEnoughMemoryToRun() {
        return Runtime.getRuntime().maxMemory() >= 3L * (Integer.MAX_VALUE / 2);
    }

    @Test
    @EnabledIf("hasEnoughMemoryToRun") // Check the test is able to run (i.e. the max heap size is at least 3GB).
    void givenBlob_whenSetBytesTooLarge_throwException() throws SQLException {
        final Blob sut = new BlobImpl();
        initSampleBlob(sut);
        final byte[] tooLargeByteArray = new byte[1_073_741_824];
        Arrays.fill(tooLargeByteArray, (byte) 0);
        // Append 1GB byte array twice to reach the maximal blob size.
        // We don't directly build a 2GB-array to avoid exceeding VM limits.
        sut.setBytes(1, tooLargeByteArray, 0, tooLargeByteArray.length);
        final SQLException sqlEx = assertThrows(SQLException.class,
            () -> sut.setBytes(1 + tooLargeByteArray.length, tooLargeByteArray, 0, tooLargeByteArray.length));
        assertEquals("Failed to set bytes: new Blob size exceeds 2GB.", sqlEx.getMessage());
    }

    static Stream<Arguments> buildGetBytesTestCases() {
        return Stream.of(
            Arguments.of(1, 0, new byte[0]),
            Arguments.of(10, 5, new byte[0]),
            Arguments.of(1, SAMPLE_BYTES.length, SAMPLE_BYTES),
            Arguments.of(1, 10, SAMPLE_BYTES),
            Arguments.of(2, 2, new byte[]{2, 3}),
            Arguments.of(2, 5, new byte[]{2, 3, 4, 5})
        );
    }

    @ParameterizedTest
    @MethodSource("buildGetBytesTestCases")
    void givenBlob_whenGetBytes_returnExpectedBytes(final long pos,
                                                    final int len,
                                                    final byte[] expectedArray) throws SQLException {
        final Blob sut = new BlobImpl();
        initSampleBlob(sut);
        assertArrayEquals(expectedArray, sut.getBytes(pos, len));
    }

    @Test
    void givenBlob_whenGetBytesAtInvalidPosition_throwException() throws SQLException {
        final Blob sut = new BlobImpl();
        initSampleBlob(sut);
        final SQLException sqlEx = assertThrows(SQLException.class, () -> sut.getBytes(0, 1));
        assertEquals("Invalid position: 0. It must be 1 or greater.", sqlEx.getMessage());
    }

    @Test
    void givenBlob_whenGetBytesWithInvalidLength_throwException() throws SQLException {
        final Blob sut = new BlobImpl();
        initSampleBlob(sut);
        final SQLException sqlEx = assertThrows(SQLException.class, () -> sut.getBytes(1, -1));
        assertEquals("Invalid length: -1. It must be 0 or greater.", sqlEx.getMessage());
    }

    @Test
    void givenBlob_whenGetBinaryStream_returnExpectedStream() throws SQLException, IOException {
        final Blob sut = new BlobImpl();
        initSampleBlob(sut);
        assertArrayEquals(SAMPLE_BYTES, sut.getBinaryStream().readAllBytes());
    }

    static Stream<Arguments> buildGetBinaryStreamTestCases() {
        return Stream.of(
            Arguments.of(1, 0, new byte[0]),
            Arguments.of(1, SAMPLE_BYTES.length, SAMPLE_BYTES),
            Arguments.of(1, 5, SAMPLE_BYTES),
            Arguments.of(2, 3, new byte[]{2, 3, 4})
        );
    }

    @ParameterizedTest
    @MethodSource("buildGetBinaryStreamTestCases")
    void givenBlob_whenGetBinaryStreamAtPositionWithLength_returnExpectedBytes(
        final long pos, final int len, final byte[] expectedArray
    ) throws SQLException, IOException {
        final Blob sut = new BlobImpl();
        initSampleBlob(sut);
        assertArrayEquals(expectedArray, sut.getBinaryStream(pos, len).readAllBytes());
    }

    @Test
    void givenBlob_whenGetBinaryStreamAtInvalidPosition_throwException() throws SQLException {
        final Blob sut = new BlobImpl();
        initSampleBlob(sut);
        SQLException sqlEx = assertThrows(SQLException.class, () -> sut.getBinaryStream(0, 1));
        assertEquals("Invalid position: 0. It must be 1 or greater.", sqlEx.getMessage());

        sqlEx = assertThrows(SQLException.class, () -> sut.getBinaryStream(10, 1));
        assertEquals("Invalid position: 10 and/or length: 1. It must be within existing blob boundaries.",
            sqlEx.getMessage());

        sqlEx = assertThrows(SQLException.class, () -> sut.getBinaryStream(2, 5));
        assertEquals("Invalid position: 2 and/or length: 5. It must be within existing blob boundaries.",
            sqlEx.getMessage());
    }

    @Test
    void givenBlob_whenGetBinaryStreamWithInvalidLength_throwException() throws SQLException {
        final Blob sut = new BlobImpl();
        initSampleBlob(sut);
        final SQLException sqlEx = assertThrows(SQLException.class, () -> sut.getBinaryStream(1, -1));
        assertEquals("Invalid length: -1. It must be 0 or greater.", sqlEx.getMessage());
    }

    @Test
    void givenBlob_whenTruncateWithInvalidLength_throwException() throws SQLException {
        final Blob sut = new BlobImpl();
        initSampleBlob(sut);
        final SQLException sqlEx = assertThrows(SQLException.class, () -> sut.truncate(-1));
        assertEquals("Invalid length: -1. It must be 0 or greater.", sqlEx.getMessage());
    }

    static Stream<Arguments> buildTruncateTestCases() {
        return Stream.of(
            Arguments.of(0, 0, new byte[0]),
            Arguments.of(SAMPLE_BYTES.length + 1, SAMPLE_BYTES.length, SAMPLE_BYTES),
            Arguments.of(SAMPLE_BYTES.length, SAMPLE_BYTES.length, SAMPLE_BYTES),
            Arguments.of(2, 2, new byte[]{1, 2})
        );
    }

    @ParameterizedTest
    @MethodSource("buildTruncateTestCases")
    void givenBlob_whenTruncate_returnExpectedArray(final long length,
                                                    final long expectedLength,
                                                    final byte[] expectedArray) throws SQLException {
        final Blob sut = new BlobImpl();
        initSampleBlob(sut);
        sut.truncate(length);
        assertEquals(expectedLength, sut.length());
        assertArrayEquals(expectedArray, sut.getBytes(1, SAMPLE_BYTES.length));
    }

    @Test
    @SuppressWarnings("resource")
    void givenBlob_whenSetBinaryStreamAtInvalidPosition_throwException() throws SQLException {
        final Blob sut = new BlobImpl();
        initSampleBlob(sut);
        SQLException sqlEx = assertThrows(SQLException.class, () -> sut.setBinaryStream(0));
        assertEquals("Invalid position: 0. It must be included between 1 and the length of existing blob + 1.",
            sqlEx.getMessage());

        sqlEx = assertThrows(SQLException.class, () -> sut.setBinaryStream(SAMPLE_BYTES.length + 2));
        assertEquals("Invalid position: 7. It must be included between 1 and the length of existing blob + 1.",
            sqlEx.getMessage());
    }

    @Test
    void givenBlob_whenSetBinaryStreamAtPosition_returnExpectedOutputStream() throws SQLException {
        final Blob sut = new BlobImpl();
        initSampleBlob(sut);
        OutputStream binaryStream = sut.setBinaryStream(1);
        assertInstanceOf(ByteArrayOutputStream.class, binaryStream);
        assertArrayEquals(SAMPLE_BYTES, ((ByteArrayOutputStream) binaryStream).toByteArray());

        binaryStream = sut.setBinaryStream(3);
        assertInstanceOf(ByteArrayOutputStream.class, binaryStream);
        assertArrayEquals(new byte[]{3, 4, 5}, ((ByteArrayOutputStream) binaryStream).toByteArray());
    }

    static Stream<Arguments> buildPositionTestCases() {
        return Stream.of(
            Arguments.of(null, 1, -1),
            Arguments.of(new byte[0], 1, 1),
            Arguments.of(new byte[]{2, 2, 3}, 1, 4),
            Arguments.of(new byte[]{2, 2, 3}, 5, 9),
            Arguments.of(new byte[]{2, 2}, 5, 9),
            Arguments.of(new byte[]{2, 2}, 10, 13),
            Arguments.of(new byte[]{2, 3, 4}, 1, -1),
            Arguments.of(new byte[]{2, 1}, 20, -1)
        );
    }

    @ParameterizedTest
    @MethodSource("buildPositionTestCases")
    void givenBlob_whenGetPositionOfPattern_returnExpectedPosition(final byte[] pattern,
                                                                   final long start,
                                                                   final long expectedPosition) throws SQLException {
        final Blob sut = new BlobImpl();
        sut.setBytes(1, PATTERN_SEARCH_TEST_BYTES);
        assertEquals(expectedPosition, sut.position(pattern, start));

        // Execute same test but using pattern as Blob object.
        Blob patternAsBlob = null;
        if (pattern != null) {
            patternAsBlob = new BlobImpl();
            patternAsBlob.setBytes(1, pattern);
        }
        assertEquals(expectedPosition, sut.position(patternAsBlob, start));
    }

    @Test
    void givenBlob_whenGetPositionWithInvalidStartPosition_throwException() {
        final Blob sut = new BlobImpl();
        final SQLException sqlEx = assertThrows(SQLException.class, () -> sut.position(new byte[]{1, 2, 3}, 0));
        assertEquals("Invalid start search position: 0. It must be 1 or greater.", sqlEx.getMessage());
    }

}
