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

import org.apache.commons.lang3.IntegerRange;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;

import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.INVALID_LENGTH;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.INVALID_LENGTH_FOR_BYTEARRAY;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.INVALID_NULL_BYTEARRAY_FOR_BLOB;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.INVALID_OFFSET_FOR_BYTEARRAY;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.INVALID_POSITION;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.INVALID_POSITION_AND_OR_LENGTH;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.INVALID_POSITION_NO_BOUNDARIES;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.INVALID_START_SEARCH_POSITION;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.MAX_BLOB_SIZE_EXCEEDED;
import static com.ing.data.cassandra.jdbc.utils.ErrorConstants.WRITING_OUTPUT_STREAM_FROM_BLOB_FAILED;
import static java.lang.String.format;
import static java.util.Arrays.mismatch;
import static java.util.Objects.requireNonNullElseGet;
import static org.apache.commons.lang3.ArrayUtils.arraycopy;
import static org.apache.commons.lang3.ArrayUtils.subarray;

/**
 * Implementation of {@link Blob} interface.
 */
public class BlobImpl implements Blob {

    private byte[] content;

    /**
     * Constructor of an empty blob.
     */
    public BlobImpl() {
        this.content = new byte[0];
    }

    @Override
    public long length() throws SQLException {
        return getSafeContent().length;
    }

    @Override
    public byte[] getBytes(final long pos, final int length) throws SQLException {
        if (length == 0) {
            return new byte[0];
        }

        final IntegerRange indexRange = convertToIndexRange(pos, length, false);
        return subarray(getSafeContent(), indexRange.getMinimum(), indexRange.getMaximum());
    }

    @Override
    public InputStream getBinaryStream() {
        return new ByteArrayInputStream(getSafeContent());
    }

    @Override
    public InputStream getBinaryStream(final long pos, final long length) throws SQLException {
        if (length == 0) {
            return new ByteArrayInputStream(new byte[0]);
        }

        final IntegerRange indexRange = convertToIndexRange(pos, (int) length, true);
        return new ByteArrayInputStream(getSafeContent(), indexRange.getMinimum(), (int) length);
    }

    @Override
    public long position(final byte[] pattern, final long start) throws SQLException {
        if (start < 1) {
            throw new SQLException(format(INVALID_START_SEARCH_POSITION, start));
        }

        final int zeroBasedStart = (int) start - 1;
        // JDBC specifications don't describe the behaviour in case of null pattern, so return the not-found pattern
        // position (-1) rather than an exception in such a case.
        if (pattern == null || zeroBasedStart > length()) {
            return -1;
        }

        final byte[] blobContent = getSafeContent();
        final int patternLength = pattern.length;
        for (int i = zeroBasedStart; i <= length() - patternLength; i++) {
            if (mismatch(blobContent, i, i + patternLength, pattern, 0, patternLength) == -1) {
                return (long) i + 1; // Convert back to 1-based index.
            }
        }

        return -1;
    }

    @Override
    public long position(final Blob pattern, final long start) throws SQLException {
        if (pattern == null) {
            return position((byte[]) null, start);
        }
        return position(pattern.getBytes(1, (int) pattern.length()), start);
    }

    @Override
    public int setBytes(final long pos, final byte[] bytes) throws SQLException {
        if (bytes == null) {
            throw new SQLException(INVALID_NULL_BYTEARRAY_FOR_BLOB);
        }

        return setBytes(pos, bytes, 0, bytes.length);
    }

    @Override
    public int setBytes(final long pos, final byte[] bytes, final int offset, final int len) throws SQLException {
        if (bytes == null) {
            throw new SQLException(INVALID_NULL_BYTEARRAY_FOR_BLOB);
        }

        if (offset < 0 || offset > bytes.length) {
            throw new SQLException(format(INVALID_OFFSET_FOR_BYTEARRAY, offset));
        }

        if (len < 0 || len > bytes.length - offset) {
            throw new SQLException(format(INVALID_LENGTH_FOR_BYTEARRAY, len));
        }

        // For Blob.setBytes methods, the position is 1-based and not 0-based. So, the specified position must be in
        // range of the existing Blob data or at most exactly 1 byte after the end of the end of data if we want to
        // append some bytes.
        if (pos <= 0 || pos > length() + 1) {
            throw new SQLException(format(INVALID_POSITION, pos));
        }

        // Adjust pos to zero based.
        final long zeroBasedPos = pos - 1;

        // Overwrite past end of value case.
        if (len >= length() - zeroBasedPos) {
            // Check the new length will not exceed the maximal size allowed for CQL type BLOB (2GB).
            if (pos + len > Integer.MAX_VALUE) {
                throw new SQLException(MAX_BLOB_SIZE_EXCEEDED);
            }

            // Start with the original value, up to the starting position
            final byte[] combinedValue = new byte[(int) zeroBasedPos + len];
            arraycopy(getSafeContent(), 0, combinedValue, 0, (int) zeroBasedPos);

            // Copy rest of data.
            arraycopy(bytes, offset, combinedValue, (int) zeroBasedPos, len);
            this.content = combinedValue;
        } else {
            // Overwrite internal to value case.
            arraycopy(bytes, offset, this.content, (int) zeroBasedPos, len);
        }

        return len;
    }

    @Override
    public OutputStream setBinaryStream(final long pos) throws SQLException {
        if (pos <= 0 || pos > length() + 1) {
            throw new SQLException(format(INVALID_POSITION, pos));
        }

        final int outputSteamLength = (int) length() - (int) pos + 1;
        final OutputStream outputStream = new ByteArrayOutputStream(outputSteamLength);
        try {
            outputStream.write(getBytes(pos, outputSteamLength));
        } catch (final IOException e) {
            throw new SQLException(format(WRITING_OUTPUT_STREAM_FROM_BLOB_FAILED, e.getMessage()));
        }
        return outputStream;
    }

    @Override
    public void truncate(final long len) throws SQLException {
        if (len < 0) {
            throw new SQLException(format(INVALID_LENGTH, len));
        }
        // Note: if the specified length is greater or equal to the length of the existing data, this method will not
        // have any effect on the blob data.
        if (length() > len) {
            this.content = subarray(getSafeContent(), 0, (int) len);
        }
    }

    @Override
    public void free() {
        this.content = null;
    }

    private byte[] getSafeContent() {
        return requireNonNullElseGet(this.content, () -> new byte[0]);
    }

    private IntegerRange convertToIndexRange(final long pos,
                                             final int length,
                                             final boolean withinContentBoundaries) throws SQLException {
        if (length < 0) {
            throw new SQLException(format(INVALID_LENGTH, length));
        }

        if (pos < 1) {
            throw new SQLException(format(INVALID_POSITION_NO_BOUNDARIES, pos));
        }
        if (withinContentBoundaries && pos - 1 + length > length()) {
            throw new SQLException(format(INVALID_POSITION_AND_OR_LENGTH, pos, length));
        }

        final long contentLength = length();
        final long startIndex = Math.min(pos - 1, contentLength);
        final long endIndex = Math.min(startIndex + length, contentLength);
        return IntegerRange.of((int) startIndex, (int) endIndex);
    }
}
