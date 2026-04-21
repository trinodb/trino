/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.filesystem;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.metrics.Metrics;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;

public interface TrinoInput
        extends Closeable
{
    /**
     * Read exactly {@code length} bytes starting from input position {@code position}
     * into the provided buffer starting at offset {@code bufferOffset}.
     * The buffer positions {@code [0..bufferOffset)} are not modified.
     *
     * @param position the position in the input to start reading from; must be non-negative
     * @param buffer the buffer to read bytes into; must be non-null
     * @param bufferOffset the offset in the buffer to start writing bytes to; must be non-negative
     * @param length the number of bytes to read; must be non-negative
     * @throws IndexOutOfBoundsException when {@code length} or {@code bufferOffset} is negative,
     * or when {@code bufferOffset + length} is greater than {@code buffer.length}
     * @throws EOFException when input has fewer than {@code position + length} bytes
     * @throws IOException when {@code position} is negative, or when any other I/O error occurs
     */
    void readFully(long position, byte[] buffer, int bufferOffset, int length)
            throws IOException;

    /**
     * Read at most {@code maxLength} bytes from the end of the input into the provided buffer
     * starting at offset {@code bufferOffset}. The buffer positions {@code [0..bufferOffset)}
     * are not modified.
     *
     * @param buffer the buffer to read bytes into; must be non-null
     * @param bufferOffset the offset in the buffer to start writing bytes to; must be non-negative
     * @param maxLength the maximum number of bytes to read; must be non-negative
     * @return the number of bytes actually read, which may be less than {@code maxLength} if
     * the input contains fewer bytes than {@code maxLength}
     */
    int readTail(byte[] buffer, int bufferOffset, int maxLength)
            throws IOException;

    /**
     * Read exactly {@code length} bytes starting from input position {@code position}
     * and return them as a Slice.
     *
     * @param position the position in the input to start reading from; must be non-negative
     * @param length the number of bytes to read; must be non-negative
     * @throws EOFException when input has fewer than {@code position + length} bytes
     * @throws IOException when {@code position} is negative, or when any other I/O error occurs
     */
    default Slice readFully(long position, int length)
            throws IOException
    {
        byte[] buffer = new byte[length];
        readFully(position, buffer, 0, length);
        return Slices.wrappedBuffer(buffer);
    }

    /**
     * Read at most {@code length} bytes from the end of the input and return them as a Slice.
     */
    default Slice readTail(int length)
            throws IOException
    {
        byte[] buffer = new byte[length];
        int read = readTail(buffer, 0, length);
        return Slices.wrappedBuffer(buffer, 0, read);
    }

    default Metrics getMetrics()
    {
        return Metrics.EMPTY;
    }
}
