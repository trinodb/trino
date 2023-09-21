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
package io.trino.hive.formats.line.text;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.CountingInputStream;
import io.trino.hive.formats.compression.Codec;
import io.trino.hive.formats.line.LineBuffer;
import io.trino.hive.formats.line.LineReader;

import java.io.IOException;
import java.io.InputStream;
import java.util.OptionalLong;
import java.util.function.LongSupplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.addExact;
import static java.util.Objects.requireNonNull;

public final class TextLineReader
        implements LineReader
{
    private static final int INSTANCE_SIZE = instanceSize(TextLineReader.class);

    private final InputStream in;
    private final byte[] buffer;
    private final OptionalLong inputEnd;
    private final LongSupplier rawInputPositionSupplier;
    private final long initialRawInputPosition;

    private boolean firstRecord = true;
    private int bufferStart;
    private int bufferEnd;
    private int bufferPosition;
    private boolean closed;
    private long readTimeNanos;

    public static TextLineReader createCompressedReader(InputStream in, int bufferSize, Codec codec)
            throws IOException
    {
        CountingInputStream countingInputStream = new CountingInputStream(in);
        LongSupplier rawInputPositionSupplier = countingInputStream::getCount;
        in = codec.createStreamDecompressor(countingInputStream);
        return new TextLineReader(in, bufferSize, 0, OptionalLong.empty(), rawInputPositionSupplier);
    }

    public static TextLineReader createUncompressedReader(InputStream in, int bufferSize)
            throws IOException
    {
        return createUncompressedReader(in, bufferSize, 0, Long.MAX_VALUE);
    }

    public static TextLineReader createUncompressedReader(InputStream in, int bufferSize, long splitStart, long splitLength)
            throws IOException
    {
        CountingInputStream countingInputStream = new CountingInputStream(in);
        LongSupplier rawInputPositionSupplier = countingInputStream::getCount;
        return new TextLineReader(countingInputStream, bufferSize, splitStart, OptionalLong.of(splitLength), rawInputPositionSupplier);
    }

    private TextLineReader(InputStream in, int bufferSize, long splitStart, OptionalLong splitLength, LongSupplier rawInputPositionSupplier)
            throws IOException
    {
        requireNonNull(in, "in is null");
        checkArgument(bufferSize >= 16, "bufferSize must be at least 16 bytes");
        checkArgument(bufferSize <= 1024 * 1024 * 1024, "bufferSize is greater than 1GB");
        checkArgument(splitStart >= 0, "splitStart is negative");
        checkArgument(splitLength.orElse(1) > 0, "splitLength must be at least one byte");
        requireNonNull(rawInputPositionSupplier, "rawInputPositionSupplier is null");

        this.in = in;
        this.buffer = new byte[bufferSize];
        this.inputEnd = splitLength.stream().map(length -> addExact(splitStart, length)).findAny();
        this.rawInputPositionSupplier = rawInputPositionSupplier;
        // the initial skip is not included in the physical read size
        this.initialRawInputPosition = splitStart;

        // If reading splitStart of file, skipping UTF-8 BOM, otherwise seek to splitStart position, and skip the remaining line
        if (splitStart == 0) {
            fillBuffer();
            if (bufferEnd >= 3 && buffer[0] == (byte) 0xEF && (buffer[1] == (byte) 0xBB) && (buffer[2] == (byte) 0xBF)) {
                bufferStart = 3;
                bufferPosition = 3;
            }
        }
        else {
            this.in.skipNBytes(splitStart);
            if (closed) {
                return;
            }
            skipLines(1);
            firstRecord = false;
        }
    }

    @Override
    public void close()
            throws IOException
    {
        closed = true;
        in.close();
    }

    @Override
    public boolean isClosed()
    {
        return closed;
    }

    @Override
    public long getRetainedSize()
    {
        return INSTANCE_SIZE + sizeOf(buffer);
    }

    @VisibleForTesting
    public long getCurrentPosition()
    {
        if (!(in instanceof CountingInputStream countingInputStream)) {
            throw new IllegalStateException("Current position only supported for uncompressed files");
        }
        int currentBufferSize = bufferEnd - bufferPosition;
        return countingInputStream.getCount() - currentBufferSize;
    }

    @Override
    public long getBytesRead()
    {
        return rawInputPositionSupplier.getAsLong() - initialRawInputPosition;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean readLine(LineBuffer lineBuffer)
            throws IOException
    {
        lineBuffer.reset();

        if (isAfterEnd()) {
            close();
            return false;
        }

        // fill buffer if necessary
        if (bufferPosition >= bufferEnd) {
            fillBuffer();
        }
        if (closed) {
            // The first record in the split is always returned
            if (firstRecord && bufferEnd > 0) {
                firstRecord = false;
                return true;
            }
            return false;
        }

        while (!closed) {
            if (seekToStartOfLineTerminator()) {
                // end of line found, copy the line without the line terminator
                lineBuffer.write(buffer, bufferStart, bufferPosition - bufferStart);

                seekPastLineTerminator();

                firstRecord = false;
                return true;
            }

            verify(bufferPosition == bufferEnd, "expected to be at the end of the buffer");
            lineBuffer.write(buffer, bufferStart, bufferPosition - bufferStart);
            fillBuffer();
        }
        // if the file does not end in a line terminator, the last line is still valid
        firstRecord = false;
        return !lineBuffer.isEmpty();
    }

    public void skipLines(int lineCount)
            throws IOException
    {
        checkArgument(lineCount >= 0, "lineCount is negative");
        while (!closed && lineCount > 0) {
            if (isAfterEnd()) {
                close();
                return;
            }

            firstRecord = false;

            // fill buffer if necessary
            if (bufferPosition >= bufferEnd) {
                fillBuffer();
                if (closed) {
                    return;
                }
            }

            if (seekToStartOfLineTerminator()) {
                seekPastLineTerminator();
                lineCount--;
            }
        }
    }

    private boolean isAfterEnd()
    {
        if (inputEnd.isPresent()) {
            long currentPosition = getCurrentPosition();
            return currentPosition > inputEnd.getAsLong();
        }
        return false;
    }

    private boolean seekToStartOfLineTerminator()
    {
        while (bufferPosition < bufferEnd) {
            if (isEndOfLineCharacter(buffer[bufferPosition])) {
                return true;
            }
            bufferPosition++;
        }
        return false;
    }

    private static boolean isEndOfLineCharacter(byte currentByte)
    {
        return currentByte == '\n' || currentByte == '\r';
    }

    private void seekPastLineTerminator()
            throws IOException
    {
        verify(isEndOfLineCharacter(buffer[bufferPosition]), "Stream is not at a line terminator");

        // skip carriage return if present
        if (buffer[bufferPosition] == '\r') {
            bufferPosition++;

            // fill buffer if necessary
            if (bufferPosition >= bufferEnd) {
                fillBuffer();
                if (closed) {
                    return;
                }
            }
        }

        // skip newline if present
        if (buffer[bufferPosition] == '\n') {
            bufferPosition++;
        }
        bufferStart = bufferPosition;
    }

    private void fillBuffer()
            throws IOException
    {
        if (closed) {
            return;
        }
        verify(bufferPosition >= bufferEnd, "Buffer is not empty");

        bufferStart = 0;
        bufferPosition = 0;
        bufferEnd = 0;

        long start = System.nanoTime();
        try {
            // fill as much of the buffer as possible
            bufferEnd = in.readNBytes(buffer, 0, buffer.length);
        }
        finally {
            long duration = System.nanoTime() - start;
            readTimeNanos += duration;
        }

        if (bufferEnd == 0) {
            close();
        }
    }
}
