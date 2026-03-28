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
package io.trino.plugin.iceberg.fileio;

import io.trino.filesystem.TrinoInput;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

import java.io.EOFException;
import java.io.IOException;

import static java.lang.Math.min;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

public class EncryptedTrinoInput
        implements TrinoInput
{
    private final InputFile inputFile;
    private final SeekableInputStream inputStream;
    private final long length;
    private boolean closed;

    public EncryptedTrinoInput(InputFile inputFile)
    {
        this.inputFile = requireNonNull(inputFile, "inputFile is null");
        this.inputStream = inputFile.newStream();
        this.length = inputFile.getLength();
    }

    @Override
    public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        ensureOpen();
        checkFromIndexSize(bufferOffset, bufferLength, buffer.length);
        inputStream.seek(position);
        int remaining = bufferLength;
        int offset = bufferOffset;
        while (remaining > 0) {
            int read = inputStream.read(buffer, offset, remaining);
            if (read < 0) {
                throw new EOFException("Reached end of encrypted input " + inputFile.location());
            }
            remaining -= read;
            offset += read;
        }
    }

    @Override
    public int readTail(byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        ensureOpen();
        checkFromIndexSize(bufferOffset, bufferLength, buffer.length);
        int readSize = (int) min(length, bufferLength);
        readFully(length - readSize, buffer, bufferOffset, readSize);
        return readSize;
    }

    @Override
    public void close()
            throws IOException
    {
        closed = true;
        inputStream.close();
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Input closed: " + inputFile.location());
        }
    }
}
