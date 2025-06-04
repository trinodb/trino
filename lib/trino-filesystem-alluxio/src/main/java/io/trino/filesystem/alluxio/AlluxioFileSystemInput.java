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
package io.trino.filesystem.alluxio;

import alluxio.client.file.FileInStream;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;

import java.io.IOException;

import static java.lang.Math.min;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

public class AlluxioFileSystemInput
        implements TrinoInput
{
    private final FileInStream stream;
    private final TrinoInputFile inputFile;

    private volatile boolean closed;

    public AlluxioFileSystemInput(FileInStream stream, TrinoInputFile inputFile)
    {
        this.stream = requireNonNull(stream, "stream is null");
        this.inputFile = requireNonNull(inputFile, "inputFile is null");
    }

    @Override
    public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        ensureOpen();
        checkFromIndexSize(bufferOffset, bufferLength, buffer.length);
        if (position + bufferLength > inputFile.length()) {
            throw new IOException("readFully position overflow %s. pos %d + buffer length %d > file size %d"
                    .formatted(inputFile.location(), position, bufferLength, inputFile.length()));
        }
        stream.positionedRead(position, buffer, bufferOffset, bufferLength);
    }

    @Override
    public int readTail(byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        ensureOpen();
        checkFromIndexSize(bufferOffset, bufferLength, buffer.length);
        long fileSize = inputFile.length();
        int readSize = (int) min(fileSize, bufferLength);
        readFully(fileSize - readSize, buffer, bufferOffset, readSize);
        return readSize;
    }

    @Override
    public String toString()
    {
        return inputFile.toString();
    }

    @Override
    public void close()
            throws IOException
    {
        closed = true;
        stream.close();
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Input stream closed: " + this);
        }
    }
}
