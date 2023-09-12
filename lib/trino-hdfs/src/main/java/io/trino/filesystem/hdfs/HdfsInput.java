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
package io.trino.filesystem.hdfs;

import io.airlift.slice.Slice;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.hdfs.FSDataInputStreamTail;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.FileNotFoundException;
import java.io.IOException;

import static io.trino.filesystem.hdfs.HdfsFileSystem.withCause;
import static java.util.Objects.requireNonNull;

class HdfsInput
        implements TrinoInput
{
    private final FSDataInputStream stream;
    private final TrinoInputFile inputFile;
    private boolean closed;

    public HdfsInput(FSDataInputStream stream, TrinoInputFile inputFile)
    {
        this.stream = requireNonNull(stream, "stream is null");
        this.inputFile = requireNonNull(inputFile, "inputFile is null");
    }

    @Override
    public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        ensureOpen();
        try {
            stream.readFully(position, buffer, bufferOffset, bufferLength);
        }
        catch (FileNotFoundException e) {
            throw withCause(new FileNotFoundException("File %s not found: %s".formatted(toString(), e.getMessage())), e);
        }
        catch (IOException e) {
            throw new IOException("Read exactly %s bytes at position %s of file %s failed: %s".formatted(bufferLength, position, toString(), e.getMessage()), e);
        }
    }

    @Override
    public int readTail(byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        ensureOpen();
        try {
            Slice tail = FSDataInputStreamTail.readTail(toString(), inputFile.length(), stream, bufferLength).getTailSlice();
            tail.getBytes(0, buffer, bufferOffset, tail.length());
            return tail.length();
        }
        catch (FileNotFoundException e) {
            throw withCause(new FileNotFoundException("File %s not found: %s".formatted(toString(), e.getMessage())), e);
        }
        catch (IOException e) {
            throw new IOException("Read %s tail bytes of file %s failed: %s".formatted(bufferLength, toString(), e.getMessage()), e);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        closed = true;
        stream.close();
    }

    @Override
    public String toString()
    {
        return inputFile.toString();
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Output stream closed: " + this);
        }
    }
}
