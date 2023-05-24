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

import java.io.IOException;

import static java.util.Objects.requireNonNull;

class HdfsInput
        implements TrinoInput
{
    private final FSDataInputStream stream;
    private final TrinoInputFile inputFile;

    public HdfsInput(FSDataInputStream stream, TrinoInputFile inputFile)
    {
        this.stream = requireNonNull(stream, "stream is null");
        this.inputFile = requireNonNull(inputFile, "inputFile is null");
    }

    @Override
    public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        stream.readFully(position, buffer, bufferOffset, bufferLength);
    }

    @Override
    public int readTail(byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        Slice tail = FSDataInputStreamTail.readTail(toString(), inputFile.length(), stream, bufferLength).getTailSlice();
        tail.getBytes(0, buffer, bufferOffset, tail.length());
        return tail.length();
    }

    @Override
    public void close()
            throws IOException
    {
        stream.close();
    }

    @Override
    public String toString()
    {
        return inputFile.toString();
    }
}
