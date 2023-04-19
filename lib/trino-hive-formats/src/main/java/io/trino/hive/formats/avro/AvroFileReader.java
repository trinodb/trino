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
package io.trino.hive.formats.avro;

import io.trino.filesystem.TrinoInputFile;
import io.trino.hive.formats.TrinoDataInputStream;
import org.apache.avro.Schema;
import org.apache.avro.file.SeekableInput;

import java.io.Closeable;
import java.io.IOException;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class AvroFileReader
        implements Closeable
{
    private final TrinoDataInputStream input;
    private final AvroFilePageIterator pageIterator;

    public AvroFileReader(
            TrinoInputFile inputFile,
            Schema schema,
            AvroTypeManager avroTypeManager,
            long offset,
            long length)
            throws IOException
    {
        requireNonNull(inputFile, "inputFile is null");
        requireNonNull(schema, "schema is null");
        requireNonNull(avroTypeManager, "avroTypeManager is null");
        long fileSize = inputFile.length();

        verify(offset >= 0, "offset is negative");
        verify(offset < inputFile.length(), "offset is greater than data size");
        verify(length >= 1, "length must be at least 1");
        long end = offset + length;
        verify(end <= fileSize, "offset plus length is greater than data size");
        input = new TrinoDataInputStream(inputFile.newStream());
        input.seek(offset);
        this.pageIterator = new AvroFilePageIterator(schema, avroTypeManager, new SeekableInput()
        {
            @Override
            public void seek(long p)
                    throws IOException
            {
                input.seek(p);
            }

            @Override
            public long tell()
                    throws IOException
            {
                return input.getPos() - offset;
            }

            @Override
            public long length()
                    throws IOException
            {
                return length;
            }

            @Override
            public int read(byte[] b, int off, int len)
                    throws IOException
            {
                return input.read(b, off, len);
            }

            @Override
            public void close()
                    throws IOException
            {
                // input closed by enclosing class
            }
        });
    }

    public AvroFilePageIterator getPageIterator()
    {
        return pageIterator;
    }

    public long getCompletedBytes()
    {
        return input.getReadBytes();
    }

    public long getReadTimeNanos()
    {
        return input.getReadTimeNanos();
    }

    @Override
    public void close()
            throws IOException
    {
        this.input.close();
    }
}
