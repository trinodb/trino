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

import com.google.common.annotations.VisibleForTesting;
import io.trino.filesystem.TrinoInputFile;
import io.trino.hive.formats.TrinoDataInputStream;
import io.trino.spi.Page;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Function;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class AvroFileReader
        implements Closeable
{
    private final TrinoDataInputStream input;
    private final AvroPageDataReader dataReader;
    private final DataFileReader<Optional<Page>> fileReader;
    private Page nextPage;
    private final OptionalLong end;

    public AvroFileReader(
            TrinoInputFile inputFile,
            Schema schema,
            AvroTypeManager avroTypeManager)
            throws IOException, AvroTypeException
    {
        this(inputFile, schema, avroTypeManager, 0, OptionalLong.empty());
    }

    public AvroFileReader(
            TrinoInputFile inputFile,
            Schema schema,
            AvroTypeManager avroTypeManager,
            long offset,
            OptionalLong length)
            throws IOException, AvroTypeException
    {
        requireNonNull(inputFile, "inputFile is null");
        requireNonNull(schema, "schema is null");
        requireNonNull(avroTypeManager, "avroTypeManager is null");
        long fileSize = inputFile.length();

        verify(offset >= 0, "offset is negative");
        verify(offset < inputFile.length(), "offset is greater than data size");
        length.ifPresent(lengthLong -> verify(lengthLong >= 1, "length must be at least 1"));
        end = length.stream().map(l -> l + offset).findFirst();
        end.ifPresent(endLong -> verify(endLong <= fileSize, "offset plus length is greater than data size"));
        input = new TrinoDataInputStream(inputFile.newStream());
        dataReader = new AvroPageDataReader(schema, avroTypeManager);
        try {
            fileReader = new DataFileReader<>(new TrinoDataInputStreamAsAvroSeekableInput(input, fileSize), dataReader);
            fileReader.sync(offset);
        }
        catch (AvroPageDataReader.UncheckedAvroTypeException runtimeWrapper) {
            // Avro Datum Reader interface can't throw checked exceptions when initialized by the file reader,
            // so the exception is wrapped in a runtime exception that must be unwrapped
            throw runtimeWrapper.getAvroTypeException();
        }
        avroTypeManager.configure(fileReader.getMetaKeys().stream().collect(toImmutableMap(Function.identity(), fileReader::getMeta)));
    }

    public long getCompletedBytes()
    {
        return input.getReadBytes();
    }

    public long getReadTimeNanos()
    {
        return input.getReadTimeNanos();
    }

    public boolean hasNext()
            throws IOException
    {
        loadNextPageIfNecessary();
        return nextPage != null;
    }

    public Page next()
            throws IOException
    {
        if (!hasNext()) {
            throw new IOException("No more pages available from Avro file");
        }
        Page result = nextPage;
        nextPage = null;
        return result;
    }

    private void loadNextPageIfNecessary()
            throws IOException
    {
        while (nextPage == null && (end.isEmpty() || !fileReader.pastSync(end.getAsLong())) && fileReader.hasNext()) {
            try {
                nextPage = fileReader.next().orElse(null);
            }
            catch (AvroRuntimeException e) {
                throw new IOException(e);
            }
        }
        if (nextPage == null) {
            nextPage = dataReader.flush().orElse(null);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        fileReader.close();
    }

    @VisibleForTesting
    record TrinoDataInputStreamAsAvroSeekableInput(TrinoDataInputStream inputStream, long fileSize)
            implements SeekableInput
    {
        TrinoDataInputStreamAsAvroSeekableInput
        {
            requireNonNull(inputStream, "inputStream is null");
        }

        @Override
        public void seek(long p)
                throws IOException
        {
            inputStream.seek(p);
        }

        @Override
        public long tell()
                throws IOException
        {
            return inputStream.getPos();
        }

        @Override
        public long length()
        {
            return fileSize;
        }

        @Override
        public int read(byte[] b, int off, int len)
                throws IOException
        {
            return inputStream.read(b, off, len);
        }

        @Override
        public void close()
                throws IOException
        {
            inputStream.close();
        }
    }
}
