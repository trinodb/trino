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
package io.trino.plugin.hive.avro;

import io.airlift.units.DataSize;
import io.trino.filesystem.TrinoInputFile;
import io.trino.hive.formats.avro.AvroFileReader;
import io.trino.hive.formats.avro.AvroTypeException;
import io.trino.hive.formats.avro.AvroTypeManager;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import org.apache.avro.Schema;

import java.io.IOException;
import java.util.OptionalLong;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static java.util.Objects.requireNonNull;

public class AvroHivePageSource
        implements ConnectorPageSource
{
    private static final long GUESSED_MEMORY_USAGE = DataSize.of(16, DataSize.Unit.MEGABYTE).toBytes();

    private final String fileName;
    private final AvroFileReader avroFileReader;

    public AvroHivePageSource(
            TrinoInputFile inputFile,
            Schema schema,
            AvroTypeManager avroTypeManager,
            long offset,
            long length)
            throws IOException, AvroTypeException
    {
        fileName = requireNonNull(inputFile, "inputFile is null").location().fileName();
        avroFileReader = new AvroFileReader(inputFile, schema, avroTypeManager, offset, OptionalLong.of(length));
    }

    @Override
    public long getCompletedBytes()
    {
        return avroFileReader.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return avroFileReader.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        try {
            return !avroFileReader.hasNext();
        }
        catch (IOException | RuntimeException e) {
            closeAllSuppress(e, this);
            throw new TrinoException(HIVE_CURSOR_ERROR, "Failed to read Avro file: " + fileName, e);
        }
    }

    @Override
    public Page getNextPage()
    {
        try {
            if (avroFileReader.hasNext()) {
                return avroFileReader.next();
            }
            else {
                return null;
            }
        }
        catch (IOException | RuntimeException e) {
            closeAllSuppress(e, this);
            throw new TrinoException(HIVE_CURSOR_ERROR, "Failed to read Avro file: " + fileName, e);
        }
    }

    @Override
    public long getMemoryUsage()
    {
        return GUESSED_MEMORY_USAGE;
    }

    @Override
    public void close()
            throws IOException
    {
        avroFileReader.close();
    }
}
