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
import io.trino.hive.formats.avro.AvroTypeManager;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;
import org.apache.avro.Schema;

import java.io.IOException;

public class AvroHivePageSource
        implements ConnectorPageSource
{
    private static final long GUESSED_MEMORY_USAGE = DataSize.of(16, DataSize.Unit.MEGABYTE).toBytes();

    private final AvroFileReader avroFileReader;

    public AvroHivePageSource(
            TrinoInputFile inputFile,
            Schema schema,
            AvroTypeManager avroTypeManager,
            long offset,
            long length)
            throws IOException
    {
        avroFileReader = new AvroFileReader(inputFile, schema, avroTypeManager, offset, length);
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
        return !avroFileReader.getPageIterator().hasNext();
    }

    @Override
    public Page getNextPage()
    {
        return avroFileReader.getPageIterator().next();
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
