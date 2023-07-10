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
package io.trino.plugin.iceberg;

import io.airlift.slice.Slice;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.orc.AbstractOrcDataSource;
import io.trino.orc.OrcDataSourceId;
import io.trino.orc.OrcReaderOptions;
import io.trino.plugin.hive.FileFormatDataSourceStats;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class TrinoOrcDataSource
        extends AbstractOrcDataSource
{
    private final FileFormatDataSourceStats stats;
    private final TrinoInput input;

    public TrinoOrcDataSource(TrinoInputFile file, OrcReaderOptions options, FileFormatDataSourceStats stats)
            throws IOException
    {
        super(new OrcDataSourceId(file.location().toString()), file.length(), options);
        this.stats = requireNonNull(stats, "stats is null");
        this.input = file.newInput();
    }

    @Override
    public void close()
            throws IOException
    {
        input.close();
    }

    @Override
    protected Slice readTailInternal(int length)
            throws IOException
    {
        long readStart = System.nanoTime();
        Slice tail = input.readTail(length);
        stats.readDataBytesPerSecond(tail.length(), System.nanoTime() - readStart);
        return tail;
    }

    @Override
    protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        long readStart = System.nanoTime();
        input.readFully(position, buffer, bufferOffset, bufferLength);
        stats.readDataBytesPerSecond(bufferLength, System.nanoTime() - readStart);
    }
}
