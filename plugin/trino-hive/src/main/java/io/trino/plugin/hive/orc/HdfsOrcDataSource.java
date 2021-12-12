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
package io.trino.plugin.hive.orc;

import io.airlift.slice.Slice;
import io.trino.orc.AbstractOrcDataSource;
import io.trino.orc.OrcDataSourceId;
import io.trino.orc.OrcReaderOptions;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.util.FSDataInputStreamTail;
import io.trino.spi.TrinoException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hdfs.BlockMissingException;

import java.io.IOException;

import static io.trino.plugin.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_MISSING_DATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_UNKNOWN_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HdfsOrcDataSource
        extends AbstractOrcDataSource
{
    private final FSDataInputStream inputStream;
    private final FileFormatDataSourceStats stats;

    public HdfsOrcDataSource(
            OrcDataSourceId id,
            long size,
            OrcReaderOptions options,
            FSDataInputStream inputStream,
            FileFormatDataSourceStats stats)
    {
        super(id, size, options);
        this.inputStream = requireNonNull(inputStream, "inputStream is null");
        this.stats = requireNonNull(stats, "stats is null");
    }

    @Override
    public void close()
            throws IOException
    {
        inputStream.close();
    }

    @Override
    protected Slice readTailInternal(int length)
            throws IOException
    {
        //  Handle potentially imprecise file lengths by reading the footer
        long readStart = System.nanoTime();
        FSDataInputStreamTail fileTail = FSDataInputStreamTail.readTail(getId().toString(), getEstimatedSize(), inputStream, length);
        Slice tailSlice = fileTail.getTailSlice();
        stats.readDataBytesPerSecond(tailSlice.length(), System.nanoTime() - readStart);
        return tailSlice;
    }

    @Override
    protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
    {
        try {
            long readStart = System.nanoTime();
            inputStream.readFully(position, buffer, bufferOffset, bufferLength);
            stats.readDataBytesPerSecond(bufferLength, System.nanoTime() - readStart);
        }
        catch (TrinoException e) {
            // just in case there is a Trino wrapper or hook
            throw e;
        }
        catch (Exception e) {
            String message = format("Error reading from %s at position %s", this, position);
            if (e instanceof BlockMissingException) {
                throw new TrinoException(HIVE_MISSING_DATA, message, e);
            }
            if (e instanceof IOException) {
                throw new TrinoException(HIVE_FILESYSTEM_ERROR, message, e);
            }
            throw new TrinoException(HIVE_UNKNOWN_ERROR, message, e);
        }
    }
}
