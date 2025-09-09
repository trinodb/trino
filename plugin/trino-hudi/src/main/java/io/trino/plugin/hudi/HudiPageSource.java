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
package io.trino.plugin.hudi;

import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hudi.reader.HudiTrinoReaderContext;
import io.trino.plugin.hudi.util.HudiAvroSerializer;
import io.trino.plugin.hudi.util.SynthesizedColumnHandler;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.metrics.Metrics;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.util.collection.ClosableIterator;

import java.io.IOException;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkState;

public class HudiPageSource
        implements ConnectorPageSource
{
    HoodieFileGroupReader<IndexedRecord> fileGroupReader;
    // TODO: Remove pageSource here, Hudi doesn't use this page source to read
    ConnectorPageSource pageSource;
    HudiTrinoReaderContext readerContext;
    PageBuilder pageBuilder;
    HudiAvroSerializer avroSerializer;
    List<HiveColumnHandle> columnHandles;
    ClosableIterator<IndexedRecord> recordIterator;

    public HudiPageSource(
            ConnectorPageSource pageSource,
            HoodieFileGroupReader<IndexedRecord> fileGroupReader,
            HudiTrinoReaderContext readerContext,
            List<HiveColumnHandle> columnHandles,
            SynthesizedColumnHandler synthesizedColumnHandler)
    {
        this.pageSource = pageSource;
        this.fileGroupReader = fileGroupReader;
        this.readerContext = readerContext;
        this.columnHandles = columnHandles;
        this.pageBuilder = new PageBuilder(columnHandles.stream().map(HiveColumnHandle::getType).toList());
        this.avroSerializer = new HudiAvroSerializer(columnHandles, synthesizedColumnHandler);
        try {
            this.recordIterator = fileGroupReader.getClosableIterator();
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to initialize file group reader!", e);
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return pageSource.getCompletedBytes();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return pageSource.getCompletedPositions();
    }

    @Override
    public long getReadTimeNanos()
    {
        return pageSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return !recordIterator.hasNext();
    }

    @Override
    public Page getNextPage()
    {
        checkState(pageBuilder.isEmpty(), "PageBuilder is not empty at the beginning of a new page");
        while (recordIterator.hasNext()) {
            avroSerializer.buildRecordInPage(pageBuilder, recordIterator.next());
        }

        Page newPage = pageBuilder.build();
        pageBuilder.reset();
        return newPage;
    }

    @Override
    public long getMemoryUsage()
    {
        return pageSource.getMemoryUsage();
    }

    @Override
    public void close()
            throws IOException
    {
        fileGroupReader.close();
        pageSource.close();
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return pageSource.isBlocked();
    }

    @Override
    public Metrics getMetrics()
    {
        return pageSource.getMetrics();
    }
}
