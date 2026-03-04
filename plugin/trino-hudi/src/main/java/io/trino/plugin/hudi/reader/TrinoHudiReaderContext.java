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
package io.trino.plugin.hudi.reader;

import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hudi.util.HudiAvroSerializer;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.avro.AvroRecordContext;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodiePreCombineAvroRecordMerger;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class TrinoHudiReaderContext
        extends HoodieReaderContext<IndexedRecord>
{
    ConnectorPageSource pageSource;
    private final HudiAvroSerializer avroSerializer;
    Map<String, Integer> colToPosMap;
    List<HiveColumnHandle> dataHandles;
    List<HiveColumnHandle> columnHandles;

    public TrinoHudiReaderContext(
            StorageConfiguration<?> storageConfiguration,
            HoodieTableConfig tableConfig,
            ConnectorPageSource pageSource,
            List<HiveColumnHandle> dataHandles,
            List<HiveColumnHandle> columnHandles)
    {
        super(storageConfiguration, tableConfig, Option.empty(), Option.empty(), new AvroRecordContext(tableConfig, tableConfig.getPayloadClass()));
        this.pageSource = pageSource;
        this.avroSerializer = new HudiAvroSerializer(columnHandles);
        this.dataHandles = dataHandles;
        this.columnHandles = columnHandles;
        this.colToPosMap = new HashMap<>();
        for (int i = 0; i < columnHandles.size(); i++) {
            HiveColumnHandle handle = columnHandles.get(i);
            colToPosMap.put(handle.getBaseColumnName(), i);
        }
    }

    @Override
    public ClosableIterator<IndexedRecord> getFileRecordIterator(
            StoragePath storagePath,
            long start,
            long length,
            Schema dataSchema,
            Schema requiredSchema,
            HoodieStorage storage)
    {
        return createRecordIterator();
    }

    @Override
    public ClosableIterator<IndexedRecord> getFileRecordIterator(
            StoragePathInfo storagePathInfo,
            long start,
            long length,
            Schema dataSchema,
            Schema requiredSchema,
            HoodieStorage storage)
    {
        return createRecordIterator();
    }

    private ClosableIterator<IndexedRecord> createRecordIterator()
    {
        return new ClosableIterator<>()
        {
            private SourcePage currentSourcePage;
            private int currentPosition;

            @Override
            public void close()
            {
                try {
                    pageSource.close();
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public boolean hasNext()
            {
                // If all records in the current page are consume, try to get next page
                if (currentSourcePage == null || currentPosition >= currentSourcePage.getPositionCount()) {
                    if (pageSource.isFinished()) {
                        return false;
                    }

                    // Get next page and reset currentPosition
                    currentSourcePage = pageSource.getNextSourcePage();
                    currentPosition = 0;

                    // If no more pages are available
                    return currentSourcePage != null;
                }

                return true;
            }

            @Override
            public IndexedRecord next()
            {
                if (!hasNext()) {
                    throw new NoSuchElementException("No more records in the iterator");
                }

                IndexedRecord record = avroSerializer.serialize(currentSourcePage, currentPosition);
                currentPosition++;
                return record;
            }
        };
    }

    @Override
    protected Option<HoodieRecordMerger> getRecordMerger(RecordMergeMode mergeMode, String mergeStrategyId, String mergeImplClasses)
    {
        return Option.of(new HoodiePreCombineAvroRecordMerger());
    }

    @Override
    public ClosableIterator<IndexedRecord> mergeBootstrapReaders(
            ClosableIterator<IndexedRecord> skeletonFileIterator,
            Schema skeletonRequiredSchema, ClosableIterator<IndexedRecord> dataFileIterator,
            Schema dataRequiredSchema,
            List<Pair<String, Object>> requiredPartitionFieldAndValues)
    {
        throw new UnsupportedOperationException("Bootstrap merging is not supported by TrinoHudiReaderContext");
    }
}
