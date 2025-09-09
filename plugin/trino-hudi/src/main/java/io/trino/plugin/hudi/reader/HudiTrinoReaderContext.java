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
import io.trino.plugin.hudi.util.SynthesizedColumnHandler;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HudiTrinoReaderContext
        extends HoodieReaderContext<IndexedRecord>
{
    ConnectorPageSource pageSource;
    private final HudiAvroSerializer avroSerializer;
    Map<String, Integer> colToPosMap;
    List<HiveColumnHandle> dataHandles;
    List<HiveColumnHandle> columnHandles;

    public HudiTrinoReaderContext(
            StorageConfiguration storageConfiguration,
            HoodieTableConfig tableConfig,
            ConnectorPageSource pageSource,
            List<HiveColumnHandle> dataHandles,
            List<HiveColumnHandle> columnHandles,
            SynthesizedColumnHandler synthesizedColumnHandler)
    {
        super(storageConfiguration, tableConfig, Option.empty(), Option.empty(), new TrinoReaderContext(tableConfig));
        this.pageSource = pageSource;
        this.avroSerializer = new HudiAvroSerializer(columnHandles, synthesizedColumnHandler);
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
        return new ClosableIterator<>()
        {
            private Page currentPage;
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
                if (currentPage == null || currentPosition >= currentPage.getPositionCount()) {
                    if (pageSource.isFinished()) {
                        return false;
                    }

                    // Get next page and reset currentPosition
                    currentPage = pageSource.getNextPage();
                    currentPosition = 0;

                    // If no more pages are available
                    return currentPage != null;
                }

                return true;
            }

            @Override
            public IndexedRecord next()
            {
                if (!hasNext()) {
                    // TODO: This can probably be removed or ignored, added this as a sanity check
                    throw new RuntimeException("No more records in the iterator");
                }

                IndexedRecord record = avroSerializer.serialize(currentPage, currentPosition);
                currentPosition++;
                return record;
            }
        };
    }

    @Override
    protected Option<HoodieRecordMerger> getRecordMerger(RecordMergeMode mergeMode, String mergeStrategyId, String mergeImplClasses)
    {
        return Option.of(HoodieAvroRecordMerger.INSTANCE);
    }

    @Override
    public ClosableIterator<IndexedRecord> mergeBootstrapReaders(ClosableIterator<IndexedRecord> skeletonFileIterator, Schema skeletonRequiredSchema, ClosableIterator<IndexedRecord> dataFileIterator, Schema dataRequiredSchema, List<Pair<String, Object>> requiredPartitionFieldAndValues)
    {
        return null;
    }
}
