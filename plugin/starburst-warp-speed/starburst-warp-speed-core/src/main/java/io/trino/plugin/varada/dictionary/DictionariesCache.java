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
package io.trino.plugin.varada.dictionary;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.Weigher;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.trino.cache.SafeCaches;
import io.trino.plugin.varada.configuration.DictionaryConfiguration;
import io.trino.plugin.varada.dispatcher.model.DictionaryKey;
import io.trino.plugin.varada.dispatcher.model.SchemaTableColumn;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.util.SliceUtils;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.stats.VaradaStatsDictionary;
import io.trino.spi.block.Block;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.VariableWidthBlock;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static io.trino.plugin.varada.dictionary.DictionaryCacheService.DICTIONARY_STAT_GROUP;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DictionariesCache
{
    private static final Logger logger = Logger.get(DictionariesCache.class);
    //number of times we allow exceeding number of max elements before marking the dictionary invalid for write
    public static final int MAX_FAILED_SPLITS = 10;

    private final DictionaryConfiguration dictionaryConfiguration;
    private final AttachDictionaryService attachDictionaryService;
    private final VaradaStatsDictionary globalVaradaStatsDictionary;
    private final Lock readLock;
    private final Lock writeLock;
    private final ExecutorService executorService;
    private final ConcurrentHashMap<DictionaryId, DictionaryMetadata> dictionaryMetadataMap = new ConcurrentHashMap<>();
    private final Object loadSynchronized = new Object();

    private ConcurrentHashMap<DictionaryId, DataValueDictionary> activeDataValuesDictionaries;
    private Cache<DictionaryKey, DataValueDictionary> cache;
    private DictionaryCacheConfiguration activeConfiguration;

    DictionariesCache(DictionaryConfiguration dictionaryConfiguration,
            MetricsManager metricsManager,
            AttachDictionaryService attachDictionaryService)
    {
        this.dictionaryConfiguration = requireNonNull(dictionaryConfiguration);
        this.attachDictionaryService = requireNonNull(attachDictionaryService);
        this.globalVaradaStatsDictionary = requireNonNull(metricsManager).registerMetric(VaradaStatsDictionary.create(DICTIONARY_STAT_GROUP));
        ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
        this.readLock = readWriteLock.readLock();
        this.writeLock = readWriteLock.writeLock();
        initCache(new DictionaryCacheConfiguration(
                dictionaryConfiguration.getMaxDictionaryTotalCacheWeight(),
                dictionaryConfiguration.getDictionaryCacheConcurrencyLevel()));
        this.executorService = Executors.newFixedThreadPool(1);
    }

    private void initCache(DictionaryCacheConfiguration dictionaryConfiguration)
    {
        Weigher<DictionaryKey, DataValueDictionary> weighByLength =
                (dictionaryKey, dataValueDictionary) -> dataValueDictionary.getDictionaryWeight();
        activeConfiguration = dictionaryConfiguration;
        RemovalListener<DictionaryKey, DataValueDictionary> listener = removalNotification -> {
            if (!removalNotification.wasEvicted()) { //do nothing
                return;
            }
            DataValueDictionary dataValueDictionary = removalNotification.getValue();
            if (dataValueDictionary == null) { //do nothing
                return;
            }
            if (dataValueDictionary.getUsingTransactions() > 0) {
                executorService.execute(() -> {
                    /*
                    we still have a rare race here:
                    1. dictionary evicted while warming (very long run warm)
                    2. just before you are about to delete the old dictionary (in attach), a query comes.
                    3. it starts loading the dictionary (since its evicted and it does not look at the active list). while the query is loading it, you delete the dictionary and we have a crash.
                     */
                    this.cache.put(dataValueDictionary.getDictionaryKey(), dataValueDictionary);
                });
            }
            else {
                globalVaradaStatsDictionary.adddictionary_entries(-1);
                globalVaradaStatsDictionary.adddictionaries_weight(dataValueDictionary.getDictionaryWeight() * -1L);
                globalVaradaStatsDictionary.incdictionary_evicted_entries();
                if (removalNotification.getValue().isVarlen()) {
                    globalVaradaStatsDictionary.adddictionaries_varlen_str_weight(dataValueDictionary.getDictionaryWeight() * -1L);
                }
            }
        };
        this.cache = SafeCaches.buildNonEvictableCache(CacheBuilder.newBuilder()
                .maximumWeight(activeConfiguration.totalWeight())
                .weigher(weighByLength)
                .removalListener(listener)
                .concurrencyLevel(activeConfiguration.concurrency()));
        activeDataValuesDictionaries = new ConcurrentHashMap<>();
    }

    long getLastCreatedTimestamp(SchemaTableColumn schemaTableColumn, String nodeIdentifier)
    {
        DictionaryId dictionaryId = DictionaryId.of(schemaTableColumn, nodeIdentifier);
        DictionaryMetadata dictionaryMetadata = dictionaryMetadataMap.get(dictionaryId);
        return dictionaryMetadata != null ? dictionaryMetadata.getLastCreatedTimestamp() : DictionaryKey.CREATED_TIMESTAMP_UNKNOWN;
    }

    private void setLastCreatedTimestamp(DictionaryId dictionaryId, long createdTimestamp)
    {
        DictionaryMetadata dictionaryMetadata = new DictionaryMetadata(createdTimestamp);
        dictionaryMetadataMap.put(dictionaryId, dictionaryMetadata);
    }

    private int getFailedWriteCount(DictionaryKey dictionaryKey)
    {
        DictionaryId dictionaryId = DictionaryId.of(dictionaryKey.schemaTableColumn(), dictionaryKey.nodeIdentifier());
        DictionaryMetadata dictionaryMetadata = dictionaryMetadataMap.get(dictionaryId);
        return dictionaryMetadata != null ? dictionaryMetadata.getFailedWriteCount() : 0;
    }

    boolean hasExceededFailedWriteLimit(DictionaryKey dictionaryKey)
    {
        return getFailedWriteCount(dictionaryKey) >= MAX_FAILED_SPLITS;
    }

    void incFailedWriteCount(DictionaryKey dictionaryKey)
    {
        DictionaryId dictionaryId = DictionaryId.of(dictionaryKey);
        DictionaryMetadata dictionaryMetadata = dictionaryMetadataMap.get(dictionaryId);
        //if (dictionaryMetadata == null) {
        //    setLastCreatedTimestamp(dictionaryId, DictionaryKey.CREATED_TIMESTAMP_UNKNOWN);
        //    dictionaryMetadata = dictionaryMetadataMap.get(dictionaryId);
        //}
        dictionaryMetadata.incFailedWriteCount();
        globalVaradaStatsDictionary.incdictionary_max_exception_count();
    }

    /**
     * get DataValueDictionary from cache if not present in cache will load it from native or will create a new instance
     * param dictionaryKey - key for cache
     * param recTypeCode - original rec type code
     *
     * @param dictionaryKey - key for cache
     * @param recTypeCode - original rec type code
     * will add entry to cache that may be evicted from cache by lru algorithm
     */
    ReadDictionary getReadDictionary(DictionaryKey dictionaryKey,
            int usedDictionarySize,
            RecTypeCode recTypeCode,
            int recTypeLength,
            int dictionaryOffset,
            String rowGroupFilePath)
    {
        // assume that createdTimestamp is known -> lookup in cache for it
        DataValueDictionary dataValueDictionary = cache.getIfPresent(dictionaryKey);

        // dictionary is not found in cache or size is less than used -> load from file
        // ensure that cache always contains the largest dictionary of all splits
        synchronized (loadSynchronized) {
            if ((dataValueDictionary == null) || (dataValueDictionary.getReadAvailableSize() < usedDictionarySize)) {
                dataValueDictionary = attachDictionaryService.load(dictionaryKey, recTypeCode, recTypeLength, dictionaryOffset, rowGroupFilePath);
                loadPreBlock(recTypeCode, dataValueDictionary);
                globalVaradaStatsDictionary.incdictionary_loaded_elements_count();

                cache.put(dictionaryKey, dataValueDictionary);
                globalVaradaStatsDictionary.incdictionary_entries();
            }
        }

        return dataValueDictionary;
    }

    /**
     * get DataValueDictionary from cache if not present in cache will load it from native or will create a new instance
     * param dictionaryKey - key for cache
     * param recTypeCode - original rec type code
     *
     * @param dictionaryKey - key for cache
     * @param recTypeCode - original rec type code
     * will add entry to cache that wont be evicted until calling releaseDictionary method
     * must call releaseDictionary method after tx is closed
     * <p>
     * we must first go to the active list under the list lock since the dictionary might be there and not in the guava cache anymore.
     * if we found it in the active list we do not update the guava cache again since it means it was recently updated by the thread
     * that added it to the active list.
     * if it was not found in the active list we go to the guava cache and potentially create/load a new dictionary but this is all done
     * under the list concurrent lock (via computeIfAbsent) so we are protected against a race between two threads reaching this state in
     * parallel and creating/loading two instances of the same dictionary
     */
    WriteDictionary getWriteDictionary(DictionaryKey dictionaryKey, RecTypeCode recTypeCode)
    {
        DictionaryId dictionaryId = DictionaryId.of(dictionaryKey);
        DataValueDictionary activeDictionary;

        // protect against other thread removing from the active list when the reference count reaches zero
        readLock.lock();
        try {
            // lookup in active
            activeDictionary = activeDataValuesDictionaries.get(dictionaryId);

            if (activeDictionary != null) {
                activeDictionary.incUsingTransactions();
                return activeDictionary;
            }
        }
        finally {
            readLock.unlock();
        }

        writeLock.lock();
        try {
            // lookup in active
            activeDictionary = activeDataValuesDictionaries.get(dictionaryId);

            // dictionary is not found in active -> lookup in cache for it
            if (activeDictionary == null) {
                // createdTimestamp is unknown -> lookup for last createdTimestamp
                if (dictionaryKey.createdTimestamp() == DictionaryKey.CREATED_TIMESTAMP_UNKNOWN) {
                    long lastCreatedTimestamp = getLastCreatedTimestamp(dictionaryKey.schemaTableColumn(), dictionaryKey.nodeIdentifier());

                    // last createdTimestamp is found -> use it
                    if (lastCreatedTimestamp != DictionaryKey.CREATED_TIMESTAMP_UNKNOWN) {
                        dictionaryKey = new DictionaryKey(dictionaryKey.schemaTableColumn(), dictionaryKey.nodeIdentifier(), lastCreatedTimestamp);
                    }
                }

                DataValueDictionary dataValueDictionary = null;

                // createdTimestamp is known -> lookup in cache for it
                if (dictionaryKey.createdTimestamp() != DictionaryKey.CREATED_TIMESTAMP_UNKNOWN) {
                    dataValueDictionary = cache.getIfPresent(dictionaryKey);
                }

                // dictionary is not found in cache or the dictionary found in cache is immutable -> create new dictionary
                if ((dataValueDictionary == null) || dataValueDictionary.isImmutable()) {
                    long createdTimestamp = Instant.now().toEpochMilli();
                    DictionaryKey createdDictionaryKey = new DictionaryKey(dictionaryKey.schemaTableColumn(), dictionaryKey.nodeIdentifier(), createdTimestamp);

                    logger.debug("create new dictionary. dictionaryKey %s", createdDictionaryKey);
                    int fixedRecTypeLength = calculateFixedRecTypeLength(recTypeCode);

                    // configuring max rec type length same as the fixed one for new dictionary. in vase of varchar it will be zero.
                    dataValueDictionary = new DataValueDictionary(dictionaryConfiguration,
                            createdDictionaryKey,
                            fixedRecTypeLength,
                            fixedRecTypeLength,
                            globalVaradaStatsDictionary);
                    globalVaradaStatsDictionary.incwrite_dictionaries_count();

                    cache.put(createdDictionaryKey, dataValueDictionary);
                    setLastCreatedTimestamp(dictionaryId, createdTimestamp);
                    globalVaradaStatsDictionary.incdictionary_entries();
                }
                activeDictionary = dataValueDictionary;
                activeDataValuesDictionaries.put(dictionaryId, activeDictionary);
                globalVaradaStatsDictionary.incdictionary_active_size();
            }
            activeDictionary.incUsingTransactions();
        }
        finally {
            writeLock.unlock();
        }
        return activeDictionary;
    }

    /**
     * get activeDataValueDictionary from active list if not present will throw Exception
     * param dictionaryKey  - key for cache
     * before calling this method must call getWriteDictionary
     */
    protected DataValueDictionary getActiveDataValueDictionary(DictionaryKey dictionaryKey)
    {
        DataValueDictionary value = activeDataValuesDictionaries.get(DictionaryId.of(dictionaryKey));

        if (value == null) {
            logger.error("dictionaryKey %s was not found in cache", dictionaryKey);
            throw new RuntimeException("dictionaryKey was not found in active list");
        }

        return value;
    }

    /**
     * get readDataValueDictionary from cache if not present will throw Exception
     * param dictionaryKey - key for cache
     */
    protected ReadDictionary getReadDictionaryIfPresent(DictionaryKey dictionaryKey)
    {
        ReadDictionary value = cache.getIfPresent(dictionaryKey);

        if (value == null) {
            logger.error("dictionaryKey %s, was not found in cache", dictionaryKey);
            throw new RuntimeException("dictionaryKey was not found in cache");
        }

        return value;
    }

    protected void releaseDictionary(DictionaryKey dictionaryKey)
    {
        DictionaryId dictionaryId = DictionaryId.of(dictionaryKey);
        DataValueDictionary value = activeDataValuesDictionaries.get(dictionaryId);

        if (value == null) {
            logger.error("dictionaryKey - %s, was not found in cache", dictionaryKey);
            throw new RuntimeException("dictionaryKey was not found in cache");
        }

        if (value.decUsingTransactions() == 0) {
            // lock to protect against other threads trying to remove and threads taking read lock to add to list if ref-count is zero
            writeLock.lock();
            try {
                if (value.getUsingTransactions() == 0) {
                    activeDataValuesDictionaries.remove(dictionaryId);
                    globalVaradaStatsDictionary.adddictionary_active_size(-1);
                }
            }
            finally {
                writeLock.unlock();
            }
        }

        cache.put(dictionaryKey, value); // required in order to update weight
    }

    Map<DebugDictionaryKey, DebugDictionaryMetadata> getWriteDictionaryMetadata()
    {
        return cache.asMap().values().stream().collect(Collectors.toMap(
                x -> new DebugDictionaryKey(x.getDictionaryKey().schemaTableColumn().schemaTableName(),
                        x.getDictionaryKey().schemaTableColumn().varadaColumn().getName(),
                        x.getDictionaryKey().nodeIdentifier()),
                x ->
                        new DebugDictionaryMetadata(new DebugDictionaryKey(x.getDictionaryKey().schemaTableColumn().schemaTableName(),
                                x.getDictionaryKey().schemaTableColumn().varadaColumn().getName(),
                                x.getDictionaryKey().nodeIdentifier()),
                                x.getWriteSize(),
                                getFailedWriteCount(x.getDictionaryKey()))));
    }

    synchronized void resetAllDictionaries()
    {
        cache.asMap().values().forEach(DataValueDictionary::reset);
        dictionaryMetadataMap.clear();
        activeDataValuesDictionaries.clear();
        initCache(new DictionaryCacheConfiguration(
                dictionaryConfiguration.getMaxDictionaryTotalCacheWeight(),
                dictionaryConfiguration.getDictionaryCacheConcurrencyLevel()));
        resetCacheStats();
    }

    private void resetCacheStats()
    {
        globalVaradaStatsDictionary.setdictionary_entries(0);
        globalVaradaStatsDictionary.setdictionaries_weight(0);
        globalVaradaStatsDictionary.setdictionaries_varlen_str_weight(0);
        globalVaradaStatsDictionary.setdictionary_active_size(0);
    }

    int resetMemoryDictionaries(long dictionaryCacheTotalWeight, int concurrency)
    {
        dictionaryMetadataMap.clear();
        activeDataValuesDictionaries.clear();
        int cleanedDictionaries = (int) cache.size();
        initCache(new DictionaryCacheConfiguration(dictionaryCacheTotalWeight, concurrency));
        resetCacheStats();
        return cleanedDictionaries;
    }

    void loadPreBlock(RecTypeCode recTypeCode, DataValueDictionary dataValuesDictionary)
    {
        Block block = convertDictionaryToBlock(dataValuesDictionary.createDictionaryToWrite(), recTypeCode);
        dataValuesDictionary.setDictionaryPreBlock(block);
        logger.debug("create preBlock of size=%s, recTypeCode=%s", block.getPositionCount(), recTypeCode);
    }

    private int calculateFixedRecTypeLength(RecTypeCode recTypeCode)
    {
        return switch (recTypeCode) {
            case REC_TYPE_BIGINT, REC_TYPE_DECIMAL_SHORT -> Long.BYTES;
            case REC_TYPE_INTEGER, REC_TYPE_REAL -> Integer.BYTES;
            case REC_TYPE_VARCHAR, REC_TYPE_ARRAY_BIGINT, REC_TYPE_ARRAY_INT, REC_TYPE_ARRAY_VARCHAR, REC_TYPE_CHAR -> 0; // means slice, each value might have a different length
            default -> throw new UnsupportedOperationException(format("recTypeCode=%s is not supported for dictionary", recTypeCode));
        };
    }

    private Block convertDictionaryToBlock(DictionaryToWrite dictionaryToWrite, RecTypeCode recTypeCode)
    {
        Block block;
        int rowsToFill = dictionaryToWrite.getSize();
        int totalRowsIncludingNull = rowsToFill + 1;
        boolean shouldTrim = false;

        switch (recTypeCode) {
            case REC_TYPE_CHAR:
                shouldTrim = true; // override default false for fixed length string
                // fall through
            case REC_TYPE_VARCHAR: {
                int finalLength = 0;
                for (int i = 0; i < rowsToFill; i++) {
                    finalLength += ((Slice) dictionaryToWrite.get(i)).length();
                }
                SliceOutput newSlice = Slices.allocate(finalLength).getOutput();
                int[] newOffsets = new int[totalRowsIncludingNull + 1];
                boolean[] newValueIsNull = new boolean[totalRowsIncludingNull];
                for (int i = 0; i < rowsToFill; ++i) {
                    Slice slice = (Slice) dictionaryToWrite.get(i);
                    int length = shouldTrim ? SliceUtils.trimSlice(slice.toByteBuffer(), slice.length(), 0) : slice.length();
                    newSlice.writeBytes(slice, 0, length);
                    newOffsets[i + 1] = newSlice.size();
                }
                newValueIsNull[rowsToFill] = true;
                newOffsets[rowsToFill + 1] = newOffsets[rowsToFill];
                block = new VariableWidthBlock(totalRowsIncludingNull, newSlice.slice(), newOffsets, Optional.of(newValueIsNull));
                break;
            }
            case REC_TYPE_DECIMAL_SHORT:
            case REC_TYPE_BIGINT: {
                long[] values = new long[totalRowsIncludingNull]; // including null
                boolean[] nulls = new boolean[totalRowsIncludingNull];
                for (int i = 0; i < rowsToFill; i++) {
                    values[i] = (long) dictionaryToWrite.get(i);
                }
                nulls[rowsToFill] = true;
                block = new LongArrayBlock(totalRowsIncludingNull, Optional.of(nulls), values);
                break;
            }
            case REC_TYPE_INTEGER:
                int[] values = new int[totalRowsIncludingNull]; // including null
                boolean[] nulls = new boolean[totalRowsIncludingNull];
                for (int i = 0; i < rowsToFill; i++) {
                    values[i] = (int) dictionaryToWrite.get(i);
                }
                nulls[rowsToFill] = true;
                block = new IntArrayBlock(totalRowsIncludingNull, Optional.of(nulls), values);
                break;
            default:
                throw new UnsupportedOperationException(format("Unsupported recTypeCode=%s to convert to pre prepared block", recTypeCode));
        }

        return block;
    }

    protected long getDictionaryCacheTotalSize()
    {
        return activeConfiguration.totalWeight();
    }

    protected int getCacheConcurrency()
    {
        return activeConfiguration.concurrency();
    }

    protected Map<String, Integer> getDictionaryCachedKeys()
    {
        return cache.asMap().entrySet().stream()
                .collect(Collectors.toMap(e -> format("%s.%s",
                        e.getKey().schemaTableColumn().schemaTableName(),
                        e.getKey().schemaTableColumn().varadaColumn().getName()), e -> e.getValue().getDictionaryWeight()));
    }

    private record DictionaryCacheConfiguration(
            @SuppressWarnings("unused") long totalWeight,
            @SuppressWarnings("unused") int concurrency) {}

    private record DictionaryId(
            @SuppressWarnings("unused") SchemaTableColumn schemaTableColumn,
            @SuppressWarnings("unused") String nodeIdentifier)
    {
        public static DictionaryId of(SchemaTableColumn schemaTableColumn, String nodeIdentifier)
        {
            return new DictionaryId(schemaTableColumn, nodeIdentifier);
        }

        public static DictionaryId of(DictionaryKey dictionaryKey)
        {
            return of(dictionaryKey.schemaTableColumn(), dictionaryKey.nodeIdentifier());
        }
    }

    private static class DictionaryMetadata
    {
        private final long lastCreatedTimestamp;
        private int failedWriteCount;

        public DictionaryMetadata(long lastCreatedTimestamp)
        {
            this.lastCreatedTimestamp = lastCreatedTimestamp;
            this.failedWriteCount = 0;
        }

        public long getLastCreatedTimestamp()
        {
            return lastCreatedTimestamp;
        }

        public void incFailedWriteCount()
        {
            failedWriteCount++;
        }

        public int getFailedWriteCount()
        {
            return failedWriteCount;
        }
    }
}
