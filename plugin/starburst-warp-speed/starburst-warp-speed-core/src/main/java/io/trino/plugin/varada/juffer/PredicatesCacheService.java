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
package io.trino.plugin.varada.juffer;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.varada.VaradaErrorCode;
import io.trino.plugin.varada.dispatcher.query.PredicateData;
import io.trino.plugin.varada.dispatcher.query.PredicateInfo;
import io.trino.plugin.varada.dispatcher.query.classifier.PredicateUtil;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.storage.read.predicates.AllPredicateFiller;
import io.trino.plugin.varada.storage.read.predicates.InverseStringValuesPredicateFiller;
import io.trino.plugin.varada.storage.read.predicates.InverseValuesPredicateFiller;
import io.trino.plugin.varada.storage.read.predicates.LucenePredicateFiller;
import io.trino.plugin.varada.storage.read.predicates.NonePredicateFiller;
import io.trino.plugin.varada.storage.read.predicates.PredicateFiller;
import io.trino.plugin.varada.storage.read.predicates.RangesPredicateFiller;
import io.trino.plugin.varada.storage.read.predicates.StringRangesPredicateFiller;
import io.trino.plugin.varada.storage.read.predicates.StringValuesPredicateFiller;
import io.trino.plugin.varada.storage.read.predicates.ValuesPredicateFiller;
import io.trino.plugin.varada.util.DomainUtils;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.plugin.warp.gen.stats.VaradaStatsCachePredicates;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.predicate.Domain;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static io.trino.plugin.varada.VaradaErrorCode.VARADA_PREDICATE_CACHE_ERROR;
import static java.util.Objects.requireNonNull;

@Singleton
public class PredicatesCacheService
{
    public static final String STATS_CACHE_PREDICATE_KEY = "cachePredicates";
    private static final Logger logger = Logger.get(PredicatesCacheService.class);
    private final Map<PredicateBufferPoolType, Map<Integer, PredicateCacheData>> predicateCachePool;
    private final Map<PredicateType, PredicateFiller> predicateTypeToFiller;
    private final BufferAllocator bufferAllocator;
    private final StorageEngineConstants storageEngineConstants;
    private final VaradaStatsCachePredicates varadaStatsCachePredicates;
    private final Lock readLock;
    private final Lock writeLock;

    @Inject
    public PredicatesCacheService(BufferAllocator bufferAllocator,
            StorageEngineConstants storageEngineConstants,
            MetricsManager metricsManager)
    {
        this.bufferAllocator = requireNonNull(bufferAllocator);
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
        ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
        this.readLock = readWriteLock.readLock();
        this.writeLock = readWriteLock.writeLock();
        this.predicateCachePool = new HashMap<>();
        this.predicateTypeToFiller = new HashMap<>();
        initPredicateCachePoll();
        initPredicateFillerMap();
        this.varadaStatsCachePredicates = metricsManager.registerMetric(VaradaStatsCachePredicates.create(STATS_CACHE_PREDICATE_KEY));
    }

    private void initPredicateCachePoll()
    {
        for (PredicateBufferPoolType predicateBufferPoolType : PredicateBufferPoolType.values()) {
            predicateCachePool.put(predicateBufferPoolType, new HashMap<>());
        }
    }

    private void initPredicateFillerMap()
    {
        PredicateFiller predicateFiller = new RangesPredicateFiller(bufferAllocator, storageEngineConstants);
        predicateTypeToFiller.put(predicateFiller.getPredicateType(), predicateFiller);
        predicateFiller = new LucenePredicateFiller(bufferAllocator);
        predicateTypeToFiller.put(predicateFiller.getPredicateType(), predicateFiller);
        PredicateFiller stringPredicate = new StringValuesPredicateFiller(storageEngineConstants, bufferAllocator);
        predicateTypeToFiller.put(stringPredicate.getPredicateType(), stringPredicate);
        PredicateFiller stringRangesPredicate = new StringRangesPredicateFiller(storageEngineConstants, bufferAllocator);
        predicateTypeToFiller.put(stringRangesPredicate.getPredicateType(), stringRangesPredicate);
        predicateFiller = new ValuesPredicateFiller(bufferAllocator);
        predicateTypeToFiller.put(predicateFiller.getPredicateType(), predicateFiller);
        predicateFiller = new AllPredicateFiller(bufferAllocator);
        predicateTypeToFiller.put(predicateFiller.getPredicateType(), predicateFiller);
        predicateFiller = new NonePredicateFiller(bufferAllocator);
        predicateTypeToFiller.put(predicateFiller.getPredicateType(), predicateFiller);
        predicateFiller = new InverseValuesPredicateFiller(bufferAllocator);
        predicateTypeToFiller.put(predicateFiller.getPredicateType(), predicateFiller);
        predicateFiller = new InverseStringValuesPredicateFiller(storageEngineConstants, bufferAllocator);
        predicateTypeToFiller.put(predicateFiller.getPredicateType(), predicateFiller);
    }

    private static boolean canMapMatchCollect(PredicateData predicateData, Domain values)
    {
        if (values == null) {
            return false;
        }

        return PredicateUtil.canMapMatchCollect(predicateData.getColumnType(),
                predicateData.getPredicateInfo().predicateType(),
                predicateData.getPredicateInfo().functionType(),
                values.getValues().getRanges().getRangeCount());
    }

    public Optional<PredicateCacheData> createPredicateCacheData(PredicateData predicateData, Domain values)
    {
        Optional<PredicateCacheData> ret = Optional.empty();
        PredicateBufferInfo predicateBufferInfo = bufferAllocator.allocPredicateBuffer(predicateData.getPredicateSize());
        if (predicateBufferInfo != null) {
            // The dictionary is generated for each predicate and cached with other predicate data, but its utilization is decided per split in createMatchCollect().
            Optional<Block> valuesDict = canMapMatchCollect(predicateData, values) ? DomainUtils.convertDomainToMapBlock(values) : Optional.empty();
            ret = Optional.of(new PredicateCacheData(predicateBufferInfo, valuesDict));
        }
        return ret;
    }

    // utility function that allocates and fills the buffer but does not store it in cache
    public Optional<PredicateCacheData> predicateDataToBuffer(PredicateData predicateData, Object values)
    {
        Optional<PredicateCacheData> predicateCacheData = createPredicateCacheData(predicateData, (Domain) values);
        if (predicateCacheData.isPresent()) {
            PredicateInfo predicateInfo = predicateData.getPredicateInfo();
            ByteBuffer predicateBuffer = bufferAllocator.memorySegment2PredicateBuff(predicateCacheData.get().getPredicateBufferInfo().buff());
            PredicateType predicateType = predicateInfo.predicateType();
            predicateTypeToFiller.get(predicateType).fillPredicate(values, predicateBuffer, predicateData);
        }
        return predicateCacheData;
    }

    public Optional<PredicateCacheData> getOrCreatePredicateBufferId(PredicateData predicateData, Object value)
    {
        Optional<PredicateCacheData> predicateCacheDataOpt = Optional.empty();
        PredicateBufferPoolType predicateBufferPoolType = bufferAllocator.getRequiredPredicateBufferType(predicateData.getPredicateSize());

        // this case is handled by the caller
        if (predicateBufferPoolType == PredicateBufferPoolType.INVALID) {
            return predicateCacheDataOpt;
        }

        Map<Integer, PredicateCacheData> predicateCache = predicateCachePool.get(predicateBufferPoolType);
        readLock.lock();
        try {
            PredicateCacheData predicateCacheData = predicateCache.get(predicateData.getPredicateHashCode());
            if (predicateCacheData != null) {
                predicateCacheDataOpt = Optional.of(predicateCacheData);
                incrementUse(predicateCacheData);
            }
        }
        catch (Exception e) {
            logger.error(e, "failed to get predicate buffer for queryMatchData=%s", predicateData);
            throw e;
        }
        finally {
            readLock.unlock();
        }
        if (predicateCacheDataOpt.isEmpty()) {
            predicateCacheDataOpt = createPredicate(predicateData, value, predicateBufferPoolType);
        }
        return predicateCacheDataOpt; // can be empty if allocation failed since there are no buffers in the pool
    }

    public void markFinished(List<PredicateCacheData> predicateCacheDatas)
    {
        for (PredicateCacheData predicateCacheData : predicateCacheDatas) {
            decrementUse(predicateCacheData);
        }
    }

    private void incrementUse(PredicateCacheData predicateCacheData)
    {
        predicateCacheData.incrementUse();
        updateMetrics(predicateCacheData.getPredicateBufferInfo().predicateBufferPoolType(), true);
    }

    private void decrementUse(PredicateCacheData predicateCacheData)
    {
        predicateCacheData.decrementUse();
        updateMetrics(predicateCacheData.getPredicateBufferInfo().predicateBufferPoolType(), false);
    }

    private void updateMetrics(PredicateBufferPoolType predicateBufferPoolType, boolean increase)
    {
        if (increase) {
            switch (predicateBufferPoolType) {
                case SMALL -> varadaStatsCachePredicates.incin_use_small();
                case MEDIUM -> varadaStatsCachePredicates.incin_use_medium();
                case LARGE -> varadaStatsCachePredicates.incin_use_large();
                default -> throw new TrinoException(VaradaErrorCode.VARADA_ILLEGAL_PARAMETER, "Uknown predicateBufferPoolType " + predicateBufferPoolType);
            }
        }
        else {
            switch (predicateBufferPoolType) {
                case SMALL -> varadaStatsCachePredicates.addin_use_small(-1);
                case MEDIUM -> varadaStatsCachePredicates.addin_use_medium(-1);
                case LARGE -> varadaStatsCachePredicates.addin_use_large(-1);
                default -> throw new TrinoException(VaradaErrorCode.VARADA_ILLEGAL_PARAMETER, "Uknown predicateBufferPoolType " + predicateBufferPoolType);
            }
        }
    }

    private Optional<PredicateCacheData> createPredicate(PredicateData predicateData,
                                                         Object value,
                                                         PredicateBufferPoolType predicateBufferPoolType)
    {
        Optional<PredicateCacheData> predicateCacheDataOpt;
        writeLock.lock();
        try {
            PredicateCacheData predicateCacheData =
                    predicateCachePool.get(predicateBufferPoolType).get(predicateData.getPredicateHashCode());
            if (predicateCacheData == null) {
                // free cache if needed
                if (predicateCachePool.get(predicateBufferPoolType).size() == bufferAllocator.getPoolSize(predicateBufferPoolType)) {
                    logger.warn("predicate cache for %s is full. maxSize=%d. clean old predicates",
                                predicateBufferPoolType, bufferAllocator.getPoolSize(predicateBufferPoolType));
                    freeCache(predicateCachePool.get(predicateBufferPoolType), predicateBufferPoolType);
                }
                // allocate and fill the buffer
                predicateCacheDataOpt = predicateDataToBuffer(predicateData, value);
                if (predicateCacheDataOpt.isPresent()) {
                    predicateCacheData = predicateCacheDataOpt.get();
                    // put in cache
                    predicateCachePool.get(predicateBufferPoolType).put(predicateData.getPredicateHashCode(), predicateCacheData);
                    logger.debug("create new %s, key=%d, cacheSize=%d",
                            predicateCacheData,
                            predicateData.getPredicateHashCode(),
                            predicateCachePool.get(predicateBufferPoolType).size());
                }
            }
            else {
                predicateCacheDataOpt = Optional.of(predicateCacheData);
            }
        }
        catch (Exception e) {
            logger.error(e, "failed to add new predicate to cache");
            throw e;
        }
        finally {
            writeLock.unlock();
        }
        predicateCacheDataOpt.ifPresent(this::incrementUse);
        return predicateCacheDataOpt;
    }

    private void freeCache(Map<Integer, PredicateCacheData> integerPredicateBufferMap, PredicateBufferPoolType predicateBufferPoolType)
    {
        int poolSize = bufferAllocator.getPoolSize(predicateBufferPoolType);
        int removeCount = poolSize < 10 ? 1 : poolSize / 10;

        List<Integer> toRemove = integerPredicateBufferMap
                .entrySet()
                .stream()
                .filter(x -> x.getValue().canRemove())
                .limit(removeCount)
                .map(Map.Entry::getKey).toList();
        if (toRemove.isEmpty()) {
            throw new TrinoException(VARADA_PREDICATE_CACHE_ERROR, String.format("all predicates for type=%s are in use, SHOULD NOT HAPPEN", predicateBufferPoolType));
        }
        toRemove.forEach(key -> {
            PredicateCacheData removed = integerPredicateBufferMap.remove(key);
            bufferAllocator.freePredicateBuffer(removed);
        });
    }

    @VisibleForTesting
    public Map<PredicateBufferPoolType, Map<Integer, PredicateCacheData>> getPredicateCachePool()
    {
        return predicateCachePool;
    }
}
