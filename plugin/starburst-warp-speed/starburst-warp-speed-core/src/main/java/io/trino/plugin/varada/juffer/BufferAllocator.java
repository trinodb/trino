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
import io.airlift.units.DataSize;
import io.trino.plugin.varada.configuration.NativeConfiguration;
import io.trino.plugin.varada.di.VaradaInitializedServiceRegistry;
import io.trino.plugin.varada.dispatcher.WarmupElementWriteMetadata;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.engine.StorageEngine;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.storage.lucene.LuceneFileType;
import io.trino.plugin.varada.type.TypeUtils;
import io.trino.plugin.varada.util.VaradaInitializedServiceMarker;
import io.trino.plugin.warp.gen.constants.JbufType;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.gen.stats.VaradaStatsBufferAllocator;
import io.trino.spi.type.TinyintType;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.ShortBuffer;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Singleton
public class BufferAllocator
        implements VaradaInitializedServiceMarker
{
    private static final Logger logger = Logger.get(BufferAllocator.class);
    private static final long BUF_ID_OFFSET_MASK = 0x00000000ffffffffL;
    private static final long INVALID_BUF_ID = -1;
    static final String BUFFER_ALLOCATOR_METRICS_GROUP = "BufferAllocator";

    @VisibleForTesting
    public static final int PREDICATE_SMALL_BUF_SIZE = 80; // around 4 bigint ranges or 9 bigint values
    private static final int PREDICATE_SMALL_NUM_BUFFERS = 2048;
    private static final int PREDICATE_MEDIUM_BUF_SIZE = (int) DataSize.of(128, DataSize.Unit.KILOBYTE).toBytes();
    private static final int PREDICATE_MEDIUM_NUM_BUFFERS = 32;
    private static final int PREDICATE_LARGE_BUF_SIZE = (int) DataSize.of(2300, DataSize.Unit.KILOBYTE).toBytes();
    private static final int PREDICATE_LARGE_NUM_BUFFERS = 0; // will be set according to memory available
    private static final int WRITE_BUFFER_SIZE_SPARE_PAGES = 16;

    private final StorageEngine storageEngine;
    private final StorageEngineConstants storageEngineConstants;
    private final MetricsManager metricsManager;
    private final ByteBuffer[] bundles;         // pool of Buffers initialized at startup. native layer manages alloc/free
    private ArrayBlockingQueue<MemorySegment> loadSegmentsQueue; // loadSegment is the bundle, we use it to allocate juffers (record/null/etc)
    private ArrayBlockingQueue<MemorySegment> loadWriteBufferQueue; // loadWriteBuffer is storage engine buffer used to write to disk
    private final NativeConfiguration nativeConfiguration;
    private final int maxRecLenForVarlenRecordBuffer;
    private final int maxRecLenForFixedRecordBuffer;
    private final int maxRecLenForVarlenTxSize;
    private final int maxRecLenForFixedTxSize;
    private final int queryStringNullValueSize;
    private int extRecBuffSize;
    private int dataTempBufferSize;
    private int indexTempBufferSize;
    private int warmBundleSize;
    private int warmWriteBufferSize;
    private final VaradaStatsBufferAllocator stats;
    private PredicateBufferPool[] predicateBufferPools;
    private int[] buffTypeSizes;
    private int[] fixedRecordBufferSizes;
    private int[] varlenRecordBufferSizes;
    private int[] fixedCollectTxSizes;
    private int[] varlenCollectTxSizes;

    @Inject
    public BufferAllocator(StorageEngine storageEngine,
            StorageEngineConstants storageEngineConstants,
            NativeConfiguration nativeConfiguration,
            MetricsManager metricsManager,
            VaradaInitializedServiceRegistry varadaInitializedServiceRegistry)
    {
        // services
        this.storageEngine = requireNonNull(storageEngine);
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
        this.metricsManager = requireNonNull(metricsManager);
        this.nativeConfiguration = requireNonNull(nativeConfiguration);
        varadaInitializedServiceRegistry.addService(this);

        // constants
        this.maxRecLenForVarlenRecordBuffer = storageEngineConstants.getFixedLengthStringLimit();
        this.maxRecLenForFixedRecordBuffer = 2 * Long.BYTES; // long decimal
        this.maxRecLenForVarlenTxSize = storageEngineConstants.getMaxRecLen();
        this.maxRecLenForFixedTxSize = 2 * Long.BYTES; // long decimal
        this.queryStringNullValueSize = storageEngineConstants.getQueryStringNullValueSize();

        initBufferTypeSizes();
        initWarmBundles();
        initWarmWriteBuffer();
        initPredicateBundle();

        // read bundles
        bundles = new ByteBuffer[storageEngineConstants.getNumBundles()];
        for (int bufIx = 0; bufIx < bundles.length; bufIx++) {
            bundles[bufIx] = storageEngine.getBundleFromPool(bufIx);
            if (bundles[bufIx] != null) {
                bundles[bufIx].order(ByteOrder.LITTLE_ENDIAN);
            }
        }

        logger.info("loadSegmentsSize %d warmBundleSize %d warmWriteBufferSize %d readNumBundles %d predicateBundleSize %dMB",
                loadSegmentsQueue.size(), warmBundleSize, warmWriteBufferSize, bundles.length, nativeConfiguration.getPredicateBundleSizeInMegaBytes());
        this.stats = VaradaStatsBufferAllocator.create(BUFFER_ALLOCATOR_METRICS_GROUP);
    }

    private void initWarmBundles()
    {
        final long alignment = storageEngineConstants.getPageSize();
        final int numSegments = nativeConfiguration.getTaskMaxWorkerThreads();
        checkArgument(numSegments > 0, "no segments configured for warm bundles");
        final long allocSize = (long) warmBundleSize * numSegments + alignment;

        SegmentAllocator nativeAllocator = SegmentAllocator.slicingAllocator(Arena.global().allocate(allocSize, alignment));
        ArrayList<MemorySegment> segmentList = new ArrayList<>(numSegments);
        for (int i = 0; i < numSegments; i++) {
            segmentList.add(nativeAllocator.allocate(warmBundleSize, alignment));
        }

        loadSegmentsQueue = new ArrayBlockingQueue<>(segmentList.size(), true, segmentList);
    }

    private void initWarmWriteBuffer()
    {
        // 3 for records, extended records and metadata (we take spare for metadata)
        int dataWriteBufferSize = storageEngineConstants.getRecordBufferMaxSize() * 3;
        int basicWriteBufferSize = storageEngineConstants.getIndexChunkMaxSize();

        final long alignment = storageEngineConstants.getPageSize();
        final int numSegments = nativeConfiguration.getTaskMaxWorkerThreads();
        checkArgument(numSegments > 0, "no segments configured for warm write buffers");

        // we take WRITE_BUFFER_SIZE_SPARE_PAGES from the start and from the end so we multiply by 2
        this.warmWriteBufferSize = Math.max(dataWriteBufferSize, basicWriteBufferSize);
        final long spareWriteBufferSize = WRITE_BUFFER_SIZE_SPARE_PAGES * alignment;
        final long allocSize = (((long) warmWriteBufferSize) + spareWriteBufferSize * 2) * numSegments + alignment;

        SegmentAllocator nativeAllocator = SegmentAllocator.slicingAllocator(Arena.global().allocate(allocSize, alignment));
        ArrayList<MemorySegment> segmentList = new ArrayList<>(numSegments);
        for (int i = 0; i < numSegments; i++) {
            nativeAllocator.allocate(spareWriteBufferSize, alignment); // allocate spare from the beginning
            segmentList.add(nativeAllocator.allocate(warmWriteBufferSize, alignment));
            nativeAllocator.allocate(spareWriteBufferSize, alignment); // allocate spare from the end
        }

        loadWriteBufferQueue = new ArrayBlockingQueue<>(segmentList.size(), true, segmentList);
    }

    private void initPredicateBundle()
    {
        this.predicateBufferPools = new PredicateBufferPool[PredicateBufferPoolType.values().length];

        final long alignment = storageEngineConstants.getPageSize();
        final long predicateBundleSize = (long) (nativeConfiguration.getPredicateBundleSizeInMegaBytes() << 20) + alignment;

        SegmentAllocator poolSlicer = SegmentAllocator.slicingAllocator(Arena.global().allocate(predicateBundleSize, alignment));
        predicateBufferPools[PredicateBufferPoolType.SMALL.ordinal()] = new PredicateBufferPool(PredicateBufferPoolType.SMALL,
                PREDICATE_SMALL_BUF_SIZE,
                PREDICATE_SMALL_NUM_BUFFERS,
                poolSlicer);
        predicateBufferPools[PredicateBufferPoolType.MEDIUM.ordinal()] = new PredicateBufferPool(PredicateBufferPoolType.MEDIUM,
                PREDICATE_MEDIUM_BUF_SIZE,
                PREDICATE_MEDIUM_NUM_BUFFERS,
                poolSlicer);
        predicateBufferPools[PredicateBufferPoolType.LARGE.ordinal()] = new PredicateBufferPool(PredicateBufferPoolType.LARGE,
                PREDICATE_LARGE_BUF_SIZE,
                PREDICATE_LARGE_NUM_BUFFERS,
                poolSlicer);
    }

    private void initBufferTypeSizes()
    {
        int chunkSize = 1 << storageEngineConstants.getChunkSizeShift();
        buffTypeSizes = new int[JbufType.JBUF_TYPE_NUM_OF.ordinal()];
        buffTypeSizes[JbufType.JBUF_TYPE_NULL.ordinal()] = chunkSize;
        buffTypeSizes[JbufType.JBUF_TYPE_CHUNKS_MAP.ordinal()] = storageEngineConstants.getChunksMapSize();
        buffTypeSizes[JbufType.JBUF_TYPE_SKIPLIST.ordinal()] = roundToPageSize(((chunkSize / storageEngineConstants.getVarlenMdGranularity()) + 1) * Integer.BYTES);

        buffTypeSizes[JbufType.JBUF_TYPE_LUCENE_SMALL_SI.ordinal()] = storageEngineConstants.getLuceneSmallJufferSize();
        buffTypeSizes[JbufType.JBUF_TYPE_LUCENE_SMALL_CFE.ordinal()] = storageEngineConstants.getLuceneSmallJufferSize();
        buffTypeSizes[JbufType.JBUF_TYPE_LUCENE_SMALL_SEGMENTS.ordinal()] = storageEngineConstants.getLuceneSmallJufferSize();
        buffTypeSizes[JbufType.JBUF_TYPE_LUCENE_BIG_CFS.ordinal()] = storageEngineConstants.getLuceneBigJufferSize();

        this.dataTempBufferSize = storageEngineConstants.getWarmupDataTempBufferSize();
        this.indexTempBufferSize = storageEngineConstants.getWarmupIndexTempBufferSize();

        this.extRecBuffSize = storageEngineConstants.getRecordBufferMaxSize();
        this.fixedRecordBufferSizes = new int[maxRecLenForFixedRecordBuffer + 1]; // largest case is long decimal
        this.varlenRecordBufferSizes = new int[maxRecLenForVarlenRecordBuffer + 1];
        storageEngine.initRecordBufferSizes(fixedRecordBufferSizes, varlenRecordBufferSizes);
        this.fixedCollectTxSizes = new int[maxRecLenForFixedTxSize + 1]; // largest case is long decimal
        this.varlenCollectTxSizes = new int[maxRecLenForVarlenTxSize + 1];
        storageEngine.initCollectTxSizes(fixedCollectTxSizes, varlenCollectTxSizes);

        int warmBufferSize = buffTypeSizes[JbufType.JBUF_TYPE_NULL.ordinal()] +
                buffTypeSizes[JbufType.JBUF_TYPE_CHUNKS_MAP.ordinal()] +
                storageEngineConstants.getPageSize() * 4; /* some page for skiplist and spare */
        int dataWarmBufferSize = warmBufferSize + fixedRecordBufferSizes[maxRecLenForVarlenRecordBuffer] + extRecBuffSize + dataTempBufferSize;
        int basicWarmBufferSize = warmBufferSize + calculateCrcBufferSize(RecTypeCode.REC_TYPE_DECIMAL_LONG, maxRecLenForFixedTxSize) + indexTempBufferSize;
        this.warmBundleSize = Math.max(dataWarmBufferSize, basicWarmBufferSize);
    }

    @Override
    public void init()
    {
        metricsManager.registerMetric(this.stats);
        updateStats(loadSegmentsQueue.size());
        stats.addallowed_loaders(loadSegmentsQueue.size());
    }

    public int getPoolSize(PredicateBufferPoolType predicateBufferPoolType)
    {
        return predicateBufferPools[predicateBufferPoolType.ordinal()].getPoolSize();
    }

    public int getBufferSize(PredicateBufferPoolType predicateBufferPoolType)
    {
        return predicateBufferPools[predicateBufferPoolType.ordinal()].getBufSize();
    }

    // return array of pointers and array of sizes
    public MemorySegment[] getWarmBuffers(WarmUpElementAllocationParams weAllocParams)
    {
        if (weAllocParams.memorySegment() == null) {
            throw new RuntimeException("memory segment is null");
        }
        SegmentAllocator slicer = SegmentAllocator.slicingAllocator(weAllocParams.memorySegment());
        final long alignment = storageEngineConstants.getPageSize();
        MemorySegment[] buffs = new MemorySegment[JbufType.JBUF_TYPE_NUM_OF.ordinal()];

        if (weAllocParams.isRecBufferNeeded()) {
            buffs[JbufType.JBUF_TYPE_REC.ordinal()] = slicer.allocate(weAllocParams.recBuffSize(), alignment);
            if (weAllocParams.extRecBuffSize() > 0) {
                buffs[JbufType.JBUF_TYPE_EXT_RECS.ordinal()] = slicer.allocate(weAllocParams.extRecBuffSize(), alignment);
            }
            // data type
            buffs[JbufType.JBUF_TYPE_TEMP.ordinal()] = slicer.allocate(dataTempBufferSize, alignment);
        }
        else {
            // for all index types
            buffs[JbufType.JBUF_TYPE_TEMP.ordinal()] = slicer.allocate(indexTempBufferSize, alignment);
        }

        if (weAllocParams.isCrcBufferNeeded()) {
            buffs[JbufType.JBUF_TYPE_CRC.ordinal()] = slicer.allocate(weAllocParams.crcBuffSize(), alignment);
        }

        buffs[JbufType.JBUF_TYPE_NULL.ordinal()] = slicer.allocate(buffTypeSizes[JbufType.JBUF_TYPE_NULL.ordinal()], alignment);

        if (weAllocParams.isMdBufferNeeded()) {
            buffs[JbufType.JBUF_TYPE_SKIPLIST.ordinal()] = slicer.allocate(buffTypeSizes[JbufType.JBUF_TYPE_SKIPLIST.ordinal()], alignment);
        }

        if (weAllocParams.isLuceneIndexNeeded()) {
            slicer.allocate(32, alignment); // @TODO KOBI - lucnee header size
            buffs[JbufType.JBUF_TYPE_LUCENE_SMALL_SI.ordinal()] = slicer.allocate(buffTypeSizes[JbufType.JBUF_TYPE_LUCENE_SMALL_SI.ordinal()]);
            buffs[JbufType.JBUF_TYPE_LUCENE_SMALL_CFE.ordinal()] = slicer.allocate(buffTypeSizes[JbufType.JBUF_TYPE_LUCENE_SMALL_CFE.ordinal()]);
            buffs[JbufType.JBUF_TYPE_LUCENE_SMALL_SEGMENTS.ordinal()] = slicer.allocate(buffTypeSizes[JbufType.JBUF_TYPE_LUCENE_SMALL_SEGMENTS.ordinal()]);
            buffs[JbufType.JBUF_TYPE_LUCENE_BIG_CFS.ordinal()] = slicer.allocate(buffTypeSizes[JbufType.JBUF_TYPE_LUCENE_BIG_CFS.ordinal()], alignment);
        }

        buffs[JbufType.JBUF_TYPE_CHUNKS_MAP.ordinal()] = slicer.allocate(buffTypeSizes[JbufType.JBUF_TYPE_CHUNKS_MAP.ordinal()], alignment);

        return buffs;
    }

    public long[] getQueryIdsArray(boolean lucene)
    {
        long[] idsArray = new long[JbufType.JBUF_TYPE_QUERY_NUM_OF.ordinal()];
        if (!lucene) {
            idsArray[JbufType.JBUF_TYPE_LUCENE_SMALL_SI.ordinal()] = INVALID_BUF_ID;
            idsArray[JbufType.JBUF_TYPE_LUCENE_SMALL_CFE.ordinal()] = INVALID_BUF_ID;
            idsArray[JbufType.JBUF_TYPE_LUCENE_SMALL_SEGMENTS.ordinal()] = INVALID_BUF_ID;
            idsArray[JbufType.JBUF_TYPE_LUCENE_BIG_CFS.ordinal()] = INVALID_BUF_ID;
        }
        return idsArray;
    }

    public ByteBuffer id2ByteBuff(long bufId)
    {
        return id2Buff(bufId);
    }

    public ByteBuffer ids2RecBuff(long[] idsByType)
    {
        return id2Buff(idsByType[JbufType.JBUF_TYPE_REC.ordinal()]);
    }

    public ByteBuffer memorySegment2RecBuff(MemorySegment[] buffs)
    {
        return memorySegment2ByteBuffer(buffs[JbufType.JBUF_TYPE_REC.ordinal()]);
    }

    public ByteBuffer memorySegment2ExtRecsBuff(MemorySegment[] buffs)
    {
        return memorySegment2ByteBuffer(buffs[JbufType.JBUF_TYPE_EXT_RECS.ordinal()]);
    }

    public ByteBuffer ids2NullBuff(long[] idsByType)
    {
        return id2Buff(idsByType[JbufType.JBUF_TYPE_NULL.ordinal()]);
    }

    public ByteBuffer memorySegment2NullBuff(MemorySegment[] buffs)
    {
        return memorySegment2ByteBuffer(buffs[JbufType.JBUF_TYPE_NULL.ordinal()]);
    }

    public ByteBuffer memorySegment2ChunksMapBuff(MemorySegment[] buffs)
    {
        return memorySegment2ByteBuffer(buffs[JbufType.JBUF_TYPE_CHUNKS_MAP.ordinal()]);
    }

    public ShortBuffer ids2RowsBuff(long id)
    {
        return id2Buff(id).asShortBuffer();
    }

    public IntBuffer ids2RecordBufferStateBuff(long id)
    {
        return id2Buff(id).asIntBuffer();
    }

    public ByteBuffer memorySegment2CrcBuff(MemorySegment[] buffs)
    {
        return memorySegment2ByteBuffer(buffs[JbufType.JBUF_TYPE_CRC.ordinal()]);
    }

    public ByteBuffer memorySegment2PredicateBuff(MemorySegment buff)
    {
        return memorySegment2ByteBuffer(buff);
    }

    public ByteBuffer ids2LuceneSmallCfeBuff(long[] idsByType)
    {
        return id2Buff(idsByType[JbufType.JBUF_TYPE_LUCENE_SMALL_CFE.ordinal()]);
    }

    public ByteBuffer ids2LuceneSmallSiBuff(long[] idsByType)
    {
        return id2Buff(idsByType[JbufType.JBUF_TYPE_LUCENE_SMALL_SI.ordinal()]);
    }

    public ByteBuffer ids2LuceneSmallSegmentsBuff(long[] idsByType)
    {
        return id2Buff(idsByType[JbufType.JBUF_TYPE_LUCENE_SMALL_SEGMENTS.ordinal()]);
    }

    public ByteBuffer ids2LuceneBigCfsBuff(long[] idsByType)
    {
        return id2Buff(idsByType[JbufType.JBUF_TYPE_LUCENE_BIG_CFS.ordinal()]);
    }

    public ByteBuffer[] ids2LuceneBuffers(long[] idsByType)
    {
        ByteBuffer[] luceneBuffers = new ByteBuffer[4];
        luceneBuffers[LuceneFileType.SI.getNativeId()] = ids2LuceneSmallSiBuff(idsByType);
        luceneBuffers[LuceneFileType.CFE.getNativeId()] = ids2LuceneSmallCfeBuff(idsByType);
        luceneBuffers[LuceneFileType.SEGMENTS.getNativeId()] = ids2LuceneSmallSegmentsBuff(idsByType);
        luceneBuffers[LuceneFileType.CFS.getNativeId()] = ids2LuceneBigCfsBuff(idsByType);
        return luceneBuffers;
    }

    public ByteBuffer memorySegment2LuceneSmallCfeBuff(MemorySegment[] buffs)
    {
        return memorySegment2ByteBuffer(buffs[JbufType.JBUF_TYPE_LUCENE_SMALL_CFE.ordinal()]);
    }

    public ByteBuffer memorySegment2LuceneSmallSiBuff(MemorySegment[] buffs)
    {
        return memorySegment2ByteBuffer(buffs[JbufType.JBUF_TYPE_LUCENE_SMALL_SI.ordinal()]);
    }

    public ByteBuffer memorySegment2LuceneSmallSegmentsBuff(MemorySegment[] buffs)
    {
        return memorySegment2ByteBuffer(buffs[JbufType.JBUF_TYPE_LUCENE_SMALL_SEGMENTS.ordinal()]);
    }

    public ByteBuffer memorySegment2LuceneBigCfsBuff(MemorySegment[] buffs)
    {
        return memorySegment2ByteBuffer(buffs[JbufType.JBUF_TYPE_LUCENE_BIG_CFS.ordinal()]);
    }

    public ByteBuffer[] memorySegment2LuceneBuffers(MemorySegment[] buffs)
    {
        ByteBuffer[] luceneBuffers = new ByteBuffer[4];
        luceneBuffers[LuceneFileType.SI.getNativeId()] = memorySegment2LuceneSmallSiBuff(buffs);
        luceneBuffers[LuceneFileType.CFE.getNativeId()] = memorySegment2LuceneSmallCfeBuff(buffs);
        luceneBuffers[LuceneFileType.SEGMENTS.getNativeId()] = memorySegment2LuceneSmallSegmentsBuff(buffs);
        luceneBuffers[LuceneFileType.CFS.getNativeId()] = memorySegment2LuceneBigCfsBuff(buffs);
        return luceneBuffers;
    }

    public ByteBuffer ids2LuceneResultBM(long[] idsByType)
    {
        return id2Buff(idsByType[JbufType.JBUF_TYPE_LUCENE_MATCH_BM.ordinal()]);
    }

    public IntBuffer memorySegment2VarlenMdBuff(MemorySegment[] buffs)
    {
        return memorySegment2ByteBuffer(buffs[JbufType.JBUF_TYPE_SKIPLIST.ordinal()]).asIntBuffer();
    }

    private ByteBuffer id2Buff(long bufId)
    {
        ByteBuffer buff = createBuffView(bundles[(int) (bufId >> 32)]);
        buff.position((int) (bufId & BUF_ID_OFFSET_MASK));
        return buff;
    }

    private ByteBuffer memorySegment2ByteBuffer(MemorySegment buff)
    {
        return createBuffView(buff.asByteBuffer());
    }

    public ByteBuffer createBuffView(ByteBuffer buf)
    {
        return buf.duplicate().order(ByteOrder.LITTLE_ENDIAN);
    }

    public PredicateBufferInfo allocPredicateBuffer(int size)
    {
        PredicateBufferInfo ret = null;

        for (int i = 1; i < predicateBufferPools.length; i++) {
            MemorySegment buff = predicateBufferPools[i].alloc(size);
            if (buff != null) {
                predicateBufferUpdateAllocStats(PredicateBufferPoolType.values()[i], true);
                ret = new PredicateBufferInfo(buff, PredicateBufferPoolType.values()[i]);
                break;
            }
        }
        return ret;
    }

    public PredicateBufferPoolType getRequiredPredicateBufferType(int size)
    {
        for (int i = 1; i < predicateBufferPools.length; i++) {
            if (predicateBufferPools[i].canHandle(size)) {
                return predicateBufferPools[i].getPredicateBufferPoolType();
            }
        }
        return PredicateBufferPoolType.INVALID;
    }

    public void freePredicateBuffer(PredicateCacheData predicateCacheData)
    {
        int poolId = predicateCacheData.getPredicateBufferInfo().predicateBufferPoolType().ordinal();
        predicateBufferPools[poolId].free(predicateCacheData.getPredicateBufferInfo().buff());
        predicateBufferUpdateAllocStats(PredicateBufferPoolType.values()[poolId], false);
    }

    private void predicateBufferUpdateAllocStats(PredicateBufferPoolType predicateBuffer, boolean alloc)
    {
        int diff = alloc ? 1 : -1;
        switch (predicateBuffer) {
            case SMALL -> stats.addpredicate_buffer_small_alloc(diff);
            case MEDIUM -> stats.addpredicate_buffer_medium_alloc(diff);
            case LARGE -> stats.addpredicate_buffer_large_alloc(diff);
            default -> throw new RuntimeException("unknown predicateBuffer code " + predicateBuffer);
        }
    }

    public void readerOnAllocBundle()
    {
        stats.addreader_taken(1);
        updateStats(-1);
    }

    public void readerOnFreeBundle()
    {
        stats.addreader_taken(-1);
        updateStats(1);
    }

    private void updateStats(long bundles)
    {
        stats.addavailable_load_bundles(bundles);
    }

    public MemorySegment allocateLoadSegment()
    {
        return loadSegmentsQueue.remove();
    }

    public void freeLoadSegment(MemorySegment loadSegment)
    {
        checkArgument(loadSegment != null, "warmingCacheData must be set");
        loadSegmentsQueue.add(loadSegment);
    }

    public MemorySegment allocateLoadWriteBuffer()
    {
        return loadWriteBufferQueue.remove();
    }

    public void freeLoadWriteBuffer(MemorySegment loadWriteBuffer)
    {
        checkArgument(loadWriteBuffer != null, "warmingCacheData must be set");
        loadWriteBufferQueue.add(loadWriteBuffer);
    }

    public WarmUpElementAllocationParams calculateAllocationParams(WarmupElementWriteMetadata warmupElementWriteMetadata, MemorySegment loadSegment)
    {
        WarmUpElement warmUpElement = warmupElementWriteMetadata.warmUpElement();
        RecTypeCode recTypeCode = warmUpElement.getRecTypeCode();
        int recTypeLength = warmUpElement.getRecTypeLength();

        boolean isRecBufferNeeded = isRecordBufferNeeded(warmUpElement.getWarmUpType(), recTypeCode);
        return new WarmUpElementAllocationParams(recTypeCode,
                recTypeLength,
                isRecBufferNeeded ? getInsertRecordBufferSize(recTypeLength) : 0,
                isCrcBufferNeeded(warmUpElement.getWarmUpType(), recTypeCode) ? calculateCrcBufferSize(recTypeCode, recTypeLength) : 0,
                isRecBufferNeeded && isExtendedBufferNeeded(recTypeLength) ? extRecBuffSize : 0,
                isRecBufferNeeded && isMdBufferNeeded(recTypeCode),
                isLuceneBuffersNeeded(warmUpElement.getWarmUpType()),
                loadSegment);
    }

    // NOTE: this is called only for fixed length since variable length in insert is only above maxRecLenForVarlenRecordBuffer
    public int getInsertRecordBufferSize(int recTypeLength)
    {
        return getFixedLengthRecordBufferSize(recTypeLength, maxRecLenForVarlenRecordBuffer);
    }

    public int getCollectRecordBufferSize(RecTypeCode recTypeCode, int recTypeLength)
    {
        if (TypeUtils.isVarlenStr(recTypeCode)) {
            return varlenRecordBufferSizes[Math.min(recTypeLength, maxRecLenForVarlenRecordBuffer)];
        }
        return getFixedLengthRecordBufferSize(recTypeLength, maxRecLenForVarlenRecordBuffer);
    }

    // NOTE: this is called only for fixed length since variable length does not support match collect (index is crc based)
    public int getMatchCollectRecordBufferSize(int recTypeLength)
    {
        return getFixedLengthRecordBufferSize(recTypeLength, maxRecLenForFixedRecordBuffer);
    }

    public int getMappedMatchCollectBufferSize()
    {
        return getFixedLengthRecordBufferSize(TinyintType.TINYINT.getFixedSize(), maxRecLenForFixedRecordBuffer);
    }

    private int getFixedLengthRecordBufferSize(int recTypeLength, int maxRecTypeLength)
    {
        return fixedRecordBufferSizes[Math.min(recTypeLength, maxRecTypeLength)];
    }

    public int getCollectTxSize(RecTypeCode recTypeCode, int recTypeLength)
    {
        if (TypeUtils.isVarlenStr(recTypeCode)) {
            return varlenCollectTxSizes[Math.min(recTypeLength, maxRecLenForVarlenTxSize)];
        }
        return fixedCollectTxSizes[Math.min(recTypeLength, maxRecLenForFixedTxSize)];
    }

    public int getQueryNullBufferSize(RecTypeCode recTypeCode)
    {
        return buffTypeSizes[JbufType.JBUF_TYPE_NULL.ordinal()] * (TypeUtils.isVarlenStr(recTypeCode) ? queryStringNullValueSize : 1);
    }

    private int calculateCrcBufferSize(RecTypeCode recTypeCode, int recTypeLength)
    {
        int recSize;
        if (recTypeCode == RecTypeCode.REC_TYPE_DECIMAL_LONG) {
            recSize = recTypeLength;
        }
        else {
            recSize = switch (recTypeLength) {
                case 1 -> Byte.BYTES;
                case 2 -> Short.BYTES;
                case 4 -> Integer.BYTES;
                default -> Long.BYTES;
            };
        }
        return (recSize + Short.BYTES) * (1 << storageEngineConstants.getChunkSizeShift());
    }

    private boolean isExtendedBufferNeeded(int recTypeLength)
    {
        return recTypeLength > storageEngineConstants.getVarlenExtLimit();
    }

    private boolean isMdBufferNeeded(RecTypeCode recTypeCode)
    {
        return TypeUtils.isVarlenStr(recTypeCode);
    }

    private boolean isLuceneBuffersNeeded(WarmUpType columnWarmUpType)
    {
        return WarmUpType.WARM_UP_TYPE_LUCENE.equals(columnWarmUpType);
    }

    private boolean isRecordBufferNeeded(WarmUpType columnWarmUpType, RecTypeCode recTypeCode)
    {
        return WarmUpType.WARM_UP_TYPE_DATA.equals(columnWarmUpType) || (recTypeCode == RecTypeCode.REC_TYPE_BOOLEAN);
    }

    private boolean isCrcBufferNeeded(WarmUpType columnWarmUpType, RecTypeCode recTypeCode)
    {
        return (WarmUpType.WARM_UP_TYPE_BASIC.equals(columnWarmUpType) || columnWarmUpType.bloom()) && (recTypeCode != RecTypeCode.REC_TYPE_BOOLEAN);
    }

    private int roundToPageSize(int size)
    {
        int pageSize = storageEngineConstants.getPageSize();

        if (pageSize == 0) {
            return 0;
        }
        return ((size + pageSize - 1) / pageSize) * pageSize;
    }
}
