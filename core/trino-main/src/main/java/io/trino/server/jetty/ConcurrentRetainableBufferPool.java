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
package io.trino.server.jetty;

import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import org.eclipse.jetty.io.ByteBufferPool;
import org.eclipse.jetty.io.RetainableByteBuffer;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class ConcurrentRetainableBufferPool
        implements ByteBufferPool
{
    private static final Logger logger = Logger.get(ConcurrentRetainableBufferPool.class);
    private static final int MIN_POOL_SIZE_SHIFT = 7;
    private static final int[] poolSizeShiftToSize;

    private final int numBuckets;
    private final int checkMaxMemoryPoint;
    private final ArenaBucket[] heapBuckets;
    private final ArenaBucket[] offHeapBuckets;
    private final AtomicBoolean evictor = new AtomicBoolean(false); // left from the original jetty code
    private long maxHeapMemory;
    private long maxOffHeapMemory;
    private int checkCount;

    static {
        poolSizeShiftToSize = new int[Integer.SIZE - MIN_POOL_SIZE_SHIFT];
        for (int i = 0; i < poolSizeShiftToSize.length; i++) {
            poolSizeShiftToSize[i] = 1 << i + MIN_POOL_SIZE_SHIFT;
        }
    }

    public ConcurrentRetainableBufferPool()
    {
        this.numBuckets = Runtime.getRuntime().availableProcessors() * 4; // factor of number of tasks
        this.checkMaxMemoryPoint = numBuckets * 100;
        this.maxHeapMemory = DataSize.of(1, MEGABYTE).toBytes();
        this.maxOffHeapMemory = maxHeapMemory;
        this.checkCount = 0;

        heapBuckets = new ArenaBucket[numBuckets];
        offHeapBuckets = new ArenaBucket[numBuckets];
        for (int bucketId = 0; bucketId < numBuckets; bucketId++) {
            heapBuckets[bucketId] = new ArenaBucket(false, bucketId);
            offHeapBuckets[bucketId] = new ArenaBucket(true, bucketId);
        }
    }

    @Override
    public RetainableByteBuffer acquire(int size, boolean isOffHeap)
    {
        ArenaBucket bucket = getBucketArray(isOffHeap)[(int) (Thread.currentThread().threadId() % numBuckets)];
        return bucket.alloc(size);
    }

    private ArenaBucket[] getBucketArray(boolean isOffHeap)
    {
        return isOffHeap ? offHeapBuckets : heapBuckets;
    }

    // similar to the original jetty code
    private void checkMaxMemory(boolean isOffHeap)
    {
        long max = isOffHeap ? maxOffHeapMemory : maxHeapMemory;
        if (max <= 0 || !evictor.compareAndSet(false, true)) {
            return;
        }

        try {
            checkCount++;
            if ((checkCount % checkMaxMemoryPoint == 0) && (getMemory(isOffHeap) > max)) {
                evict(isOffHeap);
            }
        }
        finally {
            evictor.set(false);
        }
    }

    private void evict(boolean isOffHeap)
    {
        for (ArenaBucket bucket : getBucketArray(isOffHeap)) {
            bucket.evict();
        }
    }

    private long getOffHeapMemory()
    {
        return getMemory(true);
    }

    private long getHeapMemory()
    {
        return getMemory(false);
    }

    private long getMemory(boolean isOffHeap)
    {
        return Arrays.stream(getBucketArray(isOffHeap)).mapToLong(bucket -> bucket.getMemory()).sum();
    }

    @Override
    public void clear()
    {
        evict(true);
        evict(false);
    }

    @Override
    public String toString()
    {
        return String.format("%s{heap=%d/%d,offheap=%d/%d}", super.toString(), getHeapMemory(), maxHeapMemory, getOffHeapMemory(), maxOffHeapMemory);
    }

    private class ArenaBucket
    {
        private Arena sharedArena;
        private Arena autoArena;
        private final int bucketId;
        private final boolean isOffHeap;
        private final ArrayList<FixedSizeBufferPool> pools;

        ArenaBucket(boolean isOffHeap, int bucketId)
        {
            this.sharedArena = Arena.ofShared();
            this.autoArena = Arena.ofAuto();
            this.bucketId = bucketId;
            this.isOffHeap = isOffHeap;
            this.pools = new ArrayList<>();
        }

        synchronized RetainableByteBuffer alloc(int size)
        {
            int poolSizeShift = getPoolSizeShift(size);
            if (poolSizeShift >= pools.size()) {
                addNewPools(poolSizeShift);
            }
            // The check for poolSizeShift == 8 is a HACK for bug JDK-8333849 that will be fixed in JDK 24
            Buffer buffer = pools.get(poolSizeShift).alloc((isOffHeap && (poolSizeShift == 8)) ? autoArena : sharedArena);
            return (RetainableByteBuffer) buffer;
        }

        private int getPoolSizeShift(int size)
        {
            return Math.max(MIN_POOL_SIZE_SHIFT, Integer.SIZE - Integer.numberOfLeadingZeros(size - 1)) - MIN_POOL_SIZE_SHIFT;
        }

        private void addNewPools(int poolSizeShift)
        {
            int poolSize = 0;
            for (int i = pools.size(); i <= poolSizeShift; i++) {
                pools.add(new FixedSizeBufferPool(poolSizeShiftToSize[i], isOffHeap));
            }
            updateMaxMemoryIfNeeded(poolSize * 16); // heuristically set the maximum to factor of the maximal buffer size
        }

        private void updateMaxMemoryIfNeeded(int newMaxSize)
        {
            if (isOffHeap) {
                if (newMaxSize > maxOffHeapMemory) {
                    maxOffHeapMemory = newMaxSize;
                }
            }
            else if (newMaxSize > maxHeapMemory) {
                maxHeapMemory = newMaxSize;
            }
        }

        synchronized void evict()
        {
            boolean canClose = isOffHeap;
            for (FixedSizeBufferPool pool : pools) {
                pool.evict();
                canClose &= pool.getBufferCount() == 0;
            }
            if (canClose) {
                sharedArena.close(); // free all memory associated with this arena
                sharedArena = Arena.ofShared(); // restart the arena for new allocations
            }
        }

        synchronized long getMemory()
        {
            return pools.stream().mapToLong(pool -> pool.getMemory()).sum();
        }

        @Override
        public String toString()
        {
            return String.format("%s{bucketId=%d,isOffHeap=%b,#pools=%d}", super.toString(), bucketId, isOffHeap, pools.size());
        }
    }

    // similar to the original jetty code that held a bucket per power of 2
    private class FixedSizeBufferPool
    {
        private final List<MemorySegment> bufferList;
        private final int bufferSize;
        private final boolean isOffHeap;
        private int numAllocatedBuffers;

        FixedSizeBufferPool(int bufferSize, boolean isOffHeap)
        {
            this.bufferList = new ArrayList<>();
            this.bufferSize = bufferSize;
            this.isOffHeap = isOffHeap;
        }

        synchronized Buffer alloc(Arena arena)
        {
            boolean allocateFromArean = bufferList.isEmpty();
            MemorySegment buffer = allocateFromArean ? allocate(arena) : bufferList.remove(0);
            logger.debug("allocated buffer %s", buffer);
            numAllocatedBuffers++;
            return new Buffer(buffer, this);
        }

        synchronized void free(MemorySegment buffer)
        {
            if (numAllocatedBuffers == 0) {
                throw new RuntimeException("pool is already without allocated buffers");
            }
            numAllocatedBuffers--;
            logger.debug("free buffer %s", buffer);
            bufferList.add(buffer);
        }

        synchronized long evict()
        {
            long availableMemory = getAvailableMemory();
            bufferList.clear();
            return availableMemory;
        }

        private MemorySegment allocate(Arena arena)
        {
            return isOffHeap ? arena.allocate(bufferSize, Integer.BYTES) : MemorySegment.ofArray(new byte[bufferSize]);
        }

        long getMemory()
        {
            return (numAllocatedBuffers + bufferList.size()) * (long) bufferSize;
        }

        long getAvailableMemory()
        {
            return bufferList.size() * (long) bufferSize;
        }

        int getBufferCount()
        {
            return numAllocatedBuffers + bufferList.size();
        }

        boolean isOffHeap()
        {
            return isOffHeap;
        }
    }

    // similar to the original jetty code with reference count per buffer for retain
    private class Buffer
            implements RetainableByteBuffer
    {
        private AtomicInteger refCount;
        private MemorySegment buffer;
        private ByteBuffer byteBuffer;
        private FixedSizeBufferPool pool;

        Buffer(MemorySegment buffer, FixedSizeBufferPool pool)
        {
            this.refCount = new AtomicInteger(1);
            this.buffer = buffer;
            this.pool = pool;

            this.byteBuffer = buffer.asByteBuffer();
            byteBuffer.position(0); // this is a requirement to return the byte buffer with these attributes
            byteBuffer.limit(0);    // this is a requirement to return the byte buffer with these attributes
        }

        @Override
        public void retain()
        {
            if (byteBuffer == null) {
                throw new IllegalStateException("buffer cannot be retained since already released");
            }
            refCount.getAndUpdate(c -> c + 1);
        }

        @Override
        public boolean release()
        {
            if (byteBuffer == null) {
                return true; // idiom potent
            }
            boolean shouldRelease = refCount.updateAndGet(c -> c - 1) == 0;
            if (shouldRelease) {
                pool.free(buffer);
                byteBuffer = null; // safety

                checkMaxMemory(pool.isOffHeap());
            }
            return shouldRelease;
        }

        @Override
        public boolean canRetain()
        {
            return true;
        }

        @Override
        public boolean isRetained()
        {
            return refCount.get() > 1;
        }

        @Override
        public ByteBuffer getByteBuffer()
        {
            return byteBuffer;
        }
    }
}
