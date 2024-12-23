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
package io.trino.plugin.memory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import io.trino.spi.cache.PlanSignature;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;

import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.LONG_INSTANCE_SIZE;
import static io.trino.plugin.memory.MemoryCacheManager.MAP_ENTRY_SIZE;
import static java.util.Objects.requireNonNull;

/**
 * Maps objects to numeric id. Comparing of big objects like {@link PlanSignature} can be expensive.
 * Therefore, it's more efficient to map objects to numerical ids and use them for comparison instead.
 */
public class ObjectToIdMap<T>
{
    private final Function<T, Long> retainedSizeInBytesProvider;
    private final BiMap<T, Long> objectToId = HashBiMap.create();
    /**
     * Usage count per id. When non-revocable and revocable usage count
     * for particular id drops to 0, then corresponding mapping from
     * {@link ObjectToIdMap#objectToId} can be dropped.
     */
    private final Long2LongMap idUsageCount = new Long2LongOpenHashMap();
    private final Long2LongMap idRevocableUsageCount = new Long2LongOpenHashMap();
    private long revocableBytes;
    private long nextId;

    public ObjectToIdMap(Function<T, Long> retainedSizeInBytesProvider)
    {
        this.retainedSizeInBytesProvider = requireNonNull(retainedSizeInBytesProvider, "retainedSizeInBytesProvider is null");
    }

    public Optional<Long> getId(T object)
    {
        return Optional.ofNullable(objectToId.get(object));
    }

    public long allocateId(T object)
    {
        return allocateId(object, 1L);
    }

    public long allocateId(T object, long delta)
    {
        return allocateId(object, delta, 0L);
    }

    public long allocateRevocableId(T object)
    {
        return allocateRevocableId(object, 1L);
    }

    public long allocateRevocableId(T object, long delta)
    {
        return allocateId(object, 0L, delta);
    }

    private long allocateId(T object, long delta, long revocableDelta)
    {
        checkArgument(delta >= 0, "delta is negative");
        checkArgument(revocableDelta >= 0, "revocableDelta is negative");
        Long id = objectToId.get(object);
        if (id == null) {
            id = nextId++;
            objectToId.put(object, id);
            idUsageCount.put((long) id, delta);
            idRevocableUsageCount.put((long) id, revocableDelta);
            if (revocableDelta > 0) {
                revocableBytes += getEntrySize(object);
            }
            return id;
        }

        acquireId(id, delta, revocableDelta);
        return id;
    }

    public void acquireId(long id)
    {
        acquireId(id, 1L);
    }

    public void acquireId(long id, long delta)
    {
        acquireId(id, delta, 0L);
    }

    public void acquireRevocableId(long id)
    {
        acquireRevocableId(id, 1L);
    }

    public void acquireRevocableId(long id, long delta)
    {
        acquireId(id, 0L, delta);
    }

    private void acquireId(long id, long delta, long revocableDelta)
    {
        checkArgument(delta >= 0, "delta is negative");
        checkArgument(revocableDelta >= 0, "revocableDelta is negative");
        checkArgument(objectToId.inverse().containsKey(id), "Trying to acquire missing id");

        idUsageCount.mergeLong(id, delta, Long::sum);
        if (revocableDelta > 0 && idRevocableUsageCount.mergeLong(id, revocableDelta, Long::sum) == revocableDelta) {
            revocableBytes += getEntrySize(objectToId.inverse().get(id));
        }
    }

    public void releaseId(long id)
    {
        releaseId(id, 1L);
    }

    public void releaseId(long id, long delta)
    {
        releaseId(id, delta, 0L);
    }

    public void releaseRevocableId(long id)
    {
        releaseRevocableId(id, 1L);
    }

    public void releaseRevocableId(long id, long delta)
    {
        releaseId(id, 0L, delta);
    }

    private void releaseId(long id, long delta, long revocableDelta)
    {
        checkArgument(delta >= 0, "delta is negative");
        checkArgument(revocableDelta >= 0, "revocableDelta is negative");

        long usageCount = idUsageCount.mergeLong(id, -delta, Long::sum);
        checkState(usageCount >= 0, "Usage count is negative");

        long revocableUsageCount = idRevocableUsageCount.mergeLong(id, -revocableDelta, Long::sum);
        checkState(revocableUsageCount >= 0, "Revocable usage count is negative");
        if (revocableDelta > 0 && revocableUsageCount == 0) {
            revocableBytes -= getEntrySize(objectToId.inverse().get(id));
        }

        if (usageCount == 0 && revocableUsageCount == 0) {
            requireNonNull(objectToId.inverse().remove(id));
            idUsageCount.remove(id);
            idRevocableUsageCount.remove(id);
        }
    }

    public long getTotalUsageCount(long id)
    {
        return idUsageCount.getOrDefault(id, 0L) + idRevocableUsageCount.getOrDefault(id, 0L);
    }

    public int size()
    {
        return objectToId.size();
    }

    public long getRevocableBytes()
    {
        return revocableBytes;
    }

    private long getEntrySize(T object)
    {
        return getEntrySize(object, retainedSizeInBytesProvider);
    }

    @VisibleForTesting
    static <T> long getEntrySize(T object, Function<T, Long> retainedSizeInBytesProvider)
    {
        requireNonNull(object, "object is null");
        // account for objectToId
        return MAP_ENTRY_SIZE + retainedSizeInBytesProvider.apply(object) + LONG_INSTANCE_SIZE +
                // account for idUsageCount
                MAP_ENTRY_SIZE + 2L * LONG_INSTANCE_SIZE;
    }
}
