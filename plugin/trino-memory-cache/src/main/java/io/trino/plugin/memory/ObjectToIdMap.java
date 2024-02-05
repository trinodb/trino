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
     * Usage count per id. When usage count for particular id drops to 0,
     * then corresponding mapping from {@link ObjectToIdMap#objectToId}
     * can be dropped.
     */
    private final Long2LongMap idUsageCount = new Long2LongOpenHashMap();
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

    public long allocateId(T object, long count)
    {
        Long id = objectToId.get(object);
        if (id == null) {
            id = nextId++;
            objectToId.put(object, id);
            idUsageCount.put((long) id, count);
            revocableBytes += getEntrySize(object);
            return id;
        }

        acquireId(id, count);
        return id;
    }

    public void acquireId(long id)
    {
        acquireId(id, 1L);
    }

    public void acquireId(long id, long count)
    {
        idUsageCount.merge(id, count, Long::sum);
    }

    public void releaseId(long id)
    {
        releaseId(id, 1L);
    }

    public void releaseId(long id, long count)
    {
        long usageCount = idUsageCount.merge(id, -count, Long::sum);
        checkState(usageCount >= 0, "Usage count is negative");
        if (usageCount == 0) {
            T object = requireNonNull(objectToId.inverse().remove(id));
            idUsageCount.remove(id);
            revocableBytes -= getEntrySize(object);
        }
    }

    public long getUsageCount(long id)
    {
        return idUsageCount.getOrDefault(id, 0L);
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
        // account for objectToId
        return MAP_ENTRY_SIZE + retainedSizeInBytesProvider.apply(object) + LONG_INSTANCE_SIZE +
                // account for idUsageCount
                MAP_ENTRY_SIZE + 2L * LONG_INSTANCE_SIZE;
    }
}
