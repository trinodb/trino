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
package io.trino.operator;

import io.airlift.slice.SizeOf;
import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.function.IntFunction;

import static io.airlift.slice.SizeOf.instanceSize;

/**
 * Object registration system that allows looking up objects via stable IDs.
 * <p>
 * This class may recycle deallocated IDs for new allocations.
 */
public class IdRegistry<T>
{
    private static final long INSTANCE_SIZE = instanceSize(IdRegistry.class);

    private final ObjectList<T> objects = new ObjectList<>();
    private final IntFIFOQueue emptySlots = new IntFIFOQueue();

    /**
     * Provides a new ID referencing the provided object.
     *
     * @return ID referencing the provided object
     */
    public int allocateId(IntFunction<T> factory)
    {
        int newId;
        if (!emptySlots.isEmpty()) {
            newId = emptySlots.dequeueInt();
            objects.set(newId, factory.apply(newId));
        }
        else {
            newId = objects.size();
            objects.add(factory.apply(newId));
        }
        return newId;
    }

    public void deallocate(int id)
    {
        objects.set(id, null);
        emptySlots.enqueue(id);
    }

    public T get(int id)
    {
        return objects.get(id);
    }

    /**
     * Does not include the sizes of the referenced objects themselves.
     */
    public long sizeOf()
    {
        return INSTANCE_SIZE + objects.sizeOf() + emptySlots.sizeOf();
    }

    private static class IntFIFOQueue
            extends IntArrayFIFOQueue
    {
        private static final long INSTANCE_SIZE = instanceSize(IntFIFOQueue.class);

        public long sizeOf()
        {
            return INSTANCE_SIZE + SizeOf.sizeOf(array);
        }
    }

    private static class ObjectList<T>
            extends ObjectArrayList<T>
    {
        private static final long INSTANCE_SIZE = instanceSize(ObjectList.class);

        public long sizeOf()
        {
            return INSTANCE_SIZE + SizeOf.sizeOf(a);
        }
    }
}
