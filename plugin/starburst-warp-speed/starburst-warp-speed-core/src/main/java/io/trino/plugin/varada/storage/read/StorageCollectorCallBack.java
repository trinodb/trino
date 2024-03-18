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
package io.trino.plugin.varada.storage.read;

import io.trino.plugin.varada.juffer.BufferAllocator;

import java.nio.ByteBuffer;

public class StorageCollectorCallBack
{
    private final StorageCollectorArgs storageCollectorArgs;
    private final BufferAllocator bufferAllocator;
    private int collectStoreCurrSize;

    public StorageCollectorCallBack(StorageCollectorArgs storageCollectorArgs, BufferAllocator bufferAllocator)
    {
        this.storageCollectorArgs = storageCollectorArgs;
        this.bufferAllocator = bufferAllocator;
        this.collectStoreCurrSize = 0;
    }

    // this method is used only as a StorageEngine callback from native code
    @SuppressWarnings("unused")
    void collectStoreState(long stateBuffId, int size)
    {
        ByteBuffer bufferToCopy = bufferAllocator.id2ByteBuff(stateBuffId).slice();
        bufferToCopy.get(storageCollectorArgs.collectStoreBuff(), 0, size);
        collectStoreCurrSize = size;
    }

    // this method is used only as a StorageEngine callback from native code
    // returns the size of the state restored
    @SuppressWarnings("unused")
    int collectRestoreState(long stateBuffId)
    {
        if (collectStoreCurrSize > 0) {
            ByteBuffer targetByteBuffer = bufferAllocator.id2ByteBuff(stateBuffId);
            targetByteBuffer.put(storageCollectorArgs.collectStoreBuff(), 0, collectStoreCurrSize);
        }
        return collectStoreCurrSize;
    }
}
