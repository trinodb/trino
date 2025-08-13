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
package io.trino.lance.file.v2.reader;

import io.trino.spi.block.Block;

import java.util.List;
import java.util.Optional;

public interface BufferAdapter<T>
{
    T createBuffer(int size);

    void copy(T source, int sourceIndex, T destination, int destinationIndex, int length);

    T merge(List<T> buffers);

    Block createBlock(T buffer, Optional<boolean[]> valueIsNull);

    long getRetainedBytes(T buffer);

    default Block createDictionaryBlock(T buffer, Block dictionary, Optional<boolean[]> valueIsNull)
    {
        throw new UnsupportedOperationException();
    }
}
