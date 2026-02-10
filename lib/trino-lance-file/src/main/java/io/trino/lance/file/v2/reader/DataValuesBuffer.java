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

import io.trino.lance.file.v2.metadata.RepDefLayer;
import io.trino.spi.block.Block;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DataValuesBuffer<T>
{
    private final BufferAdapter<T> bufferAdapter;
    private final List<T> valueBuffers = new ArrayList<>();

    public DataValuesBuffer(BufferAdapter<T> bufferAdapter)
    {
        this.bufferAdapter = requireNonNull(bufferAdapter, "bufferAdapter is null");
    }

    public void append(T buffer)
    {
        valueBuffers.add(buffer);
    }

    public T getMergedValues()
    {
        if (valueBuffers.size() == 1) {
            return valueBuffers.get(0);
        }
        return bufferAdapter.merge(valueBuffers);
    }

    public DecodedPage createDecodedPage(int[] definitions,
            int[] repetitions,
            List<RepDefLayer> layers,
            Optional<Block> dictionary)
    {
        T mergedValues = getMergedValues();
        BaseUnraveler unraveler = new BaseUnraveler(repetitions, definitions, layers.toArray(RepDefLayer[]::new));

        Optional<boolean[]> isNull = unraveler.calculateNulls();
        if (dictionary.isEmpty()) {
            return new DecodedPage(bufferAdapter.createBlock(mergedValues, isNull), unraveler);
        }
        return new DecodedPage(bufferAdapter.createDictionaryBlock(mergedValues, dictionary.get(), isNull), unraveler);
    }

    public void reset()
    {
        valueBuffers.clear();
    }

    public boolean isEmpty()
    {
        return valueBuffers.isEmpty();
    }

    public long getRetainedBytes()
    {
        return valueBuffers.stream().mapToLong(bufferAdapter::getRetainedBytes).sum();
    }
}
