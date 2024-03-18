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

import io.trino.spi.block.Block;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class PredicateCacheData
{
    private final PredicateBufferInfo predicateBufferInfo;
    private final AtomicInteger usedBySplitCount;
    private final Optional<Block> valuesDict;

    public PredicateCacheData(PredicateBufferInfo predicateBufferInfo, Optional<Block> valuesDict)
    {
        this.predicateBufferInfo = predicateBufferInfo;
        this.valuesDict = valuesDict;
        this.usedBySplitCount = new AtomicInteger(0);
    }

    public PredicateBufferInfo getPredicateBufferInfo()
    {
        return predicateBufferInfo;
    }

    public Optional<Block> getValuesDict()
    {
        return valuesDict;
    }

    void incrementUse()
    {
        usedBySplitCount.getAndIncrement();
    }

    void decrementUse()
    {
        usedBySplitCount.getAndDecrement();
    }

    boolean canRemove()
    {
        return usedBySplitCount.get() == 0;
    }

    public boolean isUsed()
    {
        return !canRemove();
    }

    @Override
    public String toString()
    {
        return "PredicateCacheData{" +
                "bufferData=" + predicateBufferInfo +
                ", usedBySplitCount=" + usedBySplitCount +
                '}';
    }
}
