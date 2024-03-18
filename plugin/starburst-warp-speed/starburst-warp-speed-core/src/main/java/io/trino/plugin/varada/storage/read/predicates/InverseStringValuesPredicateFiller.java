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
package io.trino.plugin.varada.storage.read.predicates;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.plugin.varada.juffer.BufferAllocator;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.util.SliceUtils;
import io.trino.plugin.varada.util.StringPredicateData;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.spi.block.Block;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.type.Type;

import java.nio.ByteBuffer;
import java.util.function.Function;

public class InverseStringValuesPredicateFiller
        extends StringValuesPredicateFiller
{
    private static final Logger logger = Logger.get(StringValuesPredicateFiller.class);

    public InverseStringValuesPredicateFiller(StorageEngineConstants storageEngineConstants,
            BufferAllocator bufferAllocator)
    {
        super(storageEngineConstants, bufferAllocator);
    }

    @Override
    public PredicateType getPredicateType()
    {
        return PredicateType.PREDICATE_TYPE_INVERSE_STRING;
    }

    @Override
    void convertString(Domain domain, ByteBuffer lowBuf, ByteBuffer highBuf, int recLength, Function<Slice, Slice> sliceConverter)
    {
        try {
            int numValues = ((SortedRangeSet) domain.getValues()).getRangeCount() - 1;
            lowBuf.position(0);
            highBuf.position(Long.BYTES * numValues);
            Type type = domain.getType();
            Block sortedRangesBlock = ((SortedRangeSet) domain.getValues()).getSortedRanges();
            for (int i = 1; i < numValues * 2; i += 2) { // each value has low, high. upper bound of one range equals the lower bound of the next range
                Slice value = sliceConverter.apply(type.getSlice(sortedRangesBlock, i));
                SliceUtils.StringPredicateDataFactory stringPredicateDataFactory = new SliceUtils.StringPredicateDataFactory();
                StringPredicateData predicateData = stringPredicateDataFactory.create(value, recLength, true);
                lowBuf.putLong(predicateData.comperationValue());
                highBuf.putLong(SliceUtils.str2int(predicateData.value(), predicateData.length(), true));
            }
        }
        catch (Exception e) {
            logger.error(e, "convertString failed");
            throw e;
        }
    }
}
