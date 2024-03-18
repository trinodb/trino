
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

import io.airlift.slice.Slice;
import io.trino.plugin.varada.dispatcher.query.PredicateData;
import io.trino.plugin.varada.juffer.BufferAllocator;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.type.TypeUtils;
import io.trino.plugin.varada.util.SliceUtils;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.type.Type;

import java.nio.ByteBuffer;
import java.util.function.Function;

public class StringRangesPredicateFiller
        extends PredicateFiller<Domain>
{
    private final StorageEngineConstants storageEngineConstants;
    private final RangesConverter rangesConverter;

    public StringRangesPredicateFiller(StorageEngineConstants storageEngineConstants, BufferAllocator bufferAllocator)
    {
        super(bufferAllocator);
        this.storageEngineConstants = storageEngineConstants;
        this.rangesConverter = new RangesConverter();
    }

    @Override
    public PredicateType getPredicateType()
    {
        return PredicateType.PREDICATE_TYPE_STRING_RANGES;
    }

    @Override
    public void fillPredicate(Domain value, ByteBuffer predicateBuffer, PredicateData predicateData)
    {
        predicateBuffer = writePredicateInfoToBuffer(predicateBuffer, predicateData);
        convertValues(value, predicateBuffer);
    }

    @Override
    public void convertValues(Domain value, ByteBuffer predicateBuffer)
    {
        Type type = value.getType();
        int typeLength = TypeUtils.getTypeLength(type, storageEngineConstants.getVarcharMaxLen());
        Function<Slice, Slice> sliceConverter =
                SliceUtils.getSliceConverter(type, typeLength, typeLength <= storageEngineConstants.getFixedLengthStringLimit(), true);

        ByteBuffer predicateBufferLow = bufferAllocator.createBuffView(predicateBuffer);
        ByteBuffer predicateBufferHigh = bufferAllocator.createBuffView(predicateBuffer);

        SortedRangeSet sortedRangeSet = (SortedRangeSet) value.getValues();
        int numRanges = sortedRangeSet.getRangeCount();
        int highBufStartPos = numRanges * (Long.BYTES + 1); // +1 for the inclusive
        predicateBufferLow.position(0);
        predicateBufferHigh.position(highBufStartPos);
        rangesConverter.setStringRanges(sortedRangeSet, predicateBufferLow, predicateBufferHigh, typeLength, sliceConverter);
    }
}
