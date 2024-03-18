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
import io.trino.plugin.varada.dispatcher.query.PredicateData;
import io.trino.plugin.varada.juffer.BufferAllocator;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.type.TypeUtils;
import io.trino.plugin.varada.util.SliceUtils;
import io.trino.plugin.varada.util.StringPredicateData;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.spi.block.Block;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.type.Type;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class StringValuesPredicateFiller
        extends PredicateFiller<Domain>
{
    private static final Logger logger = Logger.get(StringValuesPredicateFiller.class);
    private final StorageEngineConstants storageEngineConstants;

    public StringValuesPredicateFiller(StorageEngineConstants storageEngineConstants, BufferAllocator bufferAllocator)
    {
        super(bufferAllocator);
        this.storageEngineConstants = storageEngineConstants;
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
        ByteBuffer predicateBufferLow = bufferAllocator.createBuffView(predicateBuffer);
        ByteBuffer predicateBufferHigh = bufferAllocator.createBuffView(predicateBuffer);
        Type type = value.getType();
        int typeLength = TypeUtils.getTypeLength(type, storageEngineConstants.getVarcharMaxLen());
        Function<Slice, Slice> sliceConverter = SliceUtils.getSliceConverter(type,
                typeLength,
                typeLength <= storageEngineConstants.getFixedLengthStringLimit(),
                true);
        // this predicate order MUST be kept !!!
        convertString(value, predicateBufferLow, predicateBufferHigh, typeLength, sliceConverter);
    }

    @Override
    public PredicateType getPredicateType()
    {
        return PredicateType.PREDICATE_TYPE_STRING_VALUES;
    }

    void convertString(Domain domain, ByteBuffer lowBuf, ByteBuffer highBuf, int recLength, Function<Slice, Slice> sliceConverter)
    {
        SortedRangeSet sortedRangeSet = (SortedRangeSet) domain.getValues();
        try {
            int rangesCount = sortedRangeSet.getRangeCount();
            // per range and its markers, extract the low and high values and put them in the buffer
            int highBufStartPos = rangesCount * Long.BYTES; // low buf is CRCs
            long minStr = Long.MAX_VALUE;
            long maxStr = Long.MIN_VALUE;
            lowBuf.position(0);
            highBuf.position(highBufStartPos);
            List<StringPredicateData> strList = orderRanges(domain, recLength, sliceConverter);
            for (StringPredicateData str : strList) {
                lowBuf.putLong(str.comperationValue());
                long val = SliceUtils.str2int(str.value(), str.length(), true);
                if (val > maxStr) {
                    maxStr = val;
                }
                if (val < minStr) {
                    minStr = val;
                }
            }
            highBuf.putLong(minStr);
            highBuf.putLong(maxStr);
        }
        catch (Exception e) {
            logger.error(e, "convertString failed");
            throw e;
        }
    }

    private List<StringPredicateData> orderRanges(Domain domain, int recLength, Function<Slice, Slice> sliceConverter)
    {
        SortedRangeSet sortedRangeSet = (SortedRangeSet) domain.getValues();
        List<StringPredicateData> strList = new ArrayList<>(sortedRangeSet.getRangeCount());

        SliceUtils.StringPredicateDataFactory stringPredicateDataFactory = new SliceUtils.StringPredicateDataFactory();
        Block sortedRangesBlock = sortedRangeSet.getSortedRanges();
        Type type = domain.getType();
        for (int i = 0; i < sortedRangesBlock.getPositionCount(); i += 2) {
            Slice slice = type.getSlice(sortedRangesBlock, i);
            Slice value = sliceConverter.apply(slice);
            // crc is calculated on the string without the length byte in native as well
            strList.add(stringPredicateDataFactory.create(value, recLength, true));
        }

        // sort the crc array and put it in the low buf
        Collections.sort(strList);

        return strList;
    }
}
