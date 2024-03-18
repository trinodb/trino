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
import io.trino.plugin.varada.dispatcher.query.PredicateData;
import io.trino.plugin.varada.juffer.BufferAllocator;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.type.TypeUtils;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.spi.TrinoException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.type.Type;

import java.nio.ByteBuffer;

import static io.trino.plugin.varada.VaradaErrorCode.VARADA_CONTROL;

public class RangesPredicateFiller
        extends PredicateFiller<Domain>
{
    private static final Logger logger = Logger.get(RangesPredicateFiller.class);
    private final RangesConverter rangesConverter;
    private final StorageEngineConstants storageEngineConstants;

    public RangesPredicateFiller(BufferAllocator bufferAllocator,
            StorageEngineConstants storageEngineConstants)
    {
        super(bufferAllocator);
        this.rangesConverter = new RangesConverter();
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
        convertRanges(value, predicateBufferLow, predicateBufferHigh);
    }

    @Override
    public PredicateType getPredicateType()
    {
        return PredicateType.PREDICATE_TYPE_RANGES;
    }

    private void convertRanges(Domain domain, ByteBuffer lowBuf, ByteBuffer highBuf)
    {
        try {
            Type type = domain.getType();
            int recLength = TypeUtils.getTypeLength(type, storageEngineConstants.getVarcharMaxLen());
            SortedRangeSet sortedRangeSet = (SortedRangeSet) domain.getValues();
            int numRanges = sortedRangeSet.getRangeCount();
            int highBufStartPos = numRanges * (recLength + 1); // +1 for the inclusive
            lowBuf.position(0);
            highBuf.position(highBufStartPos);

            // set value in buffer according to types
            if (TypeUtils.isRealType(type)) { // must be before intType
                rangesConverter.setRealRanges(sortedRangeSet.getRanges(), lowBuf, highBuf);
            }
            else if (TypeUtils.isIntType(type) || TypeUtils.isRealType(type)) {
                rangesConverter.setIntRanges(lowBuf, highBuf, sortedRangeSet);
            }
            else if (TypeUtils.isLongType(type) || TypeUtils.isShortDecimalType(type)) {
                rangesConverter.setLongRanges(lowBuf, highBuf, sortedRangeSet);
            }
            else if (TypeUtils.isDoubleType(type)) {
                rangesConverter.setDoubleRanges(lowBuf, highBuf, sortedRangeSet);
            }
            else if (TypeUtils.isBooleanType(type)) {
                rangesConverter.setBooleanRanges(sortedRangeSet.getRanges(), lowBuf, highBuf);
            }
            else if (TypeUtils.isSmallIntType(type)) {
                rangesConverter.setSmallIntRanges(lowBuf, highBuf, sortedRangeSet);
            }
            else if (TypeUtils.isTinyIntType(type)) {
                rangesConverter.setTinyintRanges(lowBuf, highBuf, sortedRangeSet);
            }
            else if (TypeUtils.isLongDecimalType(type)) {
                rangesConverter.setLongDecimalRanges(lowBuf, highBuf, sortedRangeSet);
            }
            else {
                throw new TrinoException(VARADA_CONTROL, "unexpected ValType " + type);
            }
        }
        catch (Exception e) {
            logger.error(e, "convertRanges failed");
            throw e;
        }
    }
}
