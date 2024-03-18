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
import io.trino.plugin.varada.type.TypeUtils;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;

import java.nio.ByteBuffer;

import static io.trino.plugin.varada.VaradaErrorCode.VARADA_CONTROL;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;

public class ValuesPredicateFiller
        extends PredicateFiller<Domain>
{
    private static final Logger logger = Logger.get(ValuesPredicateFiller.class);

    public ValuesPredicateFiller(BufferAllocator bufferAllocator)
    {
        super(bufferAllocator);
    }

    @Override
    public void fillPredicate(Domain value, ByteBuffer predicateBuffer, PredicateData predicateData)
    {
        predicateBuffer = writePredicateInfoToBuffer(predicateBuffer, predicateData);
        convertValues(predicateBuffer, value);
    }

    @Override
    public PredicateType getPredicateType()
    {
        return PredicateType.PREDICATE_TYPE_VALUES;
    }

    @Override
    public void convertValues(Domain value, ByteBuffer predicateBuffer)
    {
        ByteBuffer predicateBufferVals = bufferAllocator.createBuffView(predicateBuffer);
        convertValues(predicateBufferVals, value);
    }

    //lower value and high value are equal in valuesPredicate (singleValue), so it enough to take only one value
    protected void convertValues(ByteBuffer buf, Domain domain)
    {
        Type type = domain.getType();
        try {
            Block sortedRangesBlock = ((SortedRangeSet) domain.getValues()).getSortedRanges();
            if (TypeUtils.isIntType(type) || TypeUtils.isRealType(type)) {
                for (int i = 0; i < sortedRangesBlock.getPositionCount(); i += 2) {
                    buf.putInt(INTEGER.getInt(sortedRangesBlock, i));
                }
            }
            else if (TypeUtils.isLongType(type) || TypeUtils.isShortDecimalType(type)) {
                for (int i = 0; i < sortedRangesBlock.getPositionCount(); i += 2) {
                    buf.putLong(type.getLong(sortedRangesBlock, i));
                }
            }
            else if (TypeUtils.isDoubleType(type)) {
                for (int i = 0; i < sortedRangesBlock.getPositionCount(); i += 2) {
                    buf.putDouble(type.getDouble(sortedRangesBlock, i));
                }
            }
            else if (TypeUtils.isSmallIntType(type)) {
                for (int i = 0; i < sortedRangesBlock.getPositionCount(); i += 2) {
                    buf.putShort(SMALLINT.getShort(sortedRangesBlock, i));
                }
            }
            else if (TypeUtils.isTinyIntType(type)) {
                for (int i = 0; i < sortedRangesBlock.getPositionCount(); i += 2) {
                    buf.put(TINYINT.getByte(sortedRangesBlock, i));
                }
            }
            else if (TypeUtils.isBooleanType(type)) {
                for (int i = 0; i < sortedRangesBlock.getPositionCount(); i += 2) {
                    buf.put(type.getBoolean(sortedRangesBlock, i) ? BOOLEAN_TRUE_VALUE : BOOLEAN_FALSE_VALUE);
                }
            }
            else if (TypeUtils.isLongDecimalType(type)) {
                for (int i = 0; i < sortedRangesBlock.getPositionCount(); i += 2) {
                    Int128 value = (Int128) type.getObject(sortedRangesBlock, i);
                    buf.putLong(value.getHigh());
                    buf.putLong(value.getLow());
                }
            }
            else {
                throw new TrinoException(VARADA_CONTROL, "unexpected ValType " + type);
            }
        }
        catch (Exception e) {
            logger.error(e, "convertValues failed type=%s", type);
            throw e;
        }
    }
}
