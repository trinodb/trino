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

import io.trino.plugin.varada.dispatcher.query.PredicateData;
import io.trino.plugin.varada.dispatcher.query.PredicateInfo;
import io.trino.plugin.varada.juffer.BufferAllocator;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.plugin.warp.gen.constants.PredicateHeaderFlags;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.spi.type.TimestampType;

import java.nio.ByteBuffer;

import static java.lang.String.format;

public abstract class PredicateFiller<T>
{
    protected static final byte BOOLEAN_TRUE_VALUE = 1;
    protected static final byte BOOLEAN_FALSE_VALUE = 0;

    protected final BufferAllocator bufferAllocator;

    public PredicateFiller(BufferAllocator bufferAllocator)
    {
        this.bufferAllocator = bufferAllocator;
    }

    /**
     * fill column predicate buffers with values/ranges/crcs according to predicate type:
     * ALL - constant buffer is allcoated
     * NONE - constant buffer is allocated
     * RANGES - each range is set as two fields: low and high
     * VALUES - each value is set as one field
     * STRING - crcs and then lexicographic (str2int) min max for all values
     * LUCENE - constant buffer is allocated
     */
    public abstract void fillPredicate(T value, ByteBuffer predicateBuffer, PredicateData predicateData);

    public abstract PredicateType getPredicateType();

    protected ByteBuffer writePredicateInfoToBuffer(ByteBuffer predicateBuffer, PredicateData predicateData)
    {
        PredicateInfo predicateInfo = predicateData.getPredicateInfo();
        byte typeAndFlags = (byte) predicateInfo.predicateType().ordinal();
        boolean hasFunction = predicateInfo.functionType() != FunctionType.FUNCTION_TYPE_NONE;
        if (hasFunction) {
            typeAndFlags = (byte) (typeAndFlags | (1 << PredicateHeaderFlags.PREDICATE_FLAG_HAS_FUNCTION.offset()));
        }
        if (predicateData.isCollectNulls()) {
            typeAndFlags = (byte) (typeAndFlags | (1 << PredicateHeaderFlags.PREDICATE_FLAG_COLLECT_NULL.offset()));
        }
        predicateBuffer.put(typeAndFlags);
        predicateBuffer.putInt(predicateInfo.numValues());
        if (hasFunction) {
            predicateBuffer.put((byte) predicateInfo.functionType().ordinal());
            if (predicateData.getColumnType() instanceof TimestampType) {
                predicateBuffer.put((byte) ((TimestampType) predicateData.getColumnType()).getPrecision());
            }
        }

        for (Object functionParam : predicateInfo.functionParams()) {
            if (functionParam instanceof Integer) {
                predicateBuffer.putInt((int) functionParam);
            }
            else {
                throw new UnsupportedOperationException(format("invalid functionParam=%s, predicateInfo=%s", functionParam, predicateInfo));
            }
        }
        return bufferAllocator.createBuffView(predicateBuffer.slice());
    }

    public abstract void convertValues(T value, ByteBuffer predicateBuffer);
}
