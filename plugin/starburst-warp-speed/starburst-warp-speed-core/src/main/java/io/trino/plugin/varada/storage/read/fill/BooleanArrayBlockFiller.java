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
package io.trino.plugin.varada.storage.read.fill;

import io.airlift.slice.Slice;
import io.trino.plugin.varada.juffer.ByteBufferInputStream;
import io.trino.plugin.varada.storage.common.StorageConstants;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.spi.block.Block;
import io.trino.spi.block.ByteArrayBlock;

import java.util.Optional;

import static io.trino.plugin.varada.storage.common.StorageConstants.MAY_HAVE_NULL;
import static io.trino.plugin.varada.storage.common.StorageConstants.NULL_EXISTS_MARKER_SIZE;
import static io.trino.plugin.varada.storage.common.StorageConstants.NULL_VALUE_MARKER_SIZE;
import static io.trino.spi.type.BooleanType.BOOLEAN;

public class BooleanArrayBlockFiller
        extends ArraySliceFiller
{
    public BooleanArrayBlockFiller(StorageEngineConstants storageEngineConstants)
    {
        super(storageEngineConstants, BlockFillerType.ARRAY_BOOLEAN, BOOLEAN);
    }

    @Override
    protected Block getBlock(Slice slice, int offset, int length)
    {
        byte mayHaveNull = slice.getByte(offset);
        offset += NULL_EXISTS_MARKER_SIZE; // skip mayHaveNull marker
        length = length - 1; // length of the actual array is slice length - the

        if (mayHaveNull == MAY_HAVE_NULL) {
            int numElements = length / (Byte.BYTES + NULL_VALUE_MARKER_SIZE);
            byte[] values = new byte[numElements];
            boolean[] valuesIsNull = new boolean[numElements];

            for (int currInt = 0; currInt < numElements; currInt++) {
                byte isNull = slice.getByte(offset + (currInt * (Byte.BYTES + NULL_VALUE_MARKER_SIZE)));
                if (isNull == StorageConstants.NULL_MARKER_VALUE) {
                    valuesIsNull[currInt] = true;
                }
                else {
                    values[currInt] = slice.getByte(offset + (currInt * (Byte.BYTES + NULL_VALUE_MARKER_SIZE)) + NULL_VALUE_MARKER_SIZE);
                }
            }
            return new ByteArrayBlock(numElements, Optional.of(valuesIsNull), values);
        }
        else {
            byte[] values = new byte[length];

            for (int currInt = 0; currInt < length; currInt++) {
                values[currInt] = slice.getByte(offset + currInt);
            }

            return new ByteArrayBlock(length, Optional.empty(), values);
        }
    }

    @Override
    protected Block getBlock(ByteBufferInputStream byteBufferInputStream, int offset, int length)
    {
        byte mayHaveNull = byteBufferInputStream.readByte();
        offset += NULL_EXISTS_MARKER_SIZE; // skip mayHaveNull marker
        length = length - 1; // length of the actual array is slice length - the
        if (mayHaveNull == StorageConstants.MAY_HAVE_NULL) {
            int numElements = length / (Byte.BYTES + NULL_VALUE_MARKER_SIZE);
            byte[] values = new byte[numElements];
            boolean[] valuesIsNull = new boolean[numElements];
            for (int currInt = 0; currInt < numElements; currInt++) {
                byte isNull = byteBufferInputStream.readByte();
                if (isNull == StorageConstants.NULL_MARKER_VALUE) {
                    valuesIsNull[currInt] = true;
                }

                values[currInt] = byteBufferInputStream.readByte();
            }
            return new ByteArrayBlock(numElements, Optional.of(valuesIsNull), values);
        }
        else {
            byte[] values = byteBufferInputStream.readBytes(offset, length);
            return new ByteArrayBlock(length, Optional.empty(), values);
        }
    }
}
