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
package io.trino.plugin.pinot.encoders;

import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import org.apache.pinot.spi.data.FieldSpec;

import static java.util.Objects.requireNonNull;

public class ArrayEncoder
        extends AbstractEncoder
{
    private final Encoder elementEncoder;
    private final Object defaultNullValue;

    public ArrayEncoder(FieldSpec fieldSpec, Type prestoElementType)
    {
        requireNonNull(prestoElementType, "elementType is null");
        this.elementEncoder = EncoderFactory.createEncoder(fieldSpec, prestoElementType);
        this.defaultNullValue = fieldSpec.getDefaultNullValue();
    }

    @Override
    protected Object encodeNonNull(Block block, int position)
    {
        Block arrayBlock = block.getObject(position, Block.class);
        Object[] elements = new Object[arrayBlock.getPositionCount()];
        for (int index = 0; index < arrayBlock.getPositionCount(); index++) {
            Object value = elementEncoder.encode(arrayBlock, index);
            // Arrays cannot contain null values: see org.apache.pinot.core.segment.processing.genericrow.GenericRowSerializer.serialize method.
            // Single values may contain nulls.
            if (value != null) {
                elements[index] = value;
            }
            else {
                elements[index] = defaultNullValue;
            }
        }
        return elements;
    }
}
