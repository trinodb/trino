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
package io.trino.util.variant;

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.type.ArrayType;
import io.trino.spi.variant.Metadata;

import java.util.function.IntUnaryOperator;

import static io.trino.spi.variant.VariantEncoder.encodeArrayHeading;
import static io.trino.spi.variant.VariantEncoder.encodeNull;
import static io.trino.spi.variant.VariantEncoder.encodedArraySize;

record PrimitiveArrayVariantWriter(ArrayType type, PrimitiveVariantEncoder elementEncoder)
        implements VariantWriter
{
    @Override
    public PlannedValue plan(Metadata.Builder metadataBuilder, Object value)
    {
        if (value == null) {
            return NullPlannedValue.NULL_PLANNED_VALUE;
        }

        Block array = (Block) value;
        int totalElementsLength = 0;
        for (int i = 0; i < array.getPositionCount(); i++) {
            totalElementsLength += elementEncoder.size(array, i);
        }
        int totalSize = encodedArraySize(array.getPositionCount(), totalElementsLength);
        return new PrimitiveArrayPlannedValue(array, totalSize, elementEncoder);
    }

    private record PrimitiveArrayPlannedValue(Block elements, int size, PrimitiveVariantEncoder primitiveEncoder)
            implements PlannedValue
    {
        @Override
        public void finalize(IntUnaryOperator sortedFieldIdMapping) {}

        @Override
        public int write(Slice out, int offset)
        {
            int written = encodeArrayHeading(elements.getPositionCount(), position -> primitiveEncoder.size(elements, position), out, offset);
            for (int i = 0; i < elements.getPositionCount(); i++) {
                if (elements.isNull(i)) {
                    written += encodeNull(out, offset + written);
                }
                else {
                    written += primitiveEncoder.write(elements, i, out, offset + written);
                }
            }
            return written;
        }
    }
}
