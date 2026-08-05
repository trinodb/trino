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
import io.trino.spi.type.Type;
import io.trino.spi.variant.Metadata;

import java.util.function.IntUnaryOperator;

import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static java.util.Objects.requireNonNull;

record PrimitiveVariantWriter(Type type, PrimitiveVariantEncoder encoder)
        implements VariantWriter
{
    PrimitiveVariantWriter
    {
        requireNonNull(type, "type is null");
        requireNonNull(encoder, "encoder is null");
    }

    @Override
    public PlannedValue plan(Metadata.Builder metadataBuilder, Object value)
    {
        Block block = writeNativeValue(type, value);
        return new PrimitivePlannedValue(block, encoder.size(block, 0), encoder);
    }

    private record PrimitivePlannedValue(Block block, int size, PrimitiveVariantEncoder encoder)
            implements PlannedValue
    {
        @Override
        public void finalize(IntUnaryOperator sortedFieldIdMapping) {}

        @Override
        public int write(Slice out, int offset)
        {
            return encoder.write(block, 0, out, offset);
        }
    }
}
