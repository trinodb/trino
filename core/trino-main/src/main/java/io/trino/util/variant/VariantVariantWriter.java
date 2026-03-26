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
import io.trino.spi.variant.Metadata;
import io.trino.spi.variant.Variant;
import io.trino.spi.variant.VariantFieldRemapper;

import java.util.function.IntUnaryOperator;

import static java.util.Objects.requireNonNull;

final class VariantVariantWriter
        implements VariantWriter
{
    public static final VariantVariantWriter VARIANT_VARIANT_WRITER = new VariantVariantWriter();

    private VariantVariantWriter() {}

    @Override
    public PlannedValue plan(Metadata.Builder metadataBuilder, Object value)
    {
        if (value == null) {
            return NullPlannedValue.NULL_PLANNED_VALUE;
        }
        return new PlannedVariantValue(VariantFieldRemapper.create((Variant) value, metadataBuilder));
    }

    private record PlannedVariantValue(VariantFieldRemapper variantFieldRemapper)
            implements PlannedValue
    {
        private PlannedVariantValue(VariantFieldRemapper variantFieldRemapper)
        {
            this.variantFieldRemapper = requireNonNull(variantFieldRemapper, "variantFieldRemapper is null");
        }

        @Override
        public void finalize(IntUnaryOperator sortedFieldIdMapping)
        {
            variantFieldRemapper.finalize(sortedFieldIdMapping);
        }

        @Override
        public int size()
        {
            return variantFieldRemapper.size();
        }

        @Override
        public int write(Slice out, int offset)
        {
            variantFieldRemapper.write(out, offset);
            return size();
        }
    }
}
