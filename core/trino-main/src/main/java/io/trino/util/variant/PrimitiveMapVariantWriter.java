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

import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.type.MapType;
import io.trino.spi.variant.Metadata;
import it.unimi.dsi.fastutil.ints.IntArrays;

import java.util.List;
import java.util.function.IntUnaryOperator;

import static io.trino.spi.variant.VariantEncoder.encodeObjectHeading;
import static io.trino.spi.variant.VariantEncoder.encodedObjectSize;
import static java.util.Objects.requireNonNull;

public record PrimitiveMapVariantWriter(MapType type, PrimitiveVariantEncoder valueEncoder)
        implements VariantWriter
{
    @Override
    public PlannedValue plan(Metadata.Builder metadataBuilder, Object value)
    {
        if (value == null) {
            return NullPlannedValue.NULL_PLANNED_VALUE;
        }

        SqlMap sqlMap = (SqlMap) value;
        int[] fieldIds = metadataBuilder.addFieldNames(getMapKeys(sqlMap));

        return new PlannedPrimitiveMapValue(fieldIds, sqlMap.getRawValueBlock(), sqlMap.getRawOffset(), valueEncoder);
    }

    private static final class PlannedPrimitiveMapValue
            implements PlannedValue
    {
        // Initially, field IDs are provisional and must be remapped in finalize()
        // This array is mutated in finalize()
        private final int[] fieldIds;
        private final Block valueBlock;
        private final int valueBlockOffset;
        private final PrimitiveVariantEncoder valueEncoder;

        private int size = -1;

        private PlannedPrimitiveMapValue(int[] fieldIds, Block valueBlock, int valueBlockOffset, PrimitiveVariantEncoder valueEncoder)
        {
            this.fieldIds = requireNonNull(fieldIds, "fieldIds is null");
            this.valueBlock = requireNonNull(valueBlock, "valueBlock is null");
            this.valueBlockOffset = valueBlockOffset;
            this.valueEncoder = requireNonNull(valueEncoder, "valueEncoder is null");
        }

        @Override
        public void finalize(IntUnaryOperator sortedFieldIdMapping)
        {
            if (size >= 0) {
                throw new IllegalStateException("finalize() already called");
            }

            int maxFieldId = -1;
            for (int i = 0; i < fieldIds.length; i++) {
                int finalFieldId = sortedFieldIdMapping.applyAsInt(fieldIds[i]);
                fieldIds[i] = finalFieldId;
                maxFieldId = Math.max(maxFieldId, finalFieldId);
            }

            int totalElementLength = 0;
            for (int i = 0; i < fieldIds.length; i++) {
                totalElementLength += valueEncoder.size(valueBlock, valueBlockOffset + i);
            }
            size = encodedObjectSize(maxFieldId, fieldIds.length, totalElementLength);
        }

        @Override
        public int size()
        {
            if (size < 0) {
                throw new IllegalStateException("finalize() must be called before size()");
            }
            return size;
        }

        @Override
        public int write(Slice out, int offset)
        {
            if (size < 0) {
                throw new IllegalStateException("finalize() must be called before write()");
            }

            int[] writeOrder = determineWriteOrder(fieldIds);

            int written = encodeObjectHeading(
                    fieldIds.length,
                    i -> fieldIds[writeOrder[i]],
                    i -> {
                        int entry = writeOrder[i];
                        return valueEncoder.size(valueBlock, valueBlockOffset + entry);
                    },
                    out,
                    offset);

            for (int entry : writeOrder) {
                written += valueEncoder.write(valueBlock, valueBlockOffset + entry, out, offset + written);
            }
            return written;
        }
    }

    /// Extract map keys from the SqlMap, ensuring no null or duplicate keys
    /// @throws IllegalArgumentException if a map key is null or if there are duplicate keys
    static List<Slice> getMapKeys(SqlMap sqlMap)
    {
        Block keyBlock = sqlMap.getRawKeyBlock();
        VariableWidthBlock underlyingBlock = (VariableWidthBlock) keyBlock.getUnderlyingValueBlock();
        int offset = sqlMap.getRawOffset();
        int size = sqlMap.getSize();
        ImmutableSet.Builder<Slice> keySet = ImmutableSet.builderWithExpectedSize(size);
        for (int i = 0; i < size; i++) {
            int underlyingPosition = keyBlock.getUnderlyingValuePosition(offset + i);
            if (underlyingBlock.isNull(underlyingPosition)) {
                throw new IllegalArgumentException("Map key is null");
            }
            keySet.add(underlyingBlock.getSlice(underlyingPosition));
        }
        // ImmutableSet as list preserves insertion order
        List<Slice> keys = keySet.build().asList();
        if (keys.size() != size) {
            throw new IllegalArgumentException("Map contains duplicate keys");
        }
        return keys;
    }

    /// Determine the order to write entries. Object fields must be written in lexicographical
    /// order of the field names. Since the metadata dictionary is sorted, the fieldIds are also
    /// in lexicographical order of the field names.
    /// @param fieldIds from a globally sorted metadata dictionary
    /// @return the order to write entries so that field names are in lexicographical order
    static int[] determineWriteOrder(int[] fieldIds)
    {
        int[] writeOrder = new int[fieldIds.length];
        for (int i = 0; i < writeOrder.length; i++) {
            writeOrder[i] = i;
        }
        IntArrays.unstableSort(writeOrder, 0, writeOrder.length, (left, right) -> Integer.compare(fieldIds[left], fieldIds[right]));
        return writeOrder;
    }
}
