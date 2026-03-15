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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.RowType;
import io.trino.spi.type.RowType.Field;
import io.trino.spi.type.Type;
import io.trino.spi.variant.Metadata;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntComparator;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.IntUnaryOperator;

import static io.trino.spi.variant.VariantEncoder.encodeObjectHeading;
import static io.trino.spi.variant.VariantEncoder.encodedObjectSize;
import static java.util.Objects.requireNonNull;

final class RowVariantWriter
        implements VariantWriter
{
    // all fields are in the order they will be written
    private final List<Slice> fieldNames;
    private final List<Type> fieldTypes;
    private final List<Optional<PrimitiveVariantEncoder>> fieldPrimitiveEncoders;
    private final List<Optional<VariantWriter>> fieldWriters;
    private final IntUnaryOperator outputIndexToRowIndex;

    RowVariantWriter(RowType type)
    {
        List<Slice> fieldNames = type.getFields().stream()
                .map(Field::getName)
                .map(name -> name.orElseThrow(() -> new IllegalArgumentException("Anonymous fields are not supported in RowType for Variant encoding")))
                .map(Slices::utf8Slice)
                .toList();
        int[] writeOrder = determineWriteOrder(fieldNames);

        // Build "in write order" views
        int fieldCount = type.getFields().size();
        ImmutableList.Builder<Slice> fieldNamesInWriteOrder = ImmutableList.builderWithExpectedSize(fieldCount);
        ImmutableList.Builder<Type> fieldTypesInWriteOrder = ImmutableList.builderWithExpectedSize(fieldCount);
        ImmutableList.Builder<Optional<PrimitiveVariantEncoder>> primitiveEncodersInWriteOrder = ImmutableList.builderWithExpectedSize(fieldCount);
        ImmutableList.Builder<Optional<VariantWriter>> writersInWriteOrder = ImmutableList.builderWithExpectedSize(fieldCount);

        for (int fieldIndex : writeOrder) {
            Type fieldType = type.getTypeParameters().get(fieldIndex);
            fieldNamesInWriteOrder.add(fieldNames.get(fieldIndex));
            fieldTypesInWriteOrder.add(fieldType);

            Optional<PrimitiveVariantEncoder> primitiveEncoder = PrimitiveVariantEncoder.create(fieldType);
            primitiveEncodersInWriteOrder.add(primitiveEncoder);

            if (primitiveEncoder.isPresent()) {
                writersInWriteOrder.add(Optional.empty());
            }
            else {
                writersInWriteOrder.add(Optional.of(VariantWriter.create(fieldType)));
            }
        }

        this.fieldNames = fieldNamesInWriteOrder.build();
        this.fieldTypes = fieldTypesInWriteOrder.build();
        this.fieldPrimitiveEncoders = primitiveEncodersInWriteOrder.build();
        this.fieldWriters = writersInWriteOrder.build();
        this.outputIndexToRowIndex = outputIndex -> writeOrder[outputIndex];
    }

    private static int[] determineWriteOrder(List<Slice> fieldNames)
    {
        int[] writeOrder = new int[fieldNames.size()];
        for (int i = 0; i < fieldNames.size(); i++) {
            writeOrder[i] = i;
        }
        IntArrays.unstableSort(writeOrder, 0, writeOrder.length, IntComparator.comparing(fieldNames::get));
        return writeOrder;
    }

    @Override
    public PlannedValue plan(Metadata.Builder metadataBuilder, Object value)
    {
        if (value == null) {
            return NullPlannedValue.NULL_PLANNED_VALUE;
        }

        int[] provisionalFieldIds = metadataBuilder.addFieldNames(fieldNames);

        FieldBlocks blocks = new FieldBlocks((SqlRow) value, outputIndexToRowIndex);
        List<Optional<PlannedValue>> plannedValues = new ArrayList<>(fieldWriters.size());
        for (int i = 0; i < fieldWriters.size(); i++) {
            Optional<VariantWriter> fieldWriter = fieldWriters.get(i);
            if (fieldWriter.isPresent()) {
                Object fieldValue = fieldTypes.get(i).getObject(blocks.getField(i), blocks.getPosition());
                plannedValues.add(Optional.of(fieldWriter.get().plan(metadataBuilder, fieldValue)));
            }
            else {
                plannedValues.add(Optional.empty());
            }
        }

        return new PlannedRowValue(fieldPrimitiveEncoders, plannedValues, blocks, provisionalFieldIds);
    }

    private static final class PlannedRowValue
            implements PlannedValue
    {
        private final List<Optional<PrimitiveVariantEncoder>> fieldPrimitiveEncoders;
        private final List<Optional<PlannedValue>> fieldPlannedValue;
        private final FieldBlocks fieldBlocks;

        // Initially, field IDs are provisional and must be remapped in finalize()
        // This array is mutated in finalize()
        private final int[] fieldIds;

        private int size = -1;

        public PlannedRowValue(List<Optional<PrimitiveVariantEncoder>> fieldPrimitiveEncoders, List<Optional<PlannedValue>> fieldPlannedValue, FieldBlocks fieldBlocks, int[] fieldIds)
        {
            this.fieldPrimitiveEncoders = requireNonNull(fieldPrimitiveEncoders, "fieldPrimitiveEncoders is null");
            this.fieldPlannedValue = requireNonNull(fieldPlannedValue, "fieldPlannedValue is null");
            this.fieldBlocks = requireNonNull(fieldBlocks, "fieldBlocks is null");
            this.fieldIds = requireNonNull(fieldIds, "fieldIds is null");
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
            for (int i = 0; i < fieldPrimitiveEncoders.size(); i++) {
                Optional<PrimitiveVariantEncoder> primitiveElementEncoder = fieldPrimitiveEncoders.get(i);
                if (primitiveElementEncoder.isPresent()) {
                    totalElementLength += primitiveElementEncoder.get().size(fieldBlocks.getField(i), fieldBlocks.getPosition());
                }
                else {
                    PlannedValue plannedValue = fieldPlannedValue.get(i).orElseThrow();
                    plannedValue.finalize(sortedFieldIdMapping);
                    totalElementLength += plannedValue.size();
                }
            }

            size = encodedObjectSize(maxFieldId, fieldPrimitiveEncoders.size(), totalElementLength);
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

            int written = encodeObjectHeading(
                    fieldPrimitiveEncoders.size(),
                    i -> fieldIds[i],
                    i -> fieldPrimitiveEncoders.get(i)
                            .map(elementEncoder -> elementEncoder.size(fieldBlocks.getField(i), fieldBlocks.getPosition()))
                            .orElseGet(() -> fieldPlannedValue.get(i).orElseThrow().size()),
                    out,
                    offset);

            for (int i = 0; i < fieldPrimitiveEncoders.size(); i++) {
                Optional<PrimitiveVariantEncoder> primitiveEncoder = fieldPrimitiveEncoders.get(i);
                if (primitiveEncoder.isPresent()) {
                    written += primitiveEncoder.get().write(fieldBlocks.getField(i), fieldBlocks.getPosition(), out, offset + written);
                }
                else {
                    written += fieldPlannedValue.get(i).orElseThrow().write(out, offset + written);
                }
            }
            return written;
        }
    }

    private record FieldBlocks(SqlRow row, IntUnaryOperator blockIndexMapper)
    {
        public int getPosition()
        {
            return row.getRawIndex();
        }

        public Block getField(int outputIndex)
        {
            return row.getRawFieldBlock(blockIndexMapper.applyAsInt(outputIndex));
        }
    }
}
