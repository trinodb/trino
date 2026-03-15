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
import io.airlift.slice.Slices;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.spi.type.VariantType;
import io.trino.spi.variant.Metadata;
import io.trino.spi.variant.Variant;
import io.trino.type.JsonType;

import java.util.Optional;
import java.util.function.IntUnaryOperator;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.util.variant.JsonVariantWriter.JSON_VARIANT_WRITER;
import static io.trino.util.variant.VariantVariantWriter.VARIANT_VARIANT_WRITER;

public interface VariantWriter
{
    static VariantWriter create(Type type)
    {
        return switch (type) {
            case ArrayType arrayType -> {
                Type elementType = arrayType.getElementType();
                Optional<PrimitiveVariantEncoder> primitiveElementEncoder = PrimitiveVariantEncoder.create(elementType);
                if (primitiveElementEncoder.isPresent()) {
                    yield new PrimitiveArrayVariantWriter(arrayType, primitiveElementEncoder.get());
                }
                yield new ArrayVariantWriter(arrayType, create(elementType));
            }
            case MapType mapType -> {
                checkArgument(mapType.getKeyType() instanceof VarcharType, "Map key type must be VARCHAR: %s", mapType.getKeyType());
                Type valueType = mapType.getValueType();
                Optional<PrimitiveVariantEncoder> primitiveValueEncoder = PrimitiveVariantEncoder.create(valueType);
                if (primitiveValueEncoder.isPresent()) {
                    yield new PrimitiveMapVariantWriter(mapType, primitiveValueEncoder.get());
                }
                yield new MapVariantWriter(mapType, create(valueType));
            }
            case RowType rowType -> new RowVariantWriter(rowType);
            case VariantType _ -> VARIANT_VARIANT_WRITER;
            case JsonType _ -> JSON_VARIANT_WRITER;
            // VariantWriter cannot be used for primitive types. Instead, use VariantEncoder directly, which is significantly more efficient.
            default -> throw new IllegalArgumentException("Unsupported type for VariantWriter: " + type);
        };
    }

    default Variant write(Object value)
    {
        Metadata.Builder metadataBuilder = Metadata.builder();
        PlannedValue plannedValue = plan(metadataBuilder, value);
        Metadata.Builder.SortedMetadata build = metadataBuilder.buildSorted();
        IntUnaryOperator sortedFieldIdMapping = build.sortedFieldIdMapping();
        plannedValue.finalize(sortedFieldIdMapping);
        Slice out = Slices.allocate(plannedValue.size());
        plannedValue.write(out, 0);
        return Variant.from(build.metadata(), out);
    }

    /// Plans the writing of the given value.
    /// Fields required for writing the value will be registered in the provided Builder.
    /// The returned PlannedValue can then be used to write the value after finalizing with remapped
    /// field IDs from the `Metadata.Builder`.
    /// @param metadataBuilder the metadata builder to register required fields
    /// @param value the stack value to plan writing for
    PlannedValue plan(Metadata.Builder metadataBuilder, Object value);

    /// A planned value that can be finalized and written.
    interface PlannedValue
    {
        /// Finalizes the planned value by remapping provisional field IDs to final sorted field IDs.
        /// The system creates a globally sorted metadata dictionary after all values have been planned,
        /// which may change the field IDs assigned during planning. This method can rely on the field
        /// IDs being assigned in ascending order for determining the write order of object fields.
        void finalize(IntUnaryOperator sortedFieldIdMapping);

        /// Returns the size in bytes required to write the value.
        /// This must be called after finalize().
        int size();

        /// Writes the value to the given output slice at the specified offset.
        /// This must be called after finalize().
        /// This method can be called multiple times to write the same value to different output slices.
        /// @return the number of bytes written
        int write(Slice out, int offset);
    }
}
