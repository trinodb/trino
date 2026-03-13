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
package io.trino.plugin.ducklake;

import com.google.common.collect.ImmutableList;
import io.trino.parquet.Field;
import io.trino.parquet.GroupField;
import io.trino.parquet.PrimitiveField;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.GroupColumnIO;
import org.apache.parquet.io.PrimitiveColumnIO;

import java.util.Optional;

import static io.trino.parquet.ParquetTypeUtils.getArrayElementColumn;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;

/**
 * Utility class for converting between Ducklake types and Parquet Field objects.
 * Supports primitives and arrays of primitives for MVP.
 *
 * NOT SUPPORTED (will be added later):
 * - Maps
 * - Structs/Rows
 * - Nested complex types (arrays of arrays, arrays of structs, etc.)
 */
public final class DucklakeParquetTypeUtils
{
    private DucklakeParquetTypeUtils() {}

    /**
     * Construct a Parquet Field from a ColumnIO.
     * Supports primitives and arrays of primitives.
     */
    public static Field constructField(Type trinoType, ColumnIO columnIO)
    {
        requireNonNull(trinoType, "trinoType is null");
        requireNonNull(columnIO, "columnIO is null");

        boolean required = columnIO.getType().getRepetition() != OPTIONAL;
        int repetitionLevel = columnIO.getRepetitionLevel();
        int definitionLevel = columnIO.getDefinitionLevel();

        // Handle arrays
        if (trinoType instanceof ArrayType arrayType) {
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            if (groupColumnIO.getChildrenCount() != 1) {
                throw new IllegalArgumentException("Invalid array structure in Parquet file");
            }

            Type elementType = arrayType.getElementType();
            ColumnIO elementColumnIO = getArrayElementColumn(groupColumnIO.getChild(0));

            // Only support arrays of primitives for now
            if (!(elementColumnIO instanceof PrimitiveColumnIO)) {
                throw new UnsupportedOperationException(
                        "Nested complex types not yet supported (arrays of arrays, arrays of structs). Column: " + columnIO.getType().getName());
            }

            Field elementField = constructPrimitiveField(elementType, elementColumnIO);
            return new GroupField(trinoType, repetitionLevel, definitionLevel, required, ImmutableList.of(Optional.of(elementField)));
        }

        // Handle primitives
        if (columnIO instanceof PrimitiveColumnIO) {
            return constructPrimitiveField(trinoType, columnIO);
        }

        // Maps and structs not supported yet
        throw new UnsupportedOperationException(
                "Maps and structs not yet supported. Column: " + columnIO.getType().getName());
    }

    private static PrimitiveField constructPrimitiveField(Type trinoType, ColumnIO columnIO)
    {
        PrimitiveColumnIO primitiveColumnIO = (PrimitiveColumnIO) columnIO;
        boolean required = columnIO.getType().getRepetition() != OPTIONAL;

        return new PrimitiveField(
                trinoType,
                required,
                primitiveColumnIO.getColumnDescriptor(),
                primitiveColumnIO.getId());
    }
}
