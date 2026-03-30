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
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.GroupColumnIO;
import org.apache.parquet.io.PrimitiveColumnIO;

import java.util.List;
import java.util.Optional;

import static io.trino.parquet.ParquetTypeUtils.getArrayElementColumn;
import static io.trino.parquet.ParquetTypeUtils.getMapKeyValueColumn;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;

/**
 * Utility class for converting between Ducklake/Trino types and Parquet Field objects.
 * Supports all types: primitives, arrays, maps, structs/rows, and arbitrary nesting.
 */
public final class DucklakeParquetTypeUtils
{
    private DucklakeParquetTypeUtils() {}

    /**
     * Construct a Parquet Field from a Trino type and Parquet ColumnIO.
     * Recursively handles all nested types (ROW, MAP, ARRAY).
     * Returns Optional.empty() if the columnIO is null (e.g., a struct field missing from an older Parquet file).
     */
    public static Optional<Field> constructField(Type trinoType, ColumnIO columnIO)
    {
        requireNonNull(trinoType, "trinoType is null");
        if (columnIO == null) {
            return Optional.empty();
        }

        boolean required = columnIO.getType().getRepetition() != OPTIONAL;
        int repetitionLevel = columnIO.getRepetitionLevel();
        int definitionLevel = columnIO.getDefinitionLevel();

        if (trinoType instanceof RowType rowType) {
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            List<RowType.Field> rowFields = rowType.getFields();
            ImmutableList.Builder<Optional<Field>> children = ImmutableList.builder();
            boolean hasAnyField = false;
            for (RowType.Field rowField : rowFields) {
                String fieldName = rowField.getName()
                        .orElseThrow(() -> new IllegalArgumentException("ROW type field must have a name"));
                ColumnIO childColumnIO = groupColumnIO.getChild(fieldName);
                Optional<Field> childField = constructField(rowField.getType(), childColumnIO);
                hasAnyField |= childField.isPresent();
                children.add(childField);
            }
            if (hasAnyField) {
                return Optional.of(new GroupField(trinoType, repetitionLevel, definitionLevel, required, children.build()));
            }
            return Optional.empty();
        }

        if (trinoType instanceof MapType mapType) {
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            GroupColumnIO keyValueColumnIO = getMapKeyValueColumn(groupColumnIO);
            if (keyValueColumnIO.getChildrenCount() != 2) {
                return Optional.empty();
            }
            Optional<Field> keyField = constructField(mapType.getKeyType(), keyValueColumnIO.getChild(0));
            Optional<Field> valueField = constructField(mapType.getValueType(), keyValueColumnIO.getChild(1));
            return Optional.of(new GroupField(trinoType, repetitionLevel, definitionLevel, required, ImmutableList.of(keyField, valueField)));
        }

        if (trinoType instanceof ArrayType arrayType) {
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            if (groupColumnIO.getChildrenCount() != 1) {
                return Optional.empty();
            }
            ColumnIO elementColumnIO = getArrayElementColumn(groupColumnIO.getChild(0));
            Optional<Field> elementField = constructField(arrayType.getElementType(), elementColumnIO);
            return Optional.of(new GroupField(trinoType, repetitionLevel, definitionLevel, required, ImmutableList.of(elementField)));
        }

        // Primitive type
        PrimitiveColumnIO primitiveColumnIO = (PrimitiveColumnIO) columnIO;
        return Optional.of(new PrimitiveField(
                trinoType,
                required,
                primitiveColumnIO.getColumnDescriptor(),
                primitiveColumnIO.getId()));
    }
}
