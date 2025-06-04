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
package io.trino.plugin.iceberg;

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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.parquet.ParquetTypeUtils.getArrayElementColumn;
import static io.trino.parquet.ParquetTypeUtils.getMapKeyValueColumn;
import static io.trino.parquet.ParquetTypeUtils.lookupColumnById;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;

public final class IcebergParquetColumnIOConverter
{
    private IcebergParquetColumnIOConverter() {}

    public static Optional<Field> constructField(FieldContext context, ColumnIO columnIO)
    {
        requireNonNull(context, "context is null");
        if (columnIO == null) {
            return Optional.empty();
        }
        boolean required = columnIO.getType().getRepetition() != OPTIONAL;
        int repetitionLevel = columnIO.getRepetitionLevel();
        int definitionLevel = columnIO.getDefinitionLevel();
        Type type = context.type();
        if (type instanceof RowType rowType) {
            List<ColumnIdentity> subColumns = context.columnIdentity().getChildren();
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            ImmutableList.Builder<Optional<Field>> fieldsBuilder = ImmutableList.builder();
            List<RowType.Field> fields = rowType.getFields();
            boolean structHasParameters = false;
            for (int i = 0; i < fields.size(); i++) {
                RowType.Field rowField = fields.get(i);
                ColumnIdentity fieldIdentity = subColumns.get(i);
                Optional<Field> field = constructField(new FieldContext(rowField.getType(), fieldIdentity), lookupColumnById(groupColumnIO, fieldIdentity.getId()));
                structHasParameters |= field.isPresent();
                fieldsBuilder.add(field);
            }
            if (structHasParameters) {
                return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, fieldsBuilder.build()));
            }
            return Optional.empty();
        }
        if (type instanceof MapType mapType) {
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            GroupColumnIO keyValueColumnIO = getMapKeyValueColumn(groupColumnIO);
            if (keyValueColumnIO.getChildrenCount() != 2) {
                return Optional.empty();
            }
            List<ColumnIdentity> subColumns = context.columnIdentity().getChildren();
            checkArgument(subColumns.size() == 2, "Not a map: %s", context);
            ColumnIdentity keyIdentity = subColumns.get(0);
            ColumnIdentity valueIdentity = subColumns.get(1);
            // TODO validate column ID
            Optional<Field> keyField = constructField(new FieldContext(mapType.getKeyType(), keyIdentity), keyValueColumnIO.getChild(0));
            // TODO validate column ID
            Optional<Field> valueField = constructField(new FieldContext(mapType.getValueType(), valueIdentity), keyValueColumnIO.getChild(1));
            return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, ImmutableList.of(keyField, valueField)));
        }
        if (type instanceof ArrayType arrayType) {
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            if (groupColumnIO.getChildrenCount() != 1) {
                return Optional.empty();
            }
            List<ColumnIdentity> subColumns = context.columnIdentity().getChildren();
            checkArgument(subColumns.size() == 1, "Not an array: %s", context);
            ColumnIdentity elementIdentity = getOnlyElement(subColumns);
            // TODO validate column ID
            Optional<Field> field = constructField(new FieldContext(arrayType.getElementType(), elementIdentity), getArrayElementColumn(groupColumnIO.getChild(0)));
            return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, ImmutableList.of(field)));
        }
        PrimitiveColumnIO primitiveColumnIO = (PrimitiveColumnIO) columnIO;
        return Optional.of(new PrimitiveField(type, required, primitiveColumnIO.getColumnDescriptor(), primitiveColumnIO.getId()));
    }

    public record FieldContext(Type type, ColumnIdentity columnIdentity)
    {
        public FieldContext
        {
            requireNonNull(type, "type is null");
            requireNonNull(columnIdentity, "columnIdentity is null");
        }
    }
}
