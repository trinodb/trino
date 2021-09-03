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
import io.trino.parquet.RichColumnDescriptor;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.GroupColumnIO;
import org.apache.parquet.io.PrimitiveColumnIO;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.parquet.ParquetTypeUtils.getArrayElementColumn;
import static io.trino.parquet.ParquetTypeUtils.getMapKeyValueColumn;
import static io.trino.parquet.ParquetTypeUtils.lookupColumnById;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.io.ColumnIOUtil.columnDefinitionLevel;
import static org.apache.parquet.io.ColumnIOUtil.columnRepetitionLevel;
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
        int repetitionLevel = columnRepetitionLevel(columnIO);
        int definitionLevel = columnDefinitionLevel(columnIO);
        Type type = context.getType();
        if (type instanceof RowType) {
            RowType rowType = (RowType) type;
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            ImmutableList.Builder<Optional<Field>> fieldsBuilder = ImmutableList.builder();
            List<RowType.Field> fields = rowType.getFields();
            boolean structHasParameters = false;
            for (int i = 0; i < fields.size(); i++) {
                RowType.Field rowField = fields.get(i);
                ColumnIdentity fieldIdentity = context.getColumnIdentity().getChildren().get(i);
                Optional<Field> field = constructField(new FieldContext(rowField.getType(), fieldIdentity), lookupColumnById(groupColumnIO, fieldIdentity.getId()));
                structHasParameters |= field.isPresent();
                fieldsBuilder.add(field);
            }
            if (structHasParameters) {
                return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, fieldsBuilder.build()));
            }
            return Optional.empty();
        }
        if (type instanceof MapType) {
            MapType mapType = (MapType) type;
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            GroupColumnIO keyValueColumnIO = getMapKeyValueColumn(groupColumnIO);
            if (keyValueColumnIO.getChildrenCount() != 2) {
                return Optional.empty();
            }
            checkArgument(context.getColumnIdentity().getChildren().size() == 2, "Not a map: %s", context);
            ColumnIdentity keyIdentity = context.getColumnIdentity().getChildren().get(0);
            ColumnIdentity valueIdentity = context.getColumnIdentity().getChildren().get(1);
            // TODO validate column ID
            Optional<Field> keyField = constructField(new FieldContext(mapType.getKeyType(), keyIdentity), keyValueColumnIO.getChild(0));
            // TODO validate column ID
            Optional<Field> valueField = constructField(new FieldContext(mapType.getValueType(), valueIdentity), keyValueColumnIO.getChild(1));
            return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, ImmutableList.of(keyField, valueField)));
        }
        if (type instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) type;
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            if (groupColumnIO.getChildrenCount() != 1) {
                return Optional.empty();
            }
            checkArgument(context.getColumnIdentity().getChildren().size() == 1, "Not an array: %s", context);
            ColumnIdentity elementIdentity = getOnlyElement(context.getColumnIdentity().getChildren());
            // TODO validate column ID
            Optional<Field> field = constructField(new FieldContext(arrayType.getElementType(), elementIdentity), getArrayElementColumn(groupColumnIO.getChild(0)));
            return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, ImmutableList.of(field)));
        }
        PrimitiveColumnIO primitiveColumnIO = (PrimitiveColumnIO) columnIO;
        RichColumnDescriptor column = new RichColumnDescriptor(primitiveColumnIO.getColumnDescriptor(), columnIO.getType().asPrimitiveType());
        return Optional.of(new PrimitiveField(type, repetitionLevel, definitionLevel, required, column, primitiveColumnIO.getId()));
    }

    public static class FieldContext
    {
        private final Type type;
        private final ColumnIdentity columnIdentity;

        public FieldContext(Type type, ColumnIdentity columnIdentity)
        {
            this.type = requireNonNull(type, "type is null");
            this.columnIdentity = requireNonNull(columnIdentity, "columnIdentity is null");
        }

        public Type getType()
        {
            return type;
        }

        public ColumnIdentity getColumnIdentity()
        {
            return columnIdentity;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("type", type)
                    .add("columnIdentity", columnIdentity)
                    .toString();
        }
    }
}
