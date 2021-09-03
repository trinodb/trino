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
package io.trino.plugin.hive.parquet;

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

import java.util.Optional;

import static io.trino.parquet.ParquetTypeUtils.getArrayElementColumn;
import static io.trino.parquet.ParquetTypeUtils.getMapKeyValueColumn;
import static io.trino.parquet.ParquetTypeUtils.lookupColumnByName;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.io.ColumnIOUtil.columnDefinitionLevel;
import static org.apache.parquet.io.ColumnIOUtil.columnRepetitionLevel;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;

public abstract class ParquetColumnIOConverter<Context>
{
    public static ParquetColumnIOConverter<Type> withLookupColumnByName()
    {
        return new ByNameParquetColumnIOConverter();
    }

    public Optional<Field> constructField(Context context, Optional<ColumnIO> columnIO)
    {
        if (columnIO.isEmpty()) {
            return Optional.empty();
        }
        return constructField(context, columnIO.get());
    }

    protected Optional<Field> constructField(Context context, ColumnIO columnIO)
    {
        requireNonNull(columnIO, "columnIO is null");

        boolean required = columnIO.getType().getRepetition() != OPTIONAL;
        int repetitionLevel = columnRepetitionLevel(columnIO);
        int definitionLevel = columnDefinitionLevel(columnIO);
        Type type = getType(context);
        if (type instanceof RowType) {
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            ImmutableList.Builder<Optional<Field>> fieldsBuilder = ImmutableList.builder();
            boolean structHasParameters = false;
            for (int fieldIndex = 0; fieldIndex < type.getTypeParameters().size(); fieldIndex++) {
                Optional<Field> field = getRowFieldField(context, groupColumnIO, fieldIndex);
                structHasParameters |= field.isPresent();
                fieldsBuilder.add(field);
            }
            if (structHasParameters) {
                return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, fieldsBuilder.build()));
            }
            return Optional.empty();
        }
        if (type instanceof MapType) {
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            Optional<Field> keyField = getMapKeyField(context, groupColumnIO);
            Optional<Field> valueField = getMapValueField(context, groupColumnIO);
            if (keyField.isEmpty() || valueField.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, ImmutableList.of(keyField, valueField)));
        }
        if (type instanceof ArrayType) {
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            Optional<Field> field = getArrayElementField(context, groupColumnIO);
            if (field.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, ImmutableList.of(field)));
        }
        PrimitiveColumnIO primitiveColumnIO = (PrimitiveColumnIO) columnIO;
        RichColumnDescriptor column = new RichColumnDescriptor(primitiveColumnIO.getColumnDescriptor(), columnIO.getType().asPrimitiveType());
        return Optional.of(new PrimitiveField(type, repetitionLevel, definitionLevel, required, column, primitiveColumnIO.getId()));
    }

    protected abstract Type getType(Context context);

    protected abstract Optional<Field> getArrayElementField(Context arrayContext, GroupColumnIO groupColumnIO);

    protected abstract Optional<Field> getMapKeyField(Context mapContext, GroupColumnIO groupColumnIO);

    protected abstract Optional<Field> getMapValueField(Context mapContext, GroupColumnIO groupColumnIO);

    protected abstract Optional<Field> getRowFieldField(Context rowContext, GroupColumnIO groupColumnIO, int rowFieldIndex);

    private static class ByNameParquetColumnIOConverter
            extends ParquetColumnIOConverter<Type>
    {
        @Override
        protected Type getType(Type type)
        {
            return requireNonNull(type, "type is null");
        }

        @Override
        protected Optional<Field> getArrayElementField(Type arrayType, GroupColumnIO groupColumnIO)
        {
            if (groupColumnIO.getChildrenCount() != 1) {
                return Optional.empty();
            }
            return constructField(
                    ((ArrayType) arrayType).getElementType(),
                    getArrayElementColumn(groupColumnIO.getChild(0)));
        }

        @Override
        protected Optional<Field> getMapKeyField(Type mapType, GroupColumnIO groupColumnIO)
        {
            GroupColumnIO keyValueColumnIO = getMapKeyValueColumn(groupColumnIO);
            if (keyValueColumnIO.getChildrenCount() != 2) {
                return Optional.empty();
            }
            return constructField(
                    ((MapType) mapType).getKeyType(),
                    keyValueColumnIO.getChild(0));
        }

        @Override
        protected Optional<Field> getMapValueField(Type mapType, GroupColumnIO groupColumnIO)
        {
            GroupColumnIO keyValueColumnIO = getMapKeyValueColumn(groupColumnIO);
            if (keyValueColumnIO.getChildrenCount() != 2) {
                return Optional.empty();
            }
            return constructField(
                    ((MapType) mapType).getValueType(),
                    keyValueColumnIO.getChild(1));
        }

        @Override
        protected Optional<Field> getRowFieldField(Type rowType, GroupColumnIO groupColumnIO, int rowFieldIndex)
        {
            RowType.Field rowField = ((RowType) rowType).getFields().get(rowFieldIndex);
            String name = rowField.getName().orElseThrow();
            return Optional.ofNullable(lookupColumnByName(groupColumnIO, name.toLowerCase(ENGLISH)))
                    .flatMap(columnIO -> constructField(rowField.getType(), columnIO));
        }
    }
}
