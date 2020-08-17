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
package io.prestosql.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.RowType.Field;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.prestosql.plugin.iceberg.TypeConverter.toPrestoType;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static org.apache.iceberg.types.Types.NestedField.optional;

public class FilesTable
{
    public static final IcebergColumnHandle FILE_PATH = new IcebergColumnHandle(-1, "file_path", VARCHAR, Optional.empty());
    public static final IcebergColumnHandle FILE_FORMAT = new IcebergColumnHandle(-2, "file_format", VARCHAR, Optional.empty());
    public static final IcebergColumnHandle RECORD_COUNT = new IcebergColumnHandle(-3, "record_count", BIGINT, Optional.empty());
    public static final IcebergColumnHandle FILE_SIZE_IN_BYTES = new IcebergColumnHandle(-4, "file_size_in_bytes", BIGINT, Optional.empty());
    public static final IcebergColumnHandle FILE_ORDINAL = new IcebergColumnHandle(-5, "file_ordinal", INTEGER, Optional.empty());
    public static final IcebergColumnHandle SORT_COLUMNS = new IcebergColumnHandle(-6, "sort_columns", new ArrayType(INTEGER), Optional.empty());
    public static final IcebergColumnHandle KEY_METADATA = new IcebergColumnHandle(-7, "key_metadata", VARBINARY, Optional.empty());
    public static final IcebergColumnHandle SPLIT_OFFSETS = new IcebergColumnHandle(-8, "split_offsets", new ArrayType(BIGINT), Optional.empty());
    public static final String COLUMN_SIZE = "column_size";
    public static final String VALUE_COUNTS = "value_counts";
    public static final String NULL_VALUE_COUNTS = "null_value_counts";
    public static final String LOWER_BOUND = "lower_bound";
    public static final String UPPER_BOUND = "upper_bound";
    private final List<IcebergColumnHandle> columnHandles;

    public FilesTable(Schema schema, TypeManager typeManager)
    {
        ImmutableList.Builder<IcebergColumnHandle> columnHandleBuilder = new ImmutableList.Builder<>();
        columnHandleBuilder.add(FILE_PATH);
        columnHandleBuilder.add(FILE_FORMAT);
        columnHandleBuilder.add(RECORD_COUNT);
        columnHandleBuilder.add(FILE_SIZE_IN_BYTES);
        columnHandleBuilder.add(FILE_ORDINAL);
        columnHandleBuilder.add(SORT_COLUMNS);
        columnHandleBuilder.add(KEY_METADATA);
        columnHandleBuilder.add(SPLIT_OFFSETS);

        List<Field> fields = Lists.newArrayList(
                new Field(Optional.of(COLUMN_SIZE), BIGINT),
                new Field(Optional.of(VALUE_COUNTS), BIGINT),
                new Field(Optional.of(NULL_VALUE_COUNTS), BIGINT));

        final ImmutableList.Builder<Types.NestedField> columnBuilder = new ImmutableList.Builder<>();

        schema.columns().stream()
                .forEach(column -> columnBuilder.addAll(handleNestedType(column, Optional.empty())));

        columnBuilder.build()
                .forEach(column -> {
                    final Type type = toPrestoType(column.type(), typeManager);
                    List<Field> boundFields = Lists.newArrayList(
                            new Field(Optional.of(LOWER_BOUND), type),
                            new Field(Optional.of(UPPER_BOUND), type));
                    final RowType rowType = RowType.from(Lists.newArrayList(Iterables.concat(fields, boundFields)));
                    final IcebergColumnHandle columnHandle = new IcebergColumnHandle(column.fieldId(), column.name(), rowType, Optional.empty());
                    columnHandleBuilder.add(columnHandle);
                });

        columnHandles = columnHandleBuilder.build();
    }

    public List<IcebergColumnHandle> getColumnHandles()
    {
        return columnHandles;
    }

    public List<ColumnMetadata> getColumnMetadata()
    {
        return columnHandles.stream()
                .map(column -> new ColumnMetadata(column.getName(), column.getType()))
                .collect(Collectors.toList());
    }

    public static List<Types.NestedField> handleNestedType(Types.NestedField nestedField, Optional<String> name)
    {
        org.apache.iceberg.types.Type type = nestedField.type();
        String fullName = name.map(n -> format("%s.%s", n, nestedField.name())).orElse(nestedField.name());
        if (type.isPrimitiveType()) {
            return List.of(optional(nestedField.fieldId(), fullName, nestedField.type()));
        }
        else {
            ImmutableList.Builder<Types.NestedField> builder = new ImmutableList.Builder<>();
            builder.add(optional(nestedField.fieldId(), fullName, nestedField.type()));
            type.asNestedType().fields()
                    .forEach(field -> builder.addAll(handleNestedType(field, Optional.of(fullName))));
            return builder.build();
        }
    }
}
