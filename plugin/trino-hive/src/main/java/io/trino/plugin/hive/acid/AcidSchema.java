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
package io.trino.plugin.hive.acid;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metastore.HiveType;
import io.trino.metastore.HiveTypeName;
import io.trino.spi.type.RowType;
import io.trino.spi.type.RowType.Field;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.metastore.HiveType.HIVE_INT;
import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMNS;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMN_TYPES;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RowType.field;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public final class AcidSchema
{
    // ACID format column names
    public static final String ACID_COLUMN_OPERATION = "operation";
    public static final String ACID_COLUMN_ORIGINAL_TRANSACTION = "originalTransaction";
    public static final String ACID_COLUMN_BUCKET = "bucket";
    public static final String ACID_COLUMN_ROW_ID = "rowId";
    public static final String ACID_COLUMN_ROW_STRUCT = "row";
    public static final String ACID_COLUMN_CURRENT_TRANSACTION = "currentTransaction";

    public static final List<String> ACID_COLUMN_NAMES = ImmutableList.of(
            ACID_COLUMN_OPERATION,
            ACID_COLUMN_ORIGINAL_TRANSACTION,
            ACID_COLUMN_BUCKET,
            ACID_COLUMN_ROW_ID,
            ACID_COLUMN_CURRENT_TRANSACTION,
            ACID_COLUMN_ROW_STRUCT);
    public static final List<Field> ACID_READ_FIELDS = ImmutableList.of(
            field(ACID_COLUMN_ORIGINAL_TRANSACTION, BIGINT),
            field(ACID_COLUMN_BUCKET, INTEGER),
            field(ACID_COLUMN_ROW_ID, BIGINT));

    public static final RowType ACID_ROW_ID_ROW_TYPE = RowType.from(ACID_READ_FIELDS);

    private AcidSchema() {}

    public static Map<String, String> createAcidSchema(HiveType rowType)
    {
        return ImmutableMap.<String, String>builder()
                .put(LIST_COLUMNS, String.join(",", ACID_COLUMN_NAMES))
                // We must supply an accurate row type, because Apache ORC code we don't control has a consistency
                // check that the layout of this "row" must agree with the layout of an inserted row.
                .put(LIST_COLUMN_TYPES, createAcidColumnHiveTypes(rowType).stream()
                        .map(HiveType::getHiveTypeName)
                        .map(HiveTypeName::toString)
                        .collect(joining(":")))
                .buildOrThrow();
    }

    public static Type createRowType(List<String> names, List<Type> types)
    {
        requireNonNull(names, "names is null");
        requireNonNull(types, "types is null");
        checkArgument(names.size() == types.size(), "names size %s differs from types size %s", names.size(), types.size());
        ImmutableList.Builder<Field> builder = ImmutableList.builder();
        for (int i = 0; i < names.size(); i++) {
            builder.add(new Field(Optional.of(names.get(i)), types.get(i)));
        }
        return RowType.from(builder.build());
    }

    public static List<HiveType> createAcidColumnHiveTypes(HiveType rowType)
    {
        return ImmutableList.of(HIVE_INT, HIVE_LONG, HIVE_INT, HIVE_LONG, HIVE_LONG, rowType);
    }

    public static List<Type> createAcidColumnTrinoTypes(Type rowType)
    {
        return ImmutableList.of(INTEGER, BIGINT, INTEGER, BIGINT, BIGINT, rowType);
    }
}
