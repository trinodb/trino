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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.type.NamedTypeSignature;
import io.prestosql.spi.type.RowFieldName;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.type.InternalTypeManager;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.prestosql.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.prestosql.plugin.hive.HiveTestUtils.rowType;
import static io.prestosql.plugin.hive.HiveType.toHiveType;
import static io.prestosql.spi.type.BigintType.BIGINT;

public class TestHiveReaderProjectionsUtil
{
    private TestHiveReaderProjectionsUtil() {}

    public static final RowType ROWTYPE_OF_PRIMITIVES = rowType(ImmutableList.of(
            new NamedTypeSignature(Optional.of(new RowFieldName("f_bigint_0")), BIGINT.getTypeSignature()),
            new NamedTypeSignature(Optional.of(new RowFieldName("f_bigint_1")), BIGINT.getTypeSignature())));

    public static final RowType ROWTYPE_OF_ROW_AND_PRIMITIVES = rowType(ImmutableList.of(
            new NamedTypeSignature(Optional.of(new RowFieldName("f_row_0")), ROWTYPE_OF_PRIMITIVES.getTypeSignature()),
            new NamedTypeSignature(Optional.of(new RowFieldName("f_bigint_0")), BIGINT.getTypeSignature())));

    public static final TypeManager TYPE_MANAGER = new InternalTypeManager(createTestMetadataManager());

    public static Map<String, HiveColumnHandle> createTestFullColumns(List<String> names, Map<String, Type> types)
    {
        checkArgument(names.size() == types.size());

        ImmutableMap.Builder<String, HiveColumnHandle> hiveColumns = ImmutableMap.builder();

        int regularColumnHiveIndex = 0;
        for (String name : names) {
            HiveType hiveType = toHiveType(types.get(name));
            hiveColumns.put(name, createBaseColumn(name, regularColumnHiveIndex, hiveType, types.get(name), REGULAR, Optional.empty()));
            regularColumnHiveIndex++;
        }

        return hiveColumns.build();
    }

    public static HiveColumnHandle createProjectedColumnHandle(HiveColumnHandle column, List<Integer> indices)
    {
        checkArgument(column.isBaseColumn(), "base column is expected here");

        if (indices.size() == 0) {
            return column;
        }

        HiveType baseHiveType = column.getHiveType();
        List<String> names = baseHiveType.getHiveDereferenceNames(indices);
        HiveType hiveType = baseHiveType.getHiveTypeForDereferences(indices).get();

        HiveColumnProjectionInfo columnProjection = new HiveColumnProjectionInfo(indices, names, hiveType, hiveType.getType(TYPE_MANAGER));

        return new HiveColumnHandle(
                column.getBaseColumnName(),
                column.getBaseHiveColumnIndex(),
                column.getBaseHiveType(),
                column.getBaseType(),
                Optional.of(columnProjection),
                column.getColumnType(),
                column.getComment());
    }
}
