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
package io.trino.plugin.hive.s3select;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.type.TypeInfo;
import io.trino.spi.predicate.TupleDomain;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.hive.type.TypeInfoUtils.getTypeInfosFromTypeString;
import static io.trino.plugin.hive.util.SerdeConstants.COLUMN_NAME_DELIMITER;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMNS;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMN_TYPES;

public final class S3SelectUtils
{
    private S3SelectUtils()
    { }

    public static boolean hasFilters(
            Properties schema,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            List<HiveColumnHandle> readerColumns)
    {
        //There are no effective predicates and readercolumns and columntypes are identical to schema
        //means getting all data out of S3. We can use S3 GetObject instead of S3 SelectObjectContent in these cases.
        if (effectivePredicate.isAll()) {
            return !isEquivalentSchema(readerColumns, schema);
        }
        return true;
    }

    private static boolean isEquivalentSchema(List<HiveColumnHandle> readerColumns, Properties schema)
    {
        Set<String> projectedColumnNames = getColumnProperty(readerColumns, HiveColumnHandle::getName);
        Set<String> projectedColumnTypes = getColumnProperty(readerColumns, column -> column.getHiveType().getTypeInfo().getTypeName());
        return isEquivalentColumns(projectedColumnNames, schema) && isEquivalentColumnTypes(projectedColumnTypes, schema);
    }

    private static boolean isEquivalentColumns(Set<String> projectedColumnNames, Properties schema)
    {
        Set<String> columnNames;
        String columnNameProperty = schema.getProperty(LIST_COLUMNS);
        if (columnNameProperty.length() == 0) {
            columnNames = ImmutableSet.of();
        }
        else {
            String columnNameDelimiter = (String) schema.getOrDefault(COLUMN_NAME_DELIMITER, ",");
            columnNames = Arrays.stream(columnNameProperty.split(columnNameDelimiter))
                    .collect(toImmutableSet());
        }
        return projectedColumnNames.equals(columnNames);
    }

    private static boolean isEquivalentColumnTypes(Set<String> projectedColumnTypes, Properties schema)
    {
        String columnTypeProperty = schema.getProperty(LIST_COLUMN_TYPES);
        Set<String> columnTypes;
        if (columnTypeProperty.length() == 0) {
            columnTypes = ImmutableSet.of();
        }
        else {
            columnTypes = getTypeInfosFromTypeString(columnTypeProperty)
                    .stream()
                    .map(TypeInfo::getTypeName)
                    .collect(toImmutableSet());
        }
        return projectedColumnTypes.equals(columnTypes);
    }

    private static Set<String> getColumnProperty(List<HiveColumnHandle> readerColumns, Function<HiveColumnHandle, String> mapper)
    {
        if (readerColumns.isEmpty()) {
            return ImmutableSet.of();
        }
        return readerColumns.stream()
                .map(mapper)
                .collect(toImmutableSet());
    }
}
