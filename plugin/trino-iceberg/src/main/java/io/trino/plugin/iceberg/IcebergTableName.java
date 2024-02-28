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

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.iceberg.TableType.DATA;
import static io.trino.plugin.iceberg.TableType.MATERIALIZED_VIEW_STORAGE;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class IcebergTableName
{
    private IcebergTableName() {}

    private static final Pattern TABLE_PATTERN;

    static {
        String referencableTableTypes = Stream.of(TableType.values())
                .filter(tableType -> tableType != DATA)
                .map(tableType -> tableType.name().toLowerCase(ENGLISH))
                .collect(Collectors.joining("|"));
        TABLE_PATTERN = Pattern.compile("" +
                "(?<table>[^$@]+)" +
                "(?:\\$(?<type>(?i:" + referencableTableTypes + ")))?");
    }

    public static boolean isIcebergTableName(String tableName)
    {
        return TABLE_PATTERN.matcher(tableName).matches();
    }

    public static String tableNameWithType(String tableName, TableType tableType)
    {
        requireNonNull(tableName, "tableName is null");
        return tableName + "$" + tableType.name().toLowerCase(ENGLISH);
    }

    public static String tableNameFrom(String validIcebergTableName)
    {
        Matcher match = TABLE_PATTERN.matcher(validIcebergTableName);
        checkArgument(match.matches(), "Invalid Iceberg table name: %s", validIcebergTableName);
        return match.group("table");
    }

    public static TableType tableTypeFrom(String validIcebergTableName)
    {
        Matcher match = TABLE_PATTERN.matcher(validIcebergTableName);
        checkArgument(match.matches(), "Invalid Iceberg table name: %s", validIcebergTableName);

        String typeString = match.group("type");
        if (typeString == null) {
            return DATA;
        }
        TableType parsedType = TableType.valueOf(typeString.toUpperCase(ENGLISH));
        // $data cannot be encoded in table name
        verify(parsedType != DATA, "parsedType is unexpectedly DATA");
        return parsedType;
    }

    public static boolean isDataTable(String validIcebergTableName)
    {
        Matcher match = TABLE_PATTERN.matcher(validIcebergTableName);
        checkArgument(match.matches(), "Invalid Iceberg table name: %s", validIcebergTableName);
        String typeString = match.group("type");
        return typeString == null;
    }

    public static boolean isMaterializedViewStorage(String validIcebergTableName)
    {
        return tableTypeFrom(validIcebergTableName) == MATERIALIZED_VIEW_STORAGE;
    }
}
