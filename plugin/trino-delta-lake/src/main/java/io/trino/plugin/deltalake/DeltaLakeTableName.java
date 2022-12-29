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
package io.trino.plugin.deltalake;

import io.trino.spi.TrinoException;

import java.util.Locale;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.plugin.deltalake.DeltaLakeTableType.DATA;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DeltaLakeTableName
{
    private static final Pattern TABLE_PATTERN = Pattern.compile("" +
            "(?<table>[^$@]+)" +
            "(?:\\$(?<type>[^@]+))?");

    private final String tableName;
    private final DeltaLakeTableType tableType;

    public DeltaLakeTableName(String tableName, DeltaLakeTableType tableType)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tableType = requireNonNull(tableType, "tableType is null");
    }

    public String getTableName()
    {
        return tableName;
    }

    public DeltaLakeTableType getTableType()
    {
        return tableType;
    }

    public String getTableNameWithType()
    {
        return tableName + "$" + tableType.name().toLowerCase(Locale.ENGLISH);
    }

    @Override
    public String toString()
    {
        return getTableNameWithType();
    }

    public static DeltaLakeTableName from(String name)
    {
        Matcher match = TABLE_PATTERN.matcher(name);
        if (!match.matches()) {
            throw new TrinoException(NOT_SUPPORTED, "Invalid Delta Lake table name: " + name);
        }

        String table = match.group("table");
        String typeString = match.group("type");

        DeltaLakeTableType type = DeltaLakeTableType.DATA;
        if (typeString != null) {
            try {
                type = DeltaLakeTableType.valueOf(typeString.toUpperCase(Locale.ENGLISH));
            }
            catch (IllegalArgumentException e) {
                throw new TrinoException(NOT_SUPPORTED, format("Invalid Delta Lake table name (unknown type '%s'): %s", typeString, name));
            }
        }

        return new DeltaLakeTableName(table, type);
    }

    public static String tableNameFrom(String name)
    {
        Matcher match = TABLE_PATTERN.matcher(name);
        if (!match.matches()) {
            throw new TrinoException(NOT_SUPPORTED, "Invalid Delta Lake table name: " + name);
        }

        return match.group("table");
    }

    public static Optional<DeltaLakeTableType> tableTypeFrom(String name)
    {
        Matcher match = TABLE_PATTERN.matcher(name);
        if (!match.matches()) {
            throw new TrinoException(NOT_SUPPORTED, "Invalid Delta Lake table name: " + name);
        }
        String typeString = match.group("type");
        if (typeString == null) {
            return Optional.of(DATA);
        }
        try {
            return Optional.of(DeltaLakeTableType.valueOf(typeString.toUpperCase(Locale.ENGLISH)));
        }
        catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    public static boolean isDataTable(String name)
    {
        Matcher match = TABLE_PATTERN.matcher(name);
        if (!match.matches()) {
            throw new TrinoException(NOT_SUPPORTED, "Invalid Delta Lake table name: " + name);
        }
        String typeString = match.group("type");
        if (typeString == null) {
            return true;
        }
        try {
            DeltaLakeTableType type = DeltaLakeTableType.valueOf(typeString.toUpperCase(Locale.ENGLISH));
            return type == DATA;
        }
        catch (IllegalArgumentException e) {
            return false;
        }
    }
}
