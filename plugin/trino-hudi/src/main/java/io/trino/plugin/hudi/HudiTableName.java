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
package io.trino.plugin.hudi;

import io.trino.spi.TrinoException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class HudiTableName
{
    private static final Pattern TABLE_PATTERN = Pattern.compile("" +
            "(?<table>[^$@]+)" +
            "(?:\\$(?<type>[^@]+))?");

    private final String tableName;
    private final TableType tableType;

    public HudiTableName(String tableName, TableType tableType)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tableType = requireNonNull(tableType, "tableType is null");
    }

    public String getTableName()
    {
        return tableName;
    }

    public TableType getTableType()
    {
        return tableType;
    }

    public String getTableNameWithType()
    {
        return tableName + "$" + tableType.name().toLowerCase(ENGLISH);
    }

    @Override
    public String toString()
    {
        return getTableNameWithType();
    }

    public static HudiTableName from(String name)
    {
        Matcher match = TABLE_PATTERN.matcher(name);
        if (!match.matches()) {
            throw new TrinoException(NOT_SUPPORTED, "Invalid Hudi table name: " + name);
        }

        String table = match.group("table");
        String typeString = match.group("type");

        TableType type = TableType.DATA;
        if (typeString != null) {
            try {
                type = TableType.valueOf(typeString.toUpperCase(ENGLISH));
            }
            catch (IllegalArgumentException e) {
                throw new TrinoException(NOT_SUPPORTED, format("Invalid Hudi table name (unknown type '%s'): %s", typeString, name));
            }
        }

        return new HudiTableName(table, type);
    }
}
