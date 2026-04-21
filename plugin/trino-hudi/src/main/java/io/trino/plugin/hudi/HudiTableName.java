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

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.hudi.TableType.DATA;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public record HudiTableName(String tableName, TableType tableType)
{
    private static final Pattern TABLE_PATTERN = Pattern.compile(
            "(?<table>[^$@]+)(?:\\$(?<type>(?i:timeline)))?");

    public HudiTableName(String tableName, TableType tableType)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tableType = requireNonNull(tableType, "tableType is null");
    }

    public String tableNameWithType()
    {
        return tableName + "$" + tableType.name().toLowerCase(ENGLISH);
    }

    @Override
    public String toString()
    {
        return tableNameWithType();
    }

    public static Optional<HudiTableName> from(String name)
    {
        Matcher match = TABLE_PATTERN.matcher(name);
        if (!match.matches()) {
            return Optional.empty();
        }

        String table = match.group("table");
        String typeString = match.group("type");

        TableType type = DATA;
        if (typeString != null) {
            type = TableType.valueOf(typeString.toUpperCase(ENGLISH));
            verify(type != DATA, "parsedType is unexpectedly DATA");
        }

        return Optional.of(new HudiTableName(table, type));
    }
}
