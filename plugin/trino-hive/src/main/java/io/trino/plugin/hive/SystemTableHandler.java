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
package io.trino.plugin.hive;

import io.trino.spi.connector.SchemaTableName;

import static java.util.Locale.ENGLISH;

public enum SystemTableHandler
{
    PARTITIONS, PROPERTIES;

    private final String suffix;

    SystemTableHandler()
    {
        this.suffix = "$" + name().toLowerCase(ENGLISH);
    }

    boolean matches(SchemaTableName table)
    {
        return table.getTableName().endsWith(suffix) &&
                (table.getTableName().length() > suffix.length());
    }

    SchemaTableName getSourceTableName(SchemaTableName table)
    {
        return new SchemaTableName(
                table.getSchemaName(),
                table.getTableName().substring(0, table.getTableName().length() - suffix.length()));
    }
}
