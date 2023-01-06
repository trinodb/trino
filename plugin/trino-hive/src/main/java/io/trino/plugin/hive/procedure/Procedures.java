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
package io.trino.plugin.hive.procedure;

import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.TrinoException;

import java.util.List;
import java.util.Objects;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.TableType.MATERIALIZED_VIEW;
import static io.trino.plugin.hive.TableType.VIRTUAL_VIEW;
import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;

final class Procedures
{
    private Procedures() {}

    public static void checkIsPartitionedTable(Table table)
    {
        if (table.getTableType().equals(VIRTUAL_VIEW.name())) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Table is a view: " + table.getSchemaTableName());
        }

        if (table.getTableType().equals(MATERIALIZED_VIEW.name())) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Table is a materialized view: " + table.getSchemaTableName());
        }

        if (table.getPartitionColumns().isEmpty()) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Table is not partitioned: " + table.getSchemaTableName());
        }
    }

    public static void checkPartitionColumns(Table table, List<String> expectedPartitions)
    {
        List<String> actualPartitionColumnNames = table.getPartitionColumns().stream()
                .map(Column::getName)
                .collect(toImmutableList());

        if (!Objects.equals(expectedPartitions, actualPartitionColumnNames)) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Provided partition column names do not match actual partition column names: " + actualPartitionColumnNames);
        }
    }
}
