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
package io.trino.plugin.deltalake.functions.tablechanges;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;

import java.util.List;

import static java.util.Objects.requireNonNull;

public record TableChangesTableFunctionHandle(
        SchemaTableName schemaTableName,
        long firstReadVersion,
        long tableReadVersion,
        String tableLocation,
        List<DeltaLakeColumnHandle> columns) implements ConnectorTableFunctionHandle
{
    public TableChangesTableFunctionHandle {
        requireNonNull(schemaTableName, "schemaTableName is null");
        requireNonNull(tableLocation, "tableLocation is null");
        columns = ImmutableList.copyOf(columns);
    }
}
