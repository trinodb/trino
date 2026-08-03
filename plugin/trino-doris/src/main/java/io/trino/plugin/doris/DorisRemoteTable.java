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
package io.trino.plugin.doris;

import io.trino.spi.connector.SchemaTableName;

import java.util.Comparator;
import java.util.List;

import static java.util.Objects.requireNonNull;

public record DorisRemoteTable(
        SchemaTableName schemaTableName,
        String remoteSchemaName,
        String remoteTableName,
        DorisRelationType relationType,
        List<DorisRemoteColumn> columns)
{
    public DorisRemoteTable(SchemaTableName schemaTableName, List<DorisRemoteColumn> columns)
    {
        this(requireSchemaTableName(schemaTableName), schemaTableName.getSchemaName(), schemaTableName.getTableName(), DorisRelationType.TABLE, columns);
    }

    public DorisRemoteTable(
            SchemaTableName schemaTableName,
            String remoteSchemaName,
            String remoteTableName,
            List<DorisRemoteColumn> columns)
    {
        this(schemaTableName, remoteSchemaName, remoteTableName, DorisRelationType.TABLE, columns);
    }

    public DorisRemoteTable
    {
        requireNonNull(schemaTableName, "schemaTableName is null");
        requireNonNull(remoteSchemaName, "remoteSchemaName is null");
        requireNonNull(remoteTableName, "remoteTableName is null");
        requireNonNull(relationType, "relationType is null");
        requireNonNull(columns, "columns is null");

        // Doris information_schema ordinals are 1-based. Sorting once keeps downstream handle creation deterministic.
        columns = columns.stream()
                .sorted(Comparator.comparingInt(DorisRemoteColumn::ordinalPosition))
                .toList();
    }

    private static SchemaTableName requireSchemaTableName(SchemaTableName schemaTableName)
    {
        return requireNonNull(schemaTableName, "schemaTableName is null");
    }
}
