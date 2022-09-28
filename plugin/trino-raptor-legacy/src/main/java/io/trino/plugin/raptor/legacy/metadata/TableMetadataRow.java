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
package io.trino.plugin.raptor.legacy.metadata;

import org.jdbi.v3.core.mapper.reflect.ColumnName;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public class TableMetadataRow
{
    private final long tableId;
    private final String schemaName;
    private final String tableName;
    private final OptionalLong temporalColumnId;
    private final Optional<String> distributionName;
    private final OptionalInt bucketCount;
    private final boolean organized;

    public TableMetadataRow(
            long tableId,
            String schemaName,
            String tableName,
            OptionalLong temporalColumnId,
            Optional<String> distributionName,
            OptionalInt bucketCount,
            @ColumnName("organization_enabled") boolean organized)
    {
        this.tableId = tableId;
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.temporalColumnId = requireNonNull(temporalColumnId, "temporalColumnId is null");
        this.distributionName = requireNonNull(distributionName, "distributionName is null");
        this.bucketCount = requireNonNull(bucketCount, "bucketCount is null");
        this.organized = organized;
    }

    public long getTableId()
    {
        return tableId;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public OptionalLong getTemporalColumnId()
    {
        return temporalColumnId;
    }

    public Optional<String> getDistributionName()
    {
        return distributionName;
    }

    public OptionalInt getBucketCount()
    {
        return bucketCount;
    }

    public boolean isOrganized()
    {
        return organized;
    }
}
