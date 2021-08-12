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
package io.trino.connector.system;

import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.Optional;

import static io.trino.connector.system.jdbc.FilterUtil.tryGetSingleVarcharValue;
import static io.trino.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;

public class TrinoMaterializedViewsSystemTableAdapter
        implements MaterializedViewsSystemTableAdapter
{
    @Override
    public Optional<String> getCatalogNameFilter(TupleDomain<Integer> constraint)
    {
        return tryGetSingleVarcharValue(constraint, 0);
    }

    @Override
    public Optional<String> getSchemaNameFilter(TupleDomain<Integer> constraint)
    {
        return tryGetSingleVarcharValue(constraint, 1);
    }

    @Override
    public Optional<String> getTableNameFilter(TupleDomain<Integer> constraint)
    {
        return tryGetSingleVarcharValue(constraint, 2);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return tableMetadataBuilder(new SchemaTableName("metadata", "materialized_views"))
                .column("catalog_name", createUnboundedVarcharType())
                .column("schema_name", createUnboundedVarcharType())
                .column("name", createUnboundedVarcharType())
                .column("storage_catalog", createUnboundedVarcharType())
                .column("storage_schema", createUnboundedVarcharType())
                .column("storage_table", createUnboundedVarcharType())
                .column("is_fresh", BOOLEAN)
                .column("owner", createUnboundedVarcharType())
                .column("comment", createUnboundedVarcharType())
                .column("definition", createUnboundedVarcharType())
                .build();
    }

    @Override
    public Object[] toTableRow(MaterializedViewInfo materializedView)
    {
        return new Object[] {
                materializedView.getCatalogName(),
                materializedView.getSchemaName(),
                materializedView.getName(),
                materializedView.getStorageCatalog(),
                materializedView.getStorageSchema(),
                materializedView.getStorageTable(),
                materializedView.isFresh(),
                materializedView.getOwner(),
                materializedView.getComment(),
                materializedView.getOriginalSql()
        };
    }
}
