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
package io.trino.plugin.starrocks;

import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public record StarRocksTableHandle(
        String schemaName,
        String tableName,
        Optional<String> remoteCatalogName,
        String remoteSchemaName,
        String remoteTableName,
        StarRocksRelationType relationType,
        TupleDomain<StarRocksColumnHandle> constraint,
        OptionalLong limit,
        List<StarRocksSortItem> sortOrder,
        Optional<StarRocksAggregation> aggregation,
        Optional<List<StarRocksColumnHandle>> projectedColumns)
        implements ConnectorTableHandle
{
    public StarRocksTableHandle(
            String schemaName,
            String tableName,
            Optional<String> remoteCatalogName,
            String remoteSchemaName,
            String remoteTableName,
            StarRocksRelationType relationType)
    {
        this(schemaName,
                tableName,
                remoteCatalogName,
                remoteSchemaName,
                remoteTableName,
                relationType,
                TupleDomain.all(),
                OptionalLong.empty(),
                List.of(),
                Optional.empty(),
                Optional.empty());
    }

    public StarRocksTableHandle
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(remoteCatalogName, "remoteCatalogName is null");
        requireNonNull(remoteSchemaName, "remoteSchemaName is null");
        requireNonNull(remoteTableName, "remoteTableName is null");
        requireNonNull(relationType, "relationType is null");
        requireNonNull(constraint, "constraint is null");
        requireNonNull(limit, "limit is null");
        sortOrder = List.copyOf(requireNonNull(sortOrder, "sortOrder is null"));
        requireNonNull(aggregation, "aggregation is null");
        projectedColumns = requireNonNull(projectedColumns, "projectedColumns is null")
                .map(List::copyOf);
    }

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    public StarRocksTableHandle withConstraint(TupleDomain<StarRocksColumnHandle> constraint)
    {
        return new StarRocksTableHandle(schemaName, tableName, remoteCatalogName, remoteSchemaName, remoteTableName, relationType, constraint, limit, sortOrder, aggregation, projectedColumns);
    }

    public StarRocksTableHandle withLimit(long limit)
    {
        OptionalLong newLimit = this.limit.isPresent() ? OptionalLong.of(Math.min(this.limit.getAsLong(), limit)) : OptionalLong.of(limit);
        return new StarRocksTableHandle(schemaName, tableName, remoteCatalogName, remoteSchemaName, remoteTableName, relationType, constraint, newLimit, sortOrder, aggregation, projectedColumns);
    }

    public StarRocksTableHandle withTopN(long limit, List<StarRocksSortItem> sortOrder)
    {
        OptionalLong newLimit = this.limit.isPresent() ? OptionalLong.of(Math.min(this.limit.getAsLong(), limit)) : OptionalLong.of(limit);
        return new StarRocksTableHandle(schemaName, tableName, remoteCatalogName, remoteSchemaName, remoteTableName, relationType, constraint, newLimit, sortOrder, aggregation, projectedColumns);
    }

    public StarRocksTableHandle withAggregation(StarRocksAggregation aggregation)
    {
        List<StarRocksColumnHandle> projectedColumns = new ArrayList<>(aggregation.groupingColumns());
        for (int index = 0; index < aggregation.aggregateColumns().size(); index++) {
            StarRocksAggregateColumn aggregateColumn = aggregation.aggregateColumns().get(index);
            projectedColumns.add(new StarRocksColumnHandle(
                    aggregateColumn.columnName(),
                    aggregateColumn.columnName(),
                    aggregateColumn.type(),
                    aggregation.groupingColumns().size() + index));
        }
        return new StarRocksTableHandle(schemaName, tableName, remoteCatalogName, remoteSchemaName, remoteTableName, relationType, constraint, OptionalLong.empty(), List.of(), Optional.of(aggregation), Optional.of(projectedColumns));
    }

    public StarRocksTableHandle withProjectedColumns(List<StarRocksColumnHandle> projectedColumns)
    {
        return new StarRocksTableHandle(schemaName, tableName, remoteCatalogName, remoteSchemaName, remoteTableName, relationType, constraint, limit, sortOrder, aggregation, Optional.of(projectedColumns));
    }
}
