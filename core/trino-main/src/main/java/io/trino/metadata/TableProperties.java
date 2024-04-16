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
package io.trino.metadata;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DiscretePredicates;
import io.trino.spi.connector.LocalProperty;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.PartitioningHandle;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TableProperties
{
    private final ConnectorTableProperties tableProperties;
    private final CatalogHandle catalogHandle;
    private final ConnectorTransactionHandle transaction;

    public TableProperties(CatalogHandle catalogHandle, ConnectorTransactionHandle transaction, ConnectorTableProperties tableProperties)
    {
        requireNonNull(catalogHandle, "catalogHandle is null");
        requireNonNull(transaction, "transaction is null");
        requireNonNull(tableProperties, "tableProperties is null");

        this.catalogHandle = catalogHandle;
        this.transaction = transaction;
        this.tableProperties = tableProperties;
    }

    public TupleDomain<ColumnHandle> getPredicate()
    {
        return tableProperties.getPredicate();
    }

    public List<LocalProperty<ColumnHandle>> getLocalProperties()
    {
        return tableProperties.getLocalProperties();
    }

    public Optional<TablePartitioning> getTablePartitioning()
    {
        return tableProperties.getTablePartitioning()
                .map(nodePartitioning -> new TablePartitioning(
                        new PartitioningHandle(
                                Optional.of(catalogHandle),
                                Optional.of(transaction),
                                nodePartitioning.getPartitioningHandle()),
                        nodePartitioning.getPartitioningColumns(),
                        nodePartitioning.isSingleSplitPerPartition()));
    }

    public Optional<DiscretePredicates> getDiscretePredicates()
    {
        return tableProperties.getDiscretePredicates();
    }

    public record TablePartitioning(
            PartitioningHandle partitioningHandle,
            List<ColumnHandle> partitioningColumns,
            boolean singleSplitPerPartition)
    {
        public TablePartitioning
        {
            requireNonNull(partitioningHandle, "partitioningHandle is null");
            partitioningColumns = ImmutableList.copyOf(requireNonNull(partitioningColumns, "partitioningColumns is null"));
        }
    }
}
