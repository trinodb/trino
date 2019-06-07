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
package io.prestosql.metadata;

import com.google.common.collect.ImmutableList;
import io.prestosql.connector.CatalogName;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.DiscretePredicates;
import io.prestosql.spi.connector.LocalProperty;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.sql.planner.PartitioningHandle;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class TableProperties
{
    private final ConnectorTableProperties tableProperties;
    private final CatalogName catalogName;
    private final ConnectorTransactionHandle transaction;

    public TableProperties(CatalogName catalogName, ConnectorTransactionHandle transaction, ConnectorTableProperties tableProperties)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(transaction, "transaction is null");
        requireNonNull(tableProperties, "layout is null");

        this.catalogName = catalogName;
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
                                Optional.of(catalogName),
                                Optional.of(transaction),
                                nodePartitioning.getPartitioningHandle()),
                        nodePartitioning.getPartitioningColumns()));
    }

    public Optional<Set<ColumnHandle>> getStreamPartitioningColumns()
    {
        return tableProperties.getStreamPartitioningColumns();
    }

    public Optional<DiscretePredicates> getDiscretePredicates()
    {
        return tableProperties.getDiscretePredicates();
    }

    public static class TablePartitioning
    {
        private final PartitioningHandle partitioningHandle;
        private final List<ColumnHandle> partitioningColumns;

        public TablePartitioning(PartitioningHandle partitioningHandle, List<ColumnHandle> partitioningColumns)
        {
            this.partitioningHandle = requireNonNull(partitioningHandle, "partitioningHandle is null");
            this.partitioningColumns = ImmutableList.copyOf(requireNonNull(partitioningColumns, "partitioningColumns is null"));
        }

        public PartitioningHandle getPartitioningHandle()
        {
            return partitioningHandle;
        }

        public List<ColumnHandle> getPartitioningColumns()
        {
            return partitioningColumns;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TablePartitioning that = (TablePartitioning) o;
            return Objects.equals(partitioningHandle, that.partitioningHandle) &&
                    Objects.equals(partitioningColumns, that.partitioningColumns);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(partitioningHandle, partitioningColumns);
        }
    }
}
