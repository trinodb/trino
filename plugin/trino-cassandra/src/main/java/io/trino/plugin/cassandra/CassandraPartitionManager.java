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
package io.trino.plugin.cassandra;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.plugin.cassandra.util.CassandraCqlUtils;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class CassandraPartitionManager
{
    private static final Logger log = Logger.get(CassandraPartitionManager.class);

    private final CassandraSession cassandraSession;
    private final CassandraTypeManager cassandraTypeManager;

    @Inject
    public CassandraPartitionManager(CassandraSession cassandraSession, CassandraTypeManager cassandraTypeManager)
    {
        this.cassandraSession = requireNonNull(cassandraSession, "cassandraSession is null");
        this.cassandraTypeManager = requireNonNull(cassandraTypeManager, "cassandraTypeManager is null");
    }

    public CassandraPartitionResult getPartitions(CassandraNamedRelationHandle cassandraTableHandle, TupleDomain<ColumnHandle> tupleDomain)
    {
        // TODO support repeated applyFilter
        checkArgument(cassandraTableHandle.getPartitions().isEmpty(), "getPartitions() currently does not take into account table handle's partitions");

        CassandraTable table = cassandraSession.getTable(cassandraTableHandle.getSchemaTableName());

        // fetch the partitions
        List<CassandraPartition> allPartitions = getCassandraPartitions(table, tupleDomain);
        log.debug("%s.%s #partitions: %d", cassandraTableHandle.getSchemaName(), cassandraTableHandle.getTableName(), allPartitions.size());

        // do a final pass to filter based on fields that could not be used to build the prefix
        List<CassandraPartition> partitions = allPartitions.stream()
                .filter(partition -> tupleDomain.overlaps(partition.getTupleDomain()))
                .collect(toList());

        // All partition key domains will be fully evaluated, so we don't need to include those
        TupleDomain<ColumnHandle> remainingTupleDomain = TupleDomain.none();
        if (!tupleDomain.isNone()) {
            if (partitions.size() == 1 && partitions.get(0).isUnpartitioned()) {
                remainingTupleDomain = tupleDomain;
            }
            else {
                Set<ColumnHandle> usedPartitionColumns = partitions.stream()
                        .flatMap(partition -> Optional.ofNullable(partition.getTupleDomain())
                                .flatMap(partitionTupleDomain -> partitionTupleDomain.getDomains()
                                        .map(Map::keySet)
                                        .map(Set::stream))
                                .orElseGet(Stream::empty))
                        .collect(toImmutableSet());
                remainingTupleDomain = tupleDomain.filter((column, domain) -> !usedPartitionColumns.contains(column));
            }
        }

        // Cassandra allows pushing down indexed column fixed value predicates along with token range SELECT
        if ((partitions.size() == 1) && partitions.get(0).isUnpartitioned()) {
            Map<ColumnHandle, Domain> domains = tupleDomain.getDomains().get();
            List<ColumnHandle> indexedColumns = new ArrayList<>();
            // compose partitionId by using indexed column
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
                CassandraColumnHandle column = (CassandraColumnHandle) entry.getKey();
                Domain domain = entry.getValue();
                if (column.indexed() && domain.isSingleValue()) {
                    sb.append(CassandraCqlUtils.validColumnName(column.name()))
                            .append(" = ")
                            .append(cassandraTypeManager.toCqlLiteral(column.cassandraType(), entry.getValue().getSingleValue()));
                    indexedColumns.add(column);
                    // Only one indexed column predicate can be pushed down.
                    break;
                }
            }
            if (sb.length() > 0) {
                CassandraPartition partition = partitions.get(0);
                TupleDomain<ColumnHandle> filterIndexedColumn = TupleDomain.withColumnDomains(Maps.filterKeys(remainingTupleDomain.getDomains().get(), not(in(indexedColumns))));
                return new CassandraPartitionResult(
                        ImmutableList.of(new CassandraPartition(partition.getKey(), sb.toString(), filterIndexedColumn, true)),
                        filterIndexedColumn);
            }
        }
        return new CassandraPartitionResult(partitions, remainingTupleDomain);
    }

    private List<CassandraPartition> getCassandraPartitions(CassandraTable table, TupleDomain<ColumnHandle> tupleDomain)
    {
        if (tupleDomain.isNone()) {
            return ImmutableList.of();
        }

        List<Set<Object>> partitionKeysList = getPartitionKeysList(table, tupleDomain);

        Set<List<Object>> filterList = Sets.cartesianProduct(partitionKeysList);
        // empty filters means, all partitions
        if (filterList.isEmpty()) {
            return cassandraSession.getPartitions(table, ImmutableList.of());
        }

        return cassandraSession.getPartitions(table, partitionKeysList);
    }

    private List<Set<Object>> getPartitionKeysList(CassandraTable table, TupleDomain<ColumnHandle> tupleDomain)
    {
        ImmutableList.Builder<Set<Object>> partitionColumnValues = ImmutableList.builder();
        for (CassandraColumnHandle columnHandle : table.partitionKeyColumns()) {
            Domain domain = tupleDomain.getDomains().get().get(columnHandle);

            // if there is no constraint on a partition key, return an empty set
            if (domain == null) {
                return ImmutableList.of();
            }

            // todo does cassandra allow null partition keys?
            if (domain.isNullAllowed()) {
                return ImmutableList.of();
            }

            Set<Object> values = domain.getValues().getValuesProcessor().transform(
                    ranges -> {
                        ImmutableSet.Builder<Object> columnValues = ImmutableSet.builder();
                        for (Range range : ranges.getOrderedRanges()) {
                            // if the range is not a single value, we cannot perform partition pruning
                            if (!range.isSingleValue()) {
                                return ImmutableSet.of();
                            }
                            Object value = range.getSingleValue();

                            CassandraType valueType = columnHandle.cassandraType();
                            if (valueType.kind().isSupportedPartitionKey()) {
                                columnValues.add(value);
                            }
                        }
                        return columnValues.build();
                    },
                    discreteValues -> {
                        if (discreteValues.isInclusive()) {
                            return ImmutableSet.copyOf(discreteValues.getValues());
                        }
                        return ImmutableSet.of();
                    },
                    allOrNone -> ImmutableSet.of());
            partitionColumnValues.add(values);
        }
        return partitionColumnValues.build();
    }
}
