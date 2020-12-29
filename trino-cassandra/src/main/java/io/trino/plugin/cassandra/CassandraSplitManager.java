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
package io.prestosql.plugin.cassandra;

import com.datastax.driver.core.Host;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.prestosql.plugin.cassandra.util.HostAddressFactory;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.DynamicFilter;
import io.prestosql.spi.connector.FixedSplitSource;
import io.prestosql.spi.predicate.TupleDomain;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.prestosql.plugin.cassandra.CassandraSessionProperties.getSplitsPerNode;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class CassandraSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(CassandraSplitManager.class);

    private final CassandraSession cassandraSession;
    private final int partitionSizeForBatchSelect;
    private final CassandraTokenSplitManager tokenSplitMgr;
    private final CassandraPartitionManager partitionManager;

    @Inject
    public CassandraSplitManager(
            CassandraClientConfig cassandraClientConfig,
            CassandraSession cassandraSession,
            CassandraTokenSplitManager tokenSplitMgr,
            CassandraPartitionManager partitionManager)
    {
        this.cassandraSession = requireNonNull(cassandraSession, "cassandraSession is null");
        this.partitionSizeForBatchSelect = cassandraClientConfig.getPartitionSizeForBatchSelect();
        this.tokenSplitMgr = tokenSplitMgr;
        this.partitionManager = requireNonNull(partitionManager, "partitionManager is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            SplitSchedulingStrategy splitSchedulingStrategy,
            DynamicFilter dynamicFilter)
    {
        CassandraTableHandle cassandraTableHandle = (CassandraTableHandle) tableHandle;

        List<CassandraPartition> partitions;
        String clusteringKeyPredicates;
        if (cassandraTableHandle.getPartitions().isPresent()) {
            partitions = cassandraTableHandle.getPartitions().get();
            clusteringKeyPredicates = cassandraTableHandle.getClusteringKeyPredicates();
        }
        else {
            CassandraPartitionResult partitionResult = partitionManager.getPartitions(cassandraTableHandle, TupleDomain.all());
            partitions = partitionResult.getPartitions();
            clusteringKeyPredicates = extractClusteringKeyPredicates(partitionResult, cassandraTableHandle, cassandraSession);
        }

        if (partitions.isEmpty()) {
            log.debug("No partitions matched predicates for table %s", tableHandle);
            return new FixedSplitSource(ImmutableList.of());
        }

        // if this is an unpartitioned table, split into equal ranges
        if (partitions.size() == 1) {
            CassandraPartition cassandraPartition = partitions.get(0);
            if (cassandraPartition.isUnpartitioned() || cassandraPartition.isIndexedColumnPredicatePushdown()) {
                CassandraTable table = cassandraSession.getTable(cassandraTableHandle.getSchemaTableName());
                List<ConnectorSplit> splits = getSplitsByTokenRange(table, cassandraPartition.getPartitionId(), getSplitsPerNode(session));
                log.debug("One partition matched predicates for table %s, creating %s splits by token ranges", tableHandle, splits.size());
                return new FixedSplitSource(splits);
            }
        }

        List<ConnectorSplit> splits = getSplitsForPartitions(cassandraTableHandle, partitions, clusteringKeyPredicates);
        log.debug("%s partitions matched predicates for table %s, creating %s splits", partitions.size(), tableHandle, splits.size());
        return new FixedSplitSource(splits);
    }

    private static String extractClusteringKeyPredicates(CassandraPartitionResult partitionResult, CassandraTableHandle tableHandle, CassandraSession session)
    {
        if (partitionResult.isUnpartitioned()) {
            return "";
        }

        CassandraClusteringPredicatesExtractor clusteringPredicatesExtractor = new CassandraClusteringPredicatesExtractor(
                session.getTable(tableHandle.getSchemaTableName()).getClusteringKeyColumns(),
                partitionResult.getUnenforcedConstraint(),
                session.getCassandraVersion());
        return clusteringPredicatesExtractor.getClusteringKeyPredicates();
    }

    private List<ConnectorSplit> getSplitsByTokenRange(CassandraTable table, String partitionId, Optional<Long> sessionSplitsPerNode)
    {
        String schema = table.getTableHandle().getSchemaName();
        String tableName = table.getTableHandle().getTableName();
        String tokenExpression = table.getTokenExpression();

        ImmutableList.Builder<ConnectorSplit> builder = ImmutableList.builder();
        List<CassandraTokenSplitManager.TokenSplit> tokenSplits = tokenSplitMgr.getSplits(schema, tableName, sessionSplitsPerNode);
        for (CassandraTokenSplitManager.TokenSplit tokenSplit : tokenSplits) {
            String condition = buildTokenCondition(tokenExpression, tokenSplit.getStartToken(), tokenSplit.getEndToken());
            List<HostAddress> addresses = new HostAddressFactory().hostAddressNamesToHostAddressList(tokenSplit.getHosts());
            CassandraSplit split = new CassandraSplit(partitionId, condition, addresses);
            builder.add(split);
        }

        return builder.build();
    }

    private static String buildTokenCondition(String tokenExpression, String startToken, String endToken)
    {
        return tokenExpression + " > " + startToken + " AND " + tokenExpression + " <= " + endToken;
    }

    private List<ConnectorSplit> getSplitsForPartitions(CassandraTableHandle cassTableHandle, List<CassandraPartition> partitions, String clusteringPredicates)
    {
        String schema = cassTableHandle.getSchemaName();
        HostAddressFactory hostAddressFactory = new HostAddressFactory();
        ImmutableList.Builder<ConnectorSplit> builder = ImmutableList.builder();

        // For single partition key column table, we can merge multiple partitions into a single split
        // by using IN CLAUSE in a single select query if the partitions have the same host list.
        // For multiple partition key columns table, we can't merge them into a single select query, so
        // keep them in a separate split.
        boolean singlePartitionKeyColumn = true;
        String partitionKeyColumnName = null;
        if (!partitions.isEmpty()) {
            singlePartitionKeyColumn = partitions.get(0).getTupleDomain().getDomains().get().size() == 1;
            if (singlePartitionKeyColumn) {
                String partitionId = partitions.get(0).getPartitionId();
                partitionKeyColumnName = partitionId.substring(0, partitionId.lastIndexOf('=') - 1);
            }
        }
        Map<Set<String>, Set<String>> hostsToPartitionKeys = new HashMap<>();
        Map<Set<String>, List<HostAddress>> hostMap = new HashMap<>();

        for (CassandraPartition cassandraPartition : partitions) {
            Set<Host> hosts = cassandraSession.getReplicas(schema, cassandraPartition.getKeyAsByteBuffer());
            List<HostAddress> addresses = hostAddressFactory.toHostAddressList(hosts);
            if (singlePartitionKeyColumn) {
                // host ip addresses
                ImmutableSet.Builder<String> sb = ImmutableSet.builder();
                for (HostAddress address : addresses) {
                    sb.add(address.getHostText());
                }
                Set<String> hostAddresses = sb.build();
                // partition key values
                Set<String> values = hostsToPartitionKeys.get(hostAddresses);
                if (values == null) {
                    values = new HashSet<>();
                }
                String partitionId = cassandraPartition.getPartitionId();
                values.add(partitionId.substring(partitionId.lastIndexOf('=') + 2));
                hostsToPartitionKeys.put(hostAddresses, values);
                hostMap.put(hostAddresses, addresses);
            }
            else {
                builder.add(createSplitForClusteringPredicates(cassandraPartition.getPartitionId(), addresses, clusteringPredicates));
            }
        }
        if (singlePartitionKeyColumn) {
            for (Map.Entry<Set<String>, Set<String>> entry : hostsToPartitionKeys.entrySet()) {
                StringBuilder sb = new StringBuilder(partitionSizeForBatchSelect);
                int size = 0;
                for (String value : entry.getValue()) {
                    if (size > 0) {
                        sb.append(",");
                    }
                    sb.append(value);
                    size++;
                    if (size > partitionSizeForBatchSelect) {
                        String partitionId = format("%s in (%s)", partitionKeyColumnName, sb.toString());
                        builder.add(createSplitForClusteringPredicates(partitionId, hostMap.get(entry.getKey()), clusteringPredicates));
                        size = 0;
                        sb.setLength(0);
                        sb.trimToSize();
                    }
                }
                if (size > 0) {
                    String partitionId = format("%s in (%s)", partitionKeyColumnName, sb.toString());
                    builder.add(createSplitForClusteringPredicates(partitionId, hostMap.get(entry.getKey()), clusteringPredicates));
                }
            }
        }
        return builder.build();
    }

    private CassandraSplit createSplitForClusteringPredicates(
            String partitionId,
            List<HostAddress> hosts,
            String clusteringPredicates)
    {
        if (clusteringPredicates.isEmpty()) {
            return new CassandraSplit(partitionId, null, hosts);
        }

        return new CassandraSplit(partitionId, clusteringPredicates, hosts);
    }
}
