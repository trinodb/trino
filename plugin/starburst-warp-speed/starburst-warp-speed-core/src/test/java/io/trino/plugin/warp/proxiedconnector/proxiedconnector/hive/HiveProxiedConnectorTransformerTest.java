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
package io.trino.plugin.warp.proxiedconnector.proxiedconnector.hive;

import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartition;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.HivePartitioningHandle;
import io.trino.plugin.hive.HiveSplit;
import io.trino.plugin.hive.HiveTableHandle;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.varada.configuration.ProxiedConnectorConfiguration;
import io.trino.plugin.varada.dispatcher.DispatcherSplit;
import io.trino.plugin.varada.dispatcher.DispatcherTableHandle;
import io.trino.plugin.varada.dispatcher.PartitionKey;
import io.trino.plugin.varada.dispatcher.SimplifiedColumns;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.proxiedconnector.hive.HiveProxiedConnectorTransformer;
import io.trino.plugin.warp.proxiedconnector.proxiedconnector.ProxyConnectorTransformerBaseTest;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorBucketNodeMap;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.IntegerType;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveType.HIVE_INT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HiveProxiedConnectorTransformerTest
        extends ProxyConnectorTransformerBaseTest
{
    private final HiveProxiedConnectorTransformer hiveProxiedConnectorTransformer =
            new HiveProxiedConnectorTransformer(new ProxiedConnectorConfiguration());

    @Test
    public void testCalculateColumnsStatisticsBucketPriority()
    {
        super.testCalculateColumnsStatisticsBucketPriority(
                hiveProxiedConnectorTransformer,
                Stream.of(1D, 2D, 3D)
                        .collect(Collectors.toMap(doubleVal ->
                                        new HiveColumnHandle("baseColumnName" + doubleVal,
                                                1,
                                                HiveType.HIVE_INT,
                                                IntegerType.INTEGER,
                                                Optional.empty(),
                                                HiveColumnHandle.ColumnType.REGULAR,
                                                Optional.empty()),
                                Function.identity())),
                columnHandle -> ((HiveColumnHandle) columnHandle).getName());
    }

    @Test
    public void testCreateDispatcherSplit()
    {
        HiveSplit hiveSplit = new HiveSplit(
                "partitionName",
                "path",
                1L,
                2L,
                3L,
                4L,
                new HashMap<>(),
                List.of(new HivePartitionKey("name1", "key1"),
                        new HivePartitionKey("name2", "key2")),
                List.of(HostAddress.fromString("http://host:8080")),
                OptionalInt.empty(),
                OptionalInt.empty(),
                true,
                Map.of(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                SplitWeight.fromProportion(1L));

        DispatcherTableHandle dispatcherTableHandle = new DispatcherTableHandle(
                "schemaName",
                "tableName",
                OptionalLong.of(1),
                TupleDomain.all(),
                new SimplifiedColumns(Set.of(new RegularColumn("col1"))),
                mock(HiveTableHandle.class),
                Optional.empty(),
                Collections.emptyList(),
                false);
        List<PartitionKey> partitionKeys = hiveSplit.getPartitionKeys()
                .stream()
                .map(hivePartitionKey -> new PartitionKey(new RegularColumn(hivePartitionKey.getName()), hivePartitionKey.getValue()))
                .toList();
        DispatcherSplit expectedDispatcherSplit = new DispatcherSplit(
                dispatcherTableHandle.getSchemaName(),
                dispatcherTableHandle.getTableName(),
                hiveSplit.getPath(),
                hiveSplit.getStart(),
                hiveSplit.getLength(),
                hiveSplit.getFileModifiedTime(),
                List.of(node.getHostAndPort()),
                partitionKeys,
                "",
                hiveSplit);

        super.testCreateDispatcherSplit(
                hiveProxiedConnectorTransformer,
                hiveSplit,
                dispatcherTableHandle,
                expectedDispatcherSplit);
    }

    @Test
    public void testCreateProxyTableHandleForWarming()
    {
        HiveTableHandle hiveTableHandle = new HiveTableHandle(
                "schema",
                "table",
                Optional.of(Map.of("1", "2")),
                List.of(mock(HiveColumnHandle.class)),
                List.of(mock(HiveColumnHandle.class)),
                Optional.empty(),
                Optional.of(List.of(new HivePartition(new SchemaTableName("1", "2")))),
                TupleDomain.all(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Collections.emptySet(),
                Set.of(mock(HiveColumnHandle.class)),
                mock(AcidTransaction.class),
                true,
                Optional.of(1L));

        DispatcherTableHandle dispatcherTableHandle = new DispatcherTableHandle(
                "schemaName",
                "tableName",
                OptionalLong.of(1),
                TupleDomain.all(),
                new SimplifiedColumns(Set.of(new RegularColumn("col1"))),
                hiveTableHandle,
                Optional.empty(),
                Collections.emptyList(),
                false);

        HiveTableHandle expectedTableHandleForWarming = new HiveTableHandle(
                hiveTableHandle.getSchemaName(),
                hiveTableHandle.getTableName(),
                hiveTableHandle.getTableParameters(),
                hiveTableHandle.getPartitionColumns(),
                hiveTableHandle.getDataColumns(),
                hiveTableHandle.getPartitionNames(),
                hiveTableHandle.getPartitions(),
                TupleDomain.all(),
                TupleDomain.all(),
                hiveTableHandle.getBucketHandle(),
                hiveTableHandle.getBucketFilter(),
                hiveTableHandle.getAnalyzePartitionValues(),
                Collections.emptySet(),
                hiveTableHandle.getProjectedColumns(),
                hiveTableHandle.getTransaction(),
                hiveTableHandle.isRecordScannedFiles(),
                Optional.empty());

        super.testCreateProxyTableHandleForWarming(
                hiveProxiedConnectorTransformer,
                dispatcherTableHandle,
                expectedTableHandleForWarming);
    }

    @Test
    public void testCreateProxiedConnectorTableHandleForMixedQuery()
    {
        HiveTableHandle hiveTableHandle = new HiveTableHandle(
                "schema",
                "table",
                Optional.of(Map.of("1", "2")),
                List.of(mock(HiveColumnHandle.class)),
                List.of(mock(HiveColumnHandle.class)),
                Optional.empty(),
                Optional.of(List.of(new HivePartition(new SchemaTableName("1", "2")))),
                TupleDomain.withColumnDomains(Map.of(
                        new HiveColumnHandle("col", 0, HIVE_INT, INTEGER, Optional.empty(), REGULAR, Optional.empty()),
                        Domain.singleValue(INTEGER, 1L))),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Collections.emptySet(),
                Set.of(mock(HiveColumnHandle.class)),
                mock(AcidTransaction.class),
                true,
                Optional.of(1L));

        DispatcherTableHandle dispatcherTableHandle = new DispatcherTableHandle(
                "schemaName",
                "tableName",
                OptionalLong.of(1),
                TupleDomain.all(),
                new SimplifiedColumns(Set.of(new RegularColumn("col1"))),
                hiveTableHandle,
                Optional.empty(),
                Collections.emptyList(),
                false);

        HiveTableHandle expectedTableHandleMixedQuery = new HiveTableHandle(
                hiveTableHandle.getSchemaName(),
                hiveTableHandle.getTableName(),
                hiveTableHandle.getTableParameters(),
                hiveTableHandle.getPartitionColumns(),
                hiveTableHandle.getDataColumns(),
                hiveTableHandle.getPartitionNames(),
                hiveTableHandle.getPartitions(),
                hiveTableHandle.getCompactEffectivePredicate(),
                TupleDomain.all(),
                hiveTableHandle.getBucketHandle(),
                hiveTableHandle.getBucketFilter(),
                hiveTableHandle.getAnalyzePartitionValues(),
                Collections.emptySet(),
                hiveTableHandle.getProjectedColumns(),
                hiveTableHandle.getTransaction(),
                hiveTableHandle.isRecordScannedFiles(),
                Optional.empty());

        super.testCreateProxiedConnectorTableHandleForMixedQuery(
                hiveProxiedConnectorTransformer,
                dispatcherTableHandle,
                expectedTableHandleMixedQuery);
    }

    @Test
    void testGetBucketNodeMapping()
    {
        ConnectorTransactionHandle transactionHandle = mock(ConnectorTransactionHandle.class);
        ConnectorSession session = mock(ConnectorSession.class);
        HivePartitioningHandle partitioningHandle = mock(HivePartitioningHandle.class);
        ConnectorNodePartitioningProvider nodePartitionProvider = mock(ConnectorNodePartitioningProvider.class);
        List<Node> mockNodes = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            Node node = mock(Node.class);
            when(node.getHostAndPort()).thenReturn(HostAddress.fromUri(URI.create("http://node" + i)));
            mockNodes.add(node);
        }

        List<Node> nodes = mockNodes.subList(0, 2);
        int bucketCount = 5;

        when(partitioningHandle.getBucketCount()).thenReturn(bucketCount);

        Optional<ConnectorBucketNodeMap> optional = hiveProxiedConnectorTransformer.getBucketNodeMapping(
                transactionHandle,
                session,
                partitioningHandle,
                nodePartitionProvider,
                nodes);

        assertTrue(optional.isPresent());
        assertTrue(optional.orElseThrow().hasFixedMapping());
        assertEquals(bucketCount, optional.orElseThrow().getBucketCount());
        assertNotNull(optional.orElseThrow().getFixedMapping());
        assertEquals(bucketCount, optional.orElseThrow().getFixedMapping().size());

        nodes = mockNodes;

        optional = hiveProxiedConnectorTransformer.getBucketNodeMapping(
                transactionHandle,
                session,
                partitioningHandle,
                nodePartitionProvider,
                nodes);

        assertTrue(optional.isPresent());
        assertTrue(optional.orElseThrow().hasFixedMapping());
        assertEquals(bucketCount, optional.orElseThrow().getBucketCount());
        assertNotNull(optional.orElseThrow().getFixedMapping());
        assertEquals(bucketCount, optional.orElseThrow().getFixedMapping().size());
    }

    @Override
    protected void assertTablesAreEqual(ConnectorTableHandle expected, ConnectorTableHandle actual)
    {
        super.assertTablesAreEqual(expected, actual);

        HiveTableHandle hiveActual = (HiveTableHandle) actual;

        // not part of equals() and should always be overwritten to empty
        assertThat(hiveActual.getMaxScannedFileSize()).isEmpty();
    }
}
