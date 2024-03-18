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
package io.trino.plugin.warp.proxiedconnector.proxiedconnector.hudi;

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.HivePartitioningHandle;
import io.trino.plugin.hive.HiveTableHandle;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hudi.HudiSplit;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.hudi.model.HudiTableType;
import io.trino.plugin.varada.configuration.ProxiedConnectorConfiguration;
import io.trino.plugin.varada.dispatcher.DispatcherSplit;
import io.trino.plugin.varada.dispatcher.DispatcherTableHandle;
import io.trino.plugin.varada.dispatcher.PartitionKey;
import io.trino.plugin.varada.dispatcher.SimplifiedColumns;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.proxiedconnector.hudi.HudiProxiedConnectorTransformer;
import io.trino.plugin.warp.proxiedconnector.proxiedconnector.ProxyConnectorTransformerBaseTest;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorBucketNodeMap;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HudiProxiedConnectorTransformerTest
        extends ProxyConnectorTransformerBaseTest
{
    private final HudiProxiedConnectorTransformer hudiProxiedConnectorTransformer =
            new HudiProxiedConnectorTransformer(new ProxiedConnectorConfiguration());

    @Test
    public void testCalculateColumnsStatisticsBucketPriority()
    {
        super.testCalculateColumnsStatisticsBucketPriority(
                hudiProxiedConnectorTransformer,
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
        HudiSplit hudiSplit = new HudiSplit(
                "path",
                0,
                5L,
                5L,
                3L,
                TupleDomain.all(),
                List.of(new HivePartitionKey("name1", "key1"),
                        new HivePartitionKey("name2", "key2")),
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
        List<PartitionKey> partitionKeys = hudiSplit.getPartitionKeys()
                .stream()
                .map(hivePartitionKey -> new PartitionKey(new RegularColumn(hivePartitionKey.getName()), hivePartitionKey.getValue()))
                .toList();
        DispatcherSplit expectedDispatcherSplit = new DispatcherSplit(
                dispatcherTableHandle.getSchemaName(),
                dispatcherTableHandle.getTableName(),
                hudiSplit.getLocation(),
                hudiSplit.getStart(),
                hudiSplit.getLength(),
                hudiSplit.getFileModifiedTime(),
                List.of(node.getHostAndPort()),
                partitionKeys,
                "",
                hudiSplit);

        super.testCreateDispatcherSplit(
                hudiProxiedConnectorTransformer,
                hudiSplit,
                dispatcherTableHandle,
                expectedDispatcherSplit);
    }

    @Test
    public void testCreateProxyTableHandleForWarming()
    {
        Domain domain = Domain.singleValue(IntegerType.INTEGER, 1L);
        HiveColumnHandle hiveColumnHandle = new HiveColumnHandle("col1", 0, HiveColumnHandle.PARTITION_HIVE_TYPE, VarcharType.VARCHAR, Optional.empty(), HiveColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain<HiveColumnHandle> partitionPredicates = TupleDomain.withColumnDomains(ImmutableMap.of(hiveColumnHandle, domain));
        HudiTableHandle hudiTableHandle = new HudiTableHandle(
                "schema",
                "table",
                "path",
                HudiTableType.COPY_ON_WRITE,
                List.of(hiveColumnHandle),
                partitionPredicates,
                partitionPredicates);

        DispatcherTableHandle dispatcherTableHandle = new DispatcherTableHandle(
                "schema",
                "table",
                OptionalLong.of(1),
                TupleDomain.all(),
                new SimplifiedColumns(Set.of(new RegularColumn("col1"))),
                hudiTableHandle,
                Optional.empty(),
                Collections.emptyList(),
                false);

        HudiTableHandle expectedTableHandleForWarming = new HudiTableHandle(
                hudiTableHandle.getSchemaName(),
                hudiTableHandle.getTableName(),
                hudiTableHandle.getBasePath(),
                hudiTableHandle.getTableType(),
                List.of(hiveColumnHandle),
                TupleDomain.all(),
                TupleDomain.all());

        super.testCreateProxyTableHandleForWarming(
                hudiProxiedConnectorTransformer,
                dispatcherTableHandle,
                expectedTableHandleForWarming);
    }

    @Test
    public void testCreateProxiedConnectorTableHandleForMixedQuery()
    {
        Domain varcharDomain = Domain.singleValue(VarcharType.VARCHAR, Slices.utf8Slice("bla"));
        Domain integerDomain = Domain.singleValue(IntegerType.INTEGER, 1L);
        HiveColumnHandle hiveColumnHandle = new HiveColumnHandle("col1", 0, HiveColumnHandle.PARTITION_HIVE_TYPE, VarcharType.VARCHAR, Optional.empty(), HiveColumnHandle.ColumnType.REGULAR, Optional.empty());
        HiveColumnHandle regularPredicatesColumn = new HiveColumnHandle("col2", 0, HiveType.HIVE_INT, IntegerType.INTEGER, Optional.empty(), HiveColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain<HiveColumnHandle> partitionPredicates = TupleDomain.withColumnDomains(ImmutableMap.of(hiveColumnHandle, varcharDomain));
        TupleDomain<HiveColumnHandle> regularPredicates = TupleDomain.withColumnDomains(ImmutableMap.of(regularPredicatesColumn, integerDomain));
        HudiTableHandle hudiTableHandle = new HudiTableHandle(
                "schema",
                "table",
                "path",
                HudiTableType.COPY_ON_WRITE,
                List.of(hiveColumnHandle),
                partitionPredicates,
                regularPredicates);

        DispatcherTableHandle dispatcherTableHandle = new DispatcherTableHandle(
                "schema",
                "table",
                OptionalLong.of(1),
                TupleDomain.all(),
                new SimplifiedColumns(Set.of(new RegularColumn("col1"))),
                hudiTableHandle,
                Optional.empty(),
                Collections.emptyList(),
                false);

        HudiTableHandle expectedTableHandleMixedQuery = new HudiTableHandle(
                hudiTableHandle.getSchemaName(),
                hudiTableHandle.getTableName(),
                hudiTableHandle.getBasePath(),
                hudiTableHandle.getTableType(),
                List.of(hiveColumnHandle),
                TupleDomain.all(),
                hudiTableHandle.getRegularPredicates());

        super.testCreateProxiedConnectorTableHandleForMixedQuery(
                hudiProxiedConnectorTransformer,
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

        Optional<ConnectorBucketNodeMap> optional = hudiProxiedConnectorTransformer.getBucketNodeMapping(
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

        optional = hudiProxiedConnectorTransformer.getBucketNodeMapping(
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
        // Hudi doesn't implement equals()
        HudiTableHandle hudiExpected = (HudiTableHandle) expected;
        HudiTableHandle hudiActual = (HudiTableHandle) actual;
        assertThat(hudiActual.getBasePath()).isEqualTo(hudiExpected.getBasePath());
        assertThat(hudiActual.getTableName()).isEqualTo(hudiExpected.getTableName());
        assertThat(hudiActual.getSchemaName()).isEqualTo(hudiExpected.getSchemaName());
        assertThat(hudiActual.getTableType()).isEqualTo(hudiExpected.getTableType());
        assertThat(hudiActual.getRegularPredicates()).isEqualTo(hudiExpected.getRegularPredicates());
        assertThat(hudiActual.getPartitionPredicates()).isEqualTo(TupleDomain.all());
    }
}
