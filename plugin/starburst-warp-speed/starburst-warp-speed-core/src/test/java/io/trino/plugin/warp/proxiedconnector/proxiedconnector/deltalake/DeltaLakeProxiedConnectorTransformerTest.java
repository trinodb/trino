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
package io.trino.plugin.warp.proxiedconnector.proxiedconnector.deltalake;

import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.plugin.deltalake.DeltaLakeSplit;
import io.trino.plugin.deltalake.DeltaLakeTableHandle;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.varada.configuration.ProxiedConnectorConfiguration;
import io.trino.plugin.varada.dispatcher.DispatcherSplit;
import io.trino.plugin.varada.dispatcher.DispatcherTableHandle;
import io.trino.plugin.varada.dispatcher.PartitionKey;
import io.trino.plugin.varada.dispatcher.SimplifiedColumns;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.proxiedconnector.deltalake.DeltaLakeProxiedConnectorTransformer;
import io.trino.plugin.warp.proxiedconnector.proxiedconnector.ProxyConnectorTransformerBaseTest;
import io.trino.spi.SplitWeight;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.spi.type.IntegerType.INTEGER;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DeltaLakeProxiedConnectorTransformerTest
        extends ProxyConnectorTransformerBaseTest
{
    private final DeltaLakeProxiedConnectorTransformer deltaLakeProxiedConnectorTransformer =
            new DeltaLakeProxiedConnectorTransformer(new ProxiedConnectorConfiguration());

    @Test
    public void testCalculateColumnsStatisticsBucketPriority()
    {
        super.testCalculateColumnsStatisticsBucketPriority(
                deltaLakeProxiedConnectorTransformer,
                Stream.of(1D, 2D, 3D)
                        .collect(Collectors.toMap(doubleVal ->
                        {
                            DeltaLakeColumnHandle columnHandle = mock(DeltaLakeColumnHandle.class);
                            when(columnHandle.getColumnName()).thenReturn("name-" + doubleVal);
                            return columnHandle;
                        }, Function.identity())),
                columnHandle -> ((DeltaLakeColumnHandle) columnHandle).getColumnName());
    }

    @Test
    public void testCreateDispatcherSplit()
    {
        DeltaLakeSplit deltaLakeSplit = new DeltaLakeSplit(
                "path",
                1L,
                2L,
                3L,
                Optional.empty(),
                4L,
                Optional.empty(),
                SplitWeight.fromProportion(5L),
                TupleDomain.all(),
                Map.of("part1", Optional.of("part1")));

        DispatcherTableHandle dispatcherTableHandle = new DispatcherTableHandle(
                "schemaName",
                "tableName",
                OptionalLong.of(1),
                TupleDomain.all(),
                new SimplifiedColumns(Set.of(new RegularColumn("col1"))),
                getDeltaLakeTableHandle(),
                Optional.empty(),
                Collections.emptyList(),
                false);

        DispatcherSplit expectedDispatcherSplit = getDispatcherSplit(deltaLakeSplit, dispatcherTableHandle);

        super.testCreateDispatcherSplit(
                deltaLakeProxiedConnectorTransformer,
                deltaLakeSplit,
                dispatcherTableHandle,
                expectedDispatcherSplit);
    }

    private DispatcherSplit getDispatcherSplit(DeltaLakeSplit deltaLakeSplit, DispatcherTableHandle dispatcherTableHandle)
    {
        List<PartitionKey> partitionKeys = new ArrayList<>();
        for (Map.Entry<String, Optional<String>> entry : deltaLakeSplit.getPartitionKeys().entrySet()) {
            if (entry.getValue().isPresent()) {
                partitionKeys.add(new PartitionKey(new RegularColumn(entry.getKey()), entry.getValue().orElseThrow()));
            }
        }
        return new DispatcherSplit(
                dispatcherTableHandle.getSchemaName(),
                dispatcherTableHandle.getTableName(),
                deltaLakeSplit.getPath(),
                deltaLakeSplit.getStart(),
                deltaLakeSplit.getLength(),
                deltaLakeSplit.getFileModifiedTime(),
                List.of(node.getHostAndPort()),
                partitionKeys,
                "",
                deltaLakeSplit);
    }

    @Test
    public void testCreateProxyTableHandleForWarming()
    {
        DeltaLakeTableHandle tableHandle = getDeltaLakeTableHandle();
        DispatcherTableHandle dispatcherTableHandle = new DispatcherTableHandle(
                "schemaName",
                "tableName",
                OptionalLong.of(1),
                TupleDomain.all(),
                new SimplifiedColumns(Set.of(new RegularColumn("col1"))),
                tableHandle,
                Optional.empty(),
                Collections.emptyList(),
                false);
        super.testCreateProxyTableHandleForWarming(
                deltaLakeProxiedConnectorTransformer,
                dispatcherTableHandle,
                new DeltaLakeTableHandle(
                        tableHandle.getSchemaName(),
                        tableHandle.getTableName(),
                        tableHandle.isManaged(),
                        tableHandle.getLocation(),
                        tableHandle.getMetadataEntry(),
                        tableHandle.getProtocolEntry(),
                        TupleDomain.all(),
                        TupleDomain.all(),
                        tableHandle.getWriteType(),
                        tableHandle.getProjectedColumns(),
                        tableHandle.getUpdatedColumns(),
                        tableHandle.getUpdateRowIdColumns(),
                        tableHandle.getAnalyzeHandle(),
                        tableHandle.getReadVersion()));
    }

    @Test
    public void testCreateProxiedConnectorTableHandleForMixedQuery()
    {
        DispatcherTableHandle dispatcherTableHandle = new DispatcherTableHandle(
                "schemaName",
                "tableName",
                OptionalLong.of(1),
                TupleDomain.all(),
                new SimplifiedColumns(Set.of(new RegularColumn("col1"))),
                getDeltaLakeTableHandle(),
                Optional.empty(),
                Collections.emptyList(),
                false);

        super.testCreateProxiedConnectorTableHandleForMixedQuery(
                deltaLakeProxiedConnectorTransformer,
                dispatcherTableHandle,
                dispatcherTableHandle.getProxyConnectorTableHandle());
    }

    private static DeltaLakeTableHandle getDeltaLakeTableHandle()
    {
        return new DeltaLakeTableHandle(
                "schemaName",
                "tableName",
                true,
                "location",
                mock(MetadataEntry.class),
                mock(ProtocolEntry.class),
                TupleDomain.all(),
                TupleDomain.withColumnDomains(Map.of(
                        new DeltaLakeColumnHandle("col1", INTEGER, OptionalInt.empty(), "col1", INTEGER, REGULAR, Optional.empty()),
                        Domain.singleValue(INTEGER, 1L))),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                1L);
    }
}
