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
package io.trino.plugin.warp.proxiedconnector.proxiedconnector.iceberg;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.plugin.iceberg.ColumnIdentity;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.plugin.iceberg.TableType;
import io.trino.plugin.varada.configuration.ProxiedConnectorConfiguration;
import io.trino.plugin.varada.dispatcher.DispatcherTableHandle;
import io.trino.plugin.varada.dispatcher.SimplifiedColumns;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.proxiedconnector.iceberg.IcebergProxiedConnectorTransformer;
import io.trino.plugin.warp.proxiedconnector.proxiedconnector.ProxyConnectorTransformerBaseTest;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.trino.spi.type.IntegerType.INTEGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IcebergProxiedConnectorTransformerTest
        extends ProxyConnectorTransformerBaseTest
{
    private final IcebergProxiedConnectorTransformer icebergProxiedConnectorTransformer =
            new IcebergProxiedConnectorTransformer(new ProxiedConnectorConfiguration());

    @Test
    public void testCalculateColumnsStatisticsBucketPriority()
    {
        super.testCalculateColumnsStatisticsBucketPriority(
                icebergProxiedConnectorTransformer,
                Stream.of(1D, 2D, 3D)
                        .collect(Collectors.toMap(doubleVal ->
                        {
                            IcebergColumnHandle columnHandle = mock(IcebergColumnHandle.class);
                            when(columnHandle.getName()).thenReturn("name-" + doubleVal);
                            return columnHandle;
                        }, Function.identity())),
                columnHandle -> ((IcebergColumnHandle) columnHandle).getName());
    }

    @Test
    @Disabled // too complex to mock this flow due to getPartitionKeysMap
    public void testCreateDispatcherSplit()
    {
//        IcebergSplit icebergSplit = mock(IcebergSplit.class);
//        when(icebergSplit.getPath()).thenReturn("path");
//        when(icebergSplit.getStart()).thenReturn(1L);
//        when(icebergSplit.getLength()).thenReturn(2L);
//        when(icebergSplit.getPartitionSpecJson()).thenReturn("{}");
//
//        DeleteFile deleteFile = new DeleteFile(
//                FileContent.DATA,
//                "path1111",
//                FileFormat.ORC,
//                1L,
//                2L,
//                List.of(),
//                Map.of(),
//                Map.of());
//        when(icebergSplit.getDeletes()).thenReturn(List.of(deleteFile));
//
//        IcebergTableHandle icebergTableHandle = mock(IcebergTableHandle.class);
//        when(icebergTableHandle.getTableSchemaJson()).thenReturn("{}");
//
//        DispatcherTableHandle dispatcherTableHandle = new DispatcherTableHandle(
//                "schemaName",
//                "tableName",
//                1L,
//                TupleDomain.all(),
//                Map.of(),
//                "JSON",
//                Set.of("col1"),
//                Map.of("expr1", mock(VaradaExpressionData.class)),
//                icebergTableHandle);
//
//        DispatcherSplit expectedDispatcherSplit = new DispatcherSplit(dispatcherTableHandle.getSchemaName(),
//                dispatcherTableHandle.getTableName(),
//                icebergSplit.getPath(),
//                icebergSplit.getStart(),
//                icebergSplit.getLength(),
//                0,
//                List.of(node.getHostAndPort()),
//                Map.of(),
//                "deleteFile",
//                false,
//                icebergSplit);
//
//        super.testCreateDispatcherSplit(
//                new IcebergProxyConnectorTransformer(),
//                icebergSplit,
//                dispatcherTableHandle,
//                expectedDispatcherSplit);
    }

    @Test
    public void testCreateProxyTableHandleForWarming()
    {
        IcebergTableHandle icebergTableHandle = new IcebergTableHandle(
                CatalogHandle.createRootCatalogHandle(new CatalogName("warp"), new CatalogHandle.CatalogVersion("422")),
                "schema",
                "table",
                TableType.DATA,
                Optional.of(1L),
                "tableSchemaJson",
                Optional.of("partitionSpecJson"),
                1,
                TupleDomain.all(),
                TupleDomain.all(),
                OptionalLong.empty(),
                Set.of(mock(IcebergColumnHandle.class)),
                Optional.of("nameMappingJson"),
                "tableLocation",
                Map.of(),
                false,
                Optional.of(DataSize.of(1, DataSize.Unit.BYTE)),
                Collections.emptySet(),
                Optional.empty());

        DispatcherTableHandle dispatcherTableHandle = new DispatcherTableHandle(
                "schemaName",
                "tableName",
                OptionalLong.of(1),
                TupleDomain.all(),
                new SimplifiedColumns(Set.of(new RegularColumn("col1"))),
                icebergTableHandle,
                Optional.empty(),
                Collections.emptyList(),
                false);

        IcebergTableHandle expectedTableHandleForWarming = new IcebergTableHandle(
                icebergTableHandle.getCatalog(),
                icebergTableHandle.getSchemaName(),
                icebergTableHandle.getTableName(),
                icebergTableHandle.getTableType(),
                icebergTableHandle.getSnapshotId(),
                icebergTableHandle.getTableSchemaJson(),
                icebergTableHandle.getPartitionSpecJson(),
                icebergTableHandle.getFormatVersion(),
                TupleDomain.all(),
                TupleDomain.all(),
                OptionalLong.empty(),
                icebergTableHandle.getProjectedColumns(),
                icebergTableHandle.getNameMappingJson(),
                icebergTableHandle.getTableLocation(),
                icebergTableHandle.getStorageProperties(),
                icebergTableHandle.isRecordScannedFiles(),
                Optional.empty(),
                icebergTableHandle.getConstraintColumns(),
                icebergTableHandle.getForAnalyze());

        super.testCreateProxyTableHandleForWarming(
                icebergProxiedConnectorTransformer,
                dispatcherTableHandle,
                expectedTableHandleForWarming);
    }

    @Test
    public void testCreateProxiedConnectorTableHandleForMixedQuery()
    {
        IcebergTableHandle icebergTableHandle = new IcebergTableHandle(
                CatalogHandle.createRootCatalogHandle(new CatalogName("warp"), new CatalogHandle.CatalogVersion("422")),
                "schema",
                "table",
                TableType.DATA,
                Optional.of(1L),
                "tableSchemaJson",
                Optional.of("partitionSpecJson"),
                1,
                TupleDomain.withColumnDomains(Map.of(
                        new IcebergColumnHandle(
                                new ColumnIdentity(1, "name", ColumnIdentity.TypeCategory.PRIMITIVE, ImmutableList.of()),
                                INTEGER,
                                ImmutableList.of(),
                                INTEGER,
                                false,
                                Optional.empty()),
                        Domain.singleValue(INTEGER, 1L))),
                TupleDomain.all(),
                OptionalLong.empty(),
                Set.of(mock(IcebergColumnHandle.class)),
                Optional.of("nameMappingJson"),
                "tableLocation",
                Map.of(),
                true,
                Optional.of(DataSize.of(1, DataSize.Unit.BYTE)),
                Collections.emptySet(),
                Optional.empty());

        DispatcherTableHandle dispatcherTableHandle = new DispatcherTableHandle(
                "schemaName",
                "tableName",
                OptionalLong.of(1),
                TupleDomain.all(),
                new SimplifiedColumns(Set.of(new RegularColumn("col1"))),
                icebergTableHandle,
                Optional.empty(),
                Collections.emptyList(),
                false);

        IcebergTableHandle expectedTableHandleMixedQuery = new IcebergTableHandle(
                icebergTableHandle.getCatalog(),
                icebergTableHandle.getSchemaName(),
                icebergTableHandle.getTableName(),
                icebergTableHandle.getTableType(),
                icebergTableHandle.getSnapshotId(),
                icebergTableHandle.getTableSchemaJson(),
                icebergTableHandle.getPartitionSpecJson(),
                icebergTableHandle.getFormatVersion(),
                icebergTableHandle.getUnenforcedPredicate(),
                TupleDomain.all(),
                icebergTableHandle.getLimit(),
                icebergTableHandle.getProjectedColumns(),
                icebergTableHandle.getNameMappingJson(),
                icebergTableHandle.getTableLocation(),
                icebergTableHandle.getStorageProperties(),
                icebergTableHandle.isRecordScannedFiles(),
                Optional.empty(),
                icebergTableHandle.getConstraintColumns(),
                icebergTableHandle.getForAnalyze());

        super.testCreateProxiedConnectorTableHandleForMixedQuery(
                icebergProxiedConnectorTransformer,
                dispatcherTableHandle,
                expectedTableHandleMixedQuery);
    }

    @Override
    protected void assertTablesAreEqual(ConnectorTableHandle expected, ConnectorTableHandle actual)
    {
        super.assertTablesAreEqual(expected, actual);

        IcebergTableHandle icebergActual = (IcebergTableHandle) actual;

        // not part of equals() and should always be overwritten to empty
        assertThat(icebergActual.getMaxScannedFileSize()).isEmpty();
    }
}
