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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableSet;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitSource;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.plugin.hive.HiveTransactionHandle;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static com.google.common.collect.Maps.transformValues;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.plugin.iceberg.ColumnIdentity.primitiveColumnIdentity;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.plugin.iceberg.IcebergTestUtils.loadTable;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.tpch.TpchTable.REGION;
import static org.apache.iceberg.TableUtil.formatVersion;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

final class TestIcebergSplitManager
        extends AbstractTestQueryFramework
{
    private HiveMetastore metastore;
    private TrinoFileSystemFactory fileSystemFactory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setInitialTables(REGION)
                .build();
    }

    @BeforeAll
    void setUp()
    {
        metastore = getHiveMetastore(getQueryRunner());
        fileSystemFactory = getFileSystemFactory(getQueryRunner());
    }

    @Test
    void testGetScan()
    {
        IcebergConnector connector = (IcebergConnector) getQueryRunner().getCoordinator().getConnector(ICEBERG_CATALOG);
        IcebergTransactionManager transactionManager = connector.getInjector().getInstance(IcebergTransactionManager.class);
        HiveTransactionHandle transactionHandle = new HiveTransactionHandle(true);
        transactionManager.begin(transactionHandle);
        ConnectorSplitManager splitManager = connector.getInjector().getInstance(ConnectorSplitManager.class);

        // Read only regionkey column
        ClassLoaderSafeConnectorSplitSource splitSource = (ClassLoaderSafeConnectorSplitSource) splitManager.getSplits(
                transactionHandle,
                IcebergTestUtils.SESSION,
                tableHandle(new SchemaTableName("tpch", "region"), "regionkey"),
                DynamicFilter.EMPTY,
                Constraint.alwaysTrue());
        getFutureValue(splitSource.getNextBatch(1000));

        assertThat(splitSource.getMetrics().getMetrics())
                .contains(entry("projectedFields", new LongCount(3)));
    }

    private IcebergTableHandle tableHandle(SchemaTableName schemaTableName, String projectedColumn)
    {
        BaseTable table = loadTable(schemaTableName.getTableName(), metastore, fileSystemFactory, "iceberg", schemaTableName.getSchemaName());
        return new IcebergTableHandle(
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                TableType.DATA,
                OptionalLong.of(table.currentSnapshot().snapshotId()),
                SchemaParser.toJson(table.schema()),
                OptionalInt.of(table.spec().specId()),
                transformValues(table.specs(), PartitionSpecParser::toJson),
                formatVersion(table),
                TupleDomain.all(),
                TupleDomain.all(),
                OptionalLong.empty(),
                ImmutableSet.of(IcebergColumnHandle.optional(primitiveColumnIdentity(1, projectedColumn)).columnType(INTEGER).build()),
                Optional.empty(),
                table.location(),
                table.properties(),
                Optional.empty(),
                false,
                Optional.empty(),
                ImmutableSet.of(),
                Optional.of(false));
    }
}
