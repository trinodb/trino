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
package io.trino.plugin.raptor.legacy.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.metadata.MetadataUtil.TableMetadataBuilder;
import io.trino.plugin.raptor.legacy.NodeSupplier;
import io.trino.plugin.raptor.legacy.RaptorColumnHandle;
import io.trino.plugin.raptor.legacy.RaptorMetadata;
import io.trino.plugin.raptor.legacy.RaptorPartitioningHandle;
import io.trino.plugin.raptor.legacy.RaptorSessionProperties;
import io.trino.plugin.raptor.legacy.RaptorTableHandle;
import io.trino.plugin.raptor.legacy.storage.StorageManagerConfig;
import io.trino.spi.NodeManager;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.ConnectorViewDefinition.ViewColumn;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.testing.TestingConnectorSession;
import io.trino.testing.TestingNodeManager;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Collectors;

import static com.google.common.base.Ticker.systemTicker;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static io.trino.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.trino.plugin.raptor.legacy.DatabaseTesting.createTestingJdbi;
import static io.trino.plugin.raptor.legacy.RaptorTableProperties.BUCKETED_ON_PROPERTY;
import static io.trino.plugin.raptor.legacy.RaptorTableProperties.BUCKET_COUNT_PROPERTY;
import static io.trino.plugin.raptor.legacy.RaptorTableProperties.DISTRIBUTION_NAME_PROPERTY;
import static io.trino.plugin.raptor.legacy.RaptorTableProperties.ORDERING_PROPERTY;
import static io.trino.plugin.raptor.legacy.RaptorTableProperties.ORGANIZED_PROPERTY;
import static io.trino.plugin.raptor.legacy.RaptorTableProperties.TEMPORAL_COLUMN_PROPERTY;
import static io.trino.plugin.raptor.legacy.metadata.SchemaDaoUtil.createTablesWithRetry;
import static io.trino.plugin.raptor.legacy.metadata.TestDatabaseShardManager.createShardManager;
import static io.trino.spi.StandardErrorCode.TRANSACTION_CONFLICT;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_METHOD)
@Execution(SAME_THREAD)
public class TestRaptorMetadata
{
    private static final SchemaTableName DEFAULT_TEST_ORDERS = new SchemaTableName("test", "orders");
    private static final SchemaTableName DEFAULT_TEST_LINEITEMS = new SchemaTableName("test", "lineitems");
    private static final ConnectorSession SESSION = TestingConnectorSession.builder()
            .setPropertyMetadata(new RaptorSessionProperties(new StorageManagerConfig()).getSessionProperties())
            .build();

    private Jdbi dbi;
    private Handle dummyHandle;
    private ShardManager shardManager;
    private RaptorMetadata metadata;

    @BeforeEach
    public void setupDatabase()
    {
        dbi = createTestingJdbi();
        dummyHandle = dbi.open();
        createTablesWithRetry(dbi);

        NodeManager nodeManager = new TestingNodeManager();
        NodeSupplier nodeSupplier = nodeManager::getWorkerNodes;
        shardManager = createShardManager(dbi, nodeSupplier, systemTicker());
        metadata = new RaptorMetadata(dbi, shardManager);
    }

    @AfterEach
    public void cleanupDatabase()
    {
        dummyHandle.close();
        dummyHandle = null;
    }

    @Test
    public void testRenameColumn()
    {
        assertThat(metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS)).isNull();
        metadata.createTable(SESSION, getOrdersTable(), false);
        ConnectorTableHandle tableHandle = metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS);
        assertInstanceOf(tableHandle, RaptorTableHandle.class);

        RaptorTableHandle raptorTableHandle = (RaptorTableHandle) tableHandle;
        ColumnHandle columnHandle = metadata.getColumnHandles(SESSION, tableHandle).get("orderkey");

        metadata.renameColumn(SESSION, raptorTableHandle, columnHandle, "orderkey_renamed");

        assertThat(metadata.getColumnHandles(SESSION, tableHandle).get("orderkey")).isNull();
        assertThat(metadata.getColumnHandles(SESSION, tableHandle).get("orderkey_renamed")).isNotNull();
    }

    @Test
    public void testAddColumn()
    {
        assertThat(metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS)).isNull();
        metadata.createTable(SESSION, buildTable(ImmutableMap.of(), tableMetadataBuilder(DEFAULT_TEST_ORDERS)
                        .column("orderkey", BIGINT)
                        .column("price", BIGINT)),
                false);
        ConnectorTableHandle tableHandle = metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS);
        assertInstanceOf(tableHandle, RaptorTableHandle.class);

        RaptorTableHandle raptorTableHandle = (RaptorTableHandle) tableHandle;

        metadata.addColumn(SESSION, raptorTableHandle, new ColumnMetadata("new_col", BIGINT));
        assertThat(metadata.getColumnHandles(SESSION, raptorTableHandle).get("new_col")).isNotNull();
    }

    @Test
    public void testDropColumn()
    {
        assertThat(metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS)).isNull();
        metadata.createTable(SESSION, buildTable(ImmutableMap.of(), tableMetadataBuilder(DEFAULT_TEST_ORDERS)
                        .column("orderkey", BIGINT)
                        .column("price", BIGINT)),
                false);
        ConnectorTableHandle tableHandle = metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS);
        assertInstanceOf(tableHandle, RaptorTableHandle.class);

        RaptorTableHandle raptorTableHandle = (RaptorTableHandle) tableHandle;

        ColumnHandle lastColumn = metadata.getColumnHandles(SESSION, tableHandle).get("orderkey");
        metadata.dropColumn(SESSION, raptorTableHandle, lastColumn);
        assertThat(metadata.getColumnHandles(SESSION, tableHandle).get("orderkey")).isNull();
    }

    @Test
    public void testAddColumnAfterDropColumn()
    {
        assertThat(metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS)).isNull();
        metadata.createTable(SESSION, buildTable(ImmutableMap.of(), tableMetadataBuilder(DEFAULT_TEST_ORDERS)
                        .column("orderkey", BIGINT)
                        .column("price", BIGINT)),
                false);
        ConnectorTableHandle tableHandle = metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS);
        assertInstanceOf(tableHandle, RaptorTableHandle.class);

        RaptorTableHandle raptorTableHandle = (RaptorTableHandle) tableHandle;
        ColumnHandle column = metadata.getColumnHandles(SESSION, tableHandle).get("orderkey");

        metadata.dropColumn(SESSION, raptorTableHandle, column);
        metadata.addColumn(SESSION, raptorTableHandle, new ColumnMetadata("new_col", BIGINT));
        assertThat(metadata.getColumnHandles(SESSION, tableHandle).get("orderkey")).isNull();
        assertThat(metadata.getColumnHandles(SESSION, raptorTableHandle).get("new_col")).isNotNull();
    }

    @Test
    public void testDropColumnDisallowed()
    {
        assertThat(metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS)).isNull();
        Map<String, Object> properties = ImmutableMap.of(
                BUCKET_COUNT_PROPERTY, 16,
                BUCKETED_ON_PROPERTY, ImmutableList.of("orderkey"),
                ORDERING_PROPERTY, ImmutableList.of("totalprice"),
                TEMPORAL_COLUMN_PROPERTY, "orderdate");
        ConnectorTableMetadata ordersTable = buildTable(properties, tableMetadataBuilder(DEFAULT_TEST_ORDERS)
                .column("orderkey", BIGINT)
                .column("totalprice", DOUBLE)
                .column("orderdate", DATE)
                .column("highestid", BIGINT));
        metadata.createTable(SESSION, ordersTable, false);

        ConnectorTableHandle ordersTableHandle = metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS);
        assertInstanceOf(ordersTableHandle, RaptorTableHandle.class);
        RaptorTableHandle ordersRaptorTableHandle = (RaptorTableHandle) ordersTableHandle;
        assertThat(ordersRaptorTableHandle.getTableId()).isEqualTo(1);

        assertInstanceOf(ordersRaptorTableHandle, RaptorTableHandle.class);

        // disallow dropping bucket, sort, temporal and highest-id columns
        ColumnHandle bucketColumn = metadata.getColumnHandles(SESSION, ordersRaptorTableHandle).get("orderkey");
        assertThatThrownBy(() -> metadata.dropColumn(SESSION, ordersTableHandle, bucketColumn))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Cannot drop bucket columns");

        ColumnHandle sortColumn = metadata.getColumnHandles(SESSION, ordersRaptorTableHandle).get("totalprice");
        assertThatThrownBy(() -> metadata.dropColumn(SESSION, ordersTableHandle, sortColumn))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Cannot drop sort columns");

        ColumnHandle temporalColumn = metadata.getColumnHandles(SESSION, ordersRaptorTableHandle).get("orderdate");
        assertThatThrownBy(() -> metadata.dropColumn(SESSION, ordersTableHandle, temporalColumn))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Cannot drop the temporal column");

        ColumnHandle highestColumn = metadata.getColumnHandles(SESSION, ordersRaptorTableHandle).get("highestid");
        assertThatThrownBy(() -> metadata.dropColumn(SESSION, ordersTableHandle, highestColumn))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Cannot drop the column which has the largest column ID in the table");
    }

    @Test
    public void testRenameTable()
    {
        assertThat(metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS)).isNull();
        metadata.createTable(SESSION, getOrdersTable(), false);
        ConnectorTableHandle tableHandle = metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS);
        assertInstanceOf(tableHandle, RaptorTableHandle.class);

        RaptorTableHandle raptorTableHandle = (RaptorTableHandle) tableHandle;
        SchemaTableName renamedTable = new SchemaTableName(raptorTableHandle.getSchemaName(), "orders_renamed");

        metadata.renameTable(SESSION, raptorTableHandle, renamedTable);
        assertThat(metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS)).isNull();
        ConnectorTableHandle renamedTableHandle = metadata.getTableHandle(SESSION, renamedTable);
        assertThat(renamedTableHandle).isNotNull();
        assertThat(((RaptorTableHandle) renamedTableHandle).getTableName()).isEqualTo(renamedTable.getTableName());
    }

    @Test
    public void testCreateTable()
    {
        assertThat(metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS)).isNull();

        metadata.createTable(SESSION, getOrdersTable(), false);

        ConnectorTableHandle tableHandle = metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS);
        assertInstanceOf(tableHandle, RaptorTableHandle.class);
        assertThat(((RaptorTableHandle) tableHandle).getTableId()).isEqualTo(1);

        ConnectorTableMetadata table = metadata.getTableMetadata(SESSION, tableHandle);
        assertTableEqual(table, getOrdersTable());

        ColumnHandle columnHandle = metadata.getColumnHandles(SESSION, tableHandle).get("orderkey");
        assertInstanceOf(columnHandle, RaptorColumnHandle.class);
        assertThat(((RaptorColumnHandle) columnHandle).getColumnId()).isEqualTo(1);

        ColumnMetadata columnMetadata = metadata.getColumnMetadata(SESSION, tableHandle, columnHandle);
        assertThat(columnMetadata).isNotNull();
        assertThat(columnMetadata.getName()).isEqualTo("orderkey");
        assertThat(columnMetadata.getType()).isEqualTo(BIGINT);
    }

    @Test
    public void testTableProperties()
    {
        assertThat(metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS)).isNull();

        ConnectorTableMetadata ordersTable = getOrdersTable(ImmutableMap.of(
                ORDERING_PROPERTY, ImmutableList.of("orderdate", "custkey"),
                TEMPORAL_COLUMN_PROPERTY, "orderdate"));
        metadata.createTable(SESSION, ordersTable, false);

        ConnectorTableHandle tableHandle = metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS);
        assertInstanceOf(tableHandle, RaptorTableHandle.class);
        RaptorTableHandle raptorTableHandle = (RaptorTableHandle) tableHandle;
        assertThat(raptorTableHandle.getTableId()).isEqualTo(1);

        long tableId = raptorTableHandle.getTableId();
        MetadataDao metadataDao = dbi.onDemand(MetadataDao.class);

        // verify sort columns
        List<TableColumn> sortColumns = metadataDao.listSortColumns(tableId);
        assertTableColumnsEqual(sortColumns, ImmutableList.of(
                new TableColumn(DEFAULT_TEST_ORDERS, "orderdate", DATE, 4, 3, OptionalInt.empty(), OptionalInt.of(0), true),
                new TableColumn(DEFAULT_TEST_ORDERS, "custkey", BIGINT, 2, 1, OptionalInt.empty(), OptionalInt.of(1), false)));

        // verify temporal column
        assertThat(metadataDao.getTemporalColumnId(tableId)).isEqualTo(Long.valueOf(4));

        // verify no organization
        assertThat(metadataDao.getTableInformation(tableId).isOrganized()).isFalse();

        metadata.dropTable(SESSION, tableHandle);
    }

    @Test
    public void testTablePropertiesWithOrganization()
    {
        assertThat(metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS)).isNull();

        ConnectorTableMetadata ordersTable = getOrdersTable(ImmutableMap.of(
                ORDERING_PROPERTY, ImmutableList.of("orderdate", "custkey"),
                ORGANIZED_PROPERTY, true));
        metadata.createTable(SESSION, ordersTable, false);

        ConnectorTableHandle tableHandle = metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS);
        assertInstanceOf(tableHandle, RaptorTableHandle.class);
        RaptorTableHandle raptorTableHandle = (RaptorTableHandle) tableHandle;
        assertThat(raptorTableHandle.getTableId()).isEqualTo(1);

        long tableId = raptorTableHandle.getTableId();
        MetadataDao metadataDao = dbi.onDemand(MetadataDao.class);

        // verify sort columns
        List<TableColumn> sortColumns = metadataDao.listSortColumns(tableId);
        assertTableColumnsEqual(sortColumns, ImmutableList.of(
                new TableColumn(DEFAULT_TEST_ORDERS, "orderdate", DATE, 4, 3, OptionalInt.empty(), OptionalInt.of(0), false),
                new TableColumn(DEFAULT_TEST_ORDERS, "custkey", BIGINT, 2, 1, OptionalInt.empty(), OptionalInt.of(1), false)));

        // verify organization
        assertThat(metadataDao.getTableInformation(tableId).isOrganized()).isTrue();

        metadata.dropTable(SESSION, tableHandle);
    }

    @Test
    public void testCreateBucketedTable()
    {
        assertThat(metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS)).isNull();

        ConnectorTableMetadata ordersTable = getOrdersTable(ImmutableMap.of(
                BUCKET_COUNT_PROPERTY, 16,
                BUCKETED_ON_PROPERTY, ImmutableList.of("custkey", "orderkey")));
        metadata.createTable(SESSION, ordersTable, false);

        ConnectorTableHandle tableHandle = metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS);
        assertInstanceOf(tableHandle, RaptorTableHandle.class);
        RaptorTableHandle raptorTableHandle = (RaptorTableHandle) tableHandle;
        assertThat(raptorTableHandle.getTableId()).isEqualTo(1);

        long tableId = raptorTableHandle.getTableId();
        MetadataDao metadataDao = dbi.onDemand(MetadataDao.class);

        assertTableColumnsEqual(metadataDao.listBucketColumns(tableId), ImmutableList.of(
                new TableColumn(DEFAULT_TEST_ORDERS, "custkey", BIGINT, 2, 1, OptionalInt.of(0), OptionalInt.empty(), false),
                new TableColumn(DEFAULT_TEST_ORDERS, "orderkey", BIGINT, 1, 0, OptionalInt.of(1), OptionalInt.empty(), false)));

        assertThat(raptorTableHandle.getBucketCount()).isEqualTo(OptionalInt.of(16));

        assertThat(getTableDistributionId(tableId)).isEqualTo(Long.valueOf(1));

        metadata.dropTable(SESSION, tableHandle);

        // create a new table and verify it has a different distribution
        metadata.createTable(SESSION, ordersTable, false);
        tableId = ((RaptorTableHandle) metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS)).getTableId();
        assertThat(tableId).isEqualTo(2);
        assertThat(getTableDistributionId(tableId)).isEqualTo(Long.valueOf(2));
    }

    @Test
    public void testCreateBucketedTableAsSelect()
    {
        assertThat(metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS)).isNull();

        ConnectorTableMetadata ordersTable = getOrdersTable(ImmutableMap.of(
                BUCKET_COUNT_PROPERTY, 32,
                BUCKETED_ON_PROPERTY, ImmutableList.of("orderkey", "custkey")));

        ConnectorTableLayout layout = metadata.getNewTableLayout(SESSION, ordersTable).get();
        assertThat(layout.getPartitionColumns()).isEqualTo(ImmutableList.of("orderkey", "custkey"));
        assertThat(layout.getPartitioning().isPresent()).isTrue();
        assertInstanceOf(layout.getPartitioning().get(), RaptorPartitioningHandle.class);
        RaptorPartitioningHandle partitioning = (RaptorPartitioningHandle) layout.getPartitioning().get();
        assertThat(partitioning.getDistributionId()).isEqualTo(1);

        ConnectorOutputTableHandle outputHandle = metadata.beginCreateTable(SESSION, ordersTable, Optional.of(layout), NO_RETRIES);
        metadata.finishCreateTable(SESSION, outputHandle, ImmutableList.of(), ImmutableList.of());

        ConnectorTableHandle tableHandle = metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS);
        assertInstanceOf(tableHandle, RaptorTableHandle.class);
        RaptorTableHandle raptorTableHandle = (RaptorTableHandle) tableHandle;
        assertThat(raptorTableHandle.getTableId()).isEqualTo(1);

        long tableId = raptorTableHandle.getTableId();
        MetadataDao metadataDao = dbi.onDemand(MetadataDao.class);

        assertTableColumnsEqual(metadataDao.listBucketColumns(tableId), ImmutableList.of(
                new TableColumn(DEFAULT_TEST_ORDERS, "orderkey", BIGINT, 1, 0, OptionalInt.of(0), OptionalInt.empty(), false),
                new TableColumn(DEFAULT_TEST_ORDERS, "custkey", BIGINT, 2, 1, OptionalInt.of(1), OptionalInt.empty(), false)));

        assertThat(raptorTableHandle.getBucketCount()).isEqualTo(OptionalInt.of(32));

        assertThat(getTableDistributionId(tableId)).isEqualTo(Long.valueOf(1));

        metadata.dropTable(SESSION, tableHandle);
    }

    @Test
    public void testCreateBucketedTableExistingDistribution()
    {
        MetadataDao metadataDao = dbi.onDemand(MetadataDao.class);

        // create orders table
        assertThat(metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS)).isNull();

        ConnectorTableMetadata table = getOrdersTable(ImmutableMap.of(
                BUCKET_COUNT_PROPERTY, 16,
                BUCKETED_ON_PROPERTY, ImmutableList.of("orderkey"),
                DISTRIBUTION_NAME_PROPERTY, "orders"));
        metadata.createTable(SESSION, table, false);

        ConnectorTableHandle tableHandle = metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS);
        assertInstanceOf(tableHandle, RaptorTableHandle.class);
        RaptorTableHandle raptorTableHandle = (RaptorTableHandle) tableHandle;

        long tableId = raptorTableHandle.getTableId();
        assertThat(raptorTableHandle.getTableId()).isEqualTo(1);

        assertTableColumnsEqual(metadataDao.listBucketColumns(tableId), ImmutableList.of(
                new TableColumn(DEFAULT_TEST_ORDERS, "orderkey", BIGINT, 1, 0, OptionalInt.of(0), OptionalInt.empty(), false)));

        assertThat(raptorTableHandle.getBucketCount()).isEqualTo(OptionalInt.of(16));

        assertThat(getTableDistributionId(tableId)).isEqualTo(Long.valueOf(1));

        // create lineitems table
        assertThat(metadata.getTableHandle(SESSION, DEFAULT_TEST_LINEITEMS)).isNull();

        table = getLineItemsTable(ImmutableMap.of(
                BUCKET_COUNT_PROPERTY, 16,
                BUCKETED_ON_PROPERTY, ImmutableList.of("orderkey"),
                DISTRIBUTION_NAME_PROPERTY, "orders"));
        metadata.createTable(SESSION, table, false);

        tableHandle = metadata.getTableHandle(SESSION, DEFAULT_TEST_LINEITEMS);
        assertInstanceOf(tableHandle, RaptorTableHandle.class);
        raptorTableHandle = (RaptorTableHandle) tableHandle;

        tableId = raptorTableHandle.getTableId();
        assertThat(tableId).isEqualTo(2);

        assertTableColumnsEqual(metadataDao.listBucketColumns(tableId), ImmutableList.of(
                new TableColumn(DEFAULT_TEST_LINEITEMS, "orderkey", BIGINT, 1, 0, OptionalInt.of(0), OptionalInt.empty(), false)));

        assertThat(raptorTableHandle.getBucketCount()).isEqualTo(OptionalInt.of(16));

        assertThat(getTableDistributionId(tableId)).isEqualTo(Long.valueOf(1));
    }

    @Test
    public void testInvalidOrderingColumns()
    {
        assertThat(metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS)).isNull();

        assertThatThrownBy(() -> metadata.createTable(SESSION, getOrdersTable(ImmutableMap.of(ORDERING_PROPERTY, ImmutableList.of("orderdatefoo"))), false))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Ordering column does not exist: orderdatefoo");
    }

    @Test
    public void testInvalidTemporalColumn()
    {
        assertThat(metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS)).isNull();

        assertThatThrownBy(() -> metadata.createTable(SESSION, getOrdersTable(ImmutableMap.of(TEMPORAL_COLUMN_PROPERTY, "foo")), false))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Temporal column does not exist: foo");
    }

    @Test
    public void testInvalidTemporalColumnType()
    {
        assertThat(metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS)).isNull();
        assertThatThrownBy(() -> metadata.createTable(SESSION, getOrdersTable(ImmutableMap.of(TEMPORAL_COLUMN_PROPERTY, "orderkey")), false))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Temporal column must be of type timestamp or date: orderkey");
    }

    @Test
    public void testInvalidTemporalOrganization()
    {
        assertThat(metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS)).isNull();
        assertThatThrownBy(() -> metadata.createTable(SESSION, getOrdersTable(ImmutableMap.of(
                        TEMPORAL_COLUMN_PROPERTY, "orderdate",
                        ORGANIZED_PROPERTY, true)),
                false))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Table with temporal columns cannot be organized");
    }

    @Test
    public void testInvalidOrderingOrganization()
    {
        assertThat(metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS)).isNull();
        assertThatThrownBy(() -> metadata.createTable(SESSION, getOrdersTable(ImmutableMap.of(ORGANIZED_PROPERTY, true)), false))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Table organization requires an ordering");
    }

    @Test
    public void testSortOrderProperty()
    {
        assertThat(metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS)).isNull();

        ConnectorTableMetadata ordersTable = getOrdersTable(ImmutableMap.of(ORDERING_PROPERTY, ImmutableList.of("orderdate", "custkey")));
        metadata.createTable(SESSION, ordersTable, false);

        ConnectorTableHandle tableHandle = metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS);
        assertInstanceOf(tableHandle, RaptorTableHandle.class);
        RaptorTableHandle raptorTableHandle = (RaptorTableHandle) tableHandle;
        assertThat(raptorTableHandle.getTableId()).isEqualTo(1);

        long tableId = raptorTableHandle.getTableId();
        MetadataDao metadataDao = dbi.onDemand(MetadataDao.class);

        // verify sort columns
        List<TableColumn> sortColumns = metadataDao.listSortColumns(tableId);
        assertTableColumnsEqual(sortColumns, ImmutableList.of(
                new TableColumn(DEFAULT_TEST_ORDERS, "orderdate", DATE, 4, 3, OptionalInt.empty(), OptionalInt.of(0), false),
                new TableColumn(DEFAULT_TEST_ORDERS, "custkey", BIGINT, 2, 1, OptionalInt.empty(), OptionalInt.of(1), false)));

        // verify temporal column is not set
        assertThat(metadataDao.getTemporalColumnId(tableId)).isEqualTo(null);
        metadata.dropTable(SESSION, tableHandle);
    }

    @Test
    public void testTemporalColumn()
    {
        assertThat(metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS)).isNull();

        ConnectorTableMetadata ordersTable = getOrdersTable(ImmutableMap.of(TEMPORAL_COLUMN_PROPERTY, "orderdate"));
        metadata.createTable(SESSION, ordersTable, false);

        ConnectorTableHandle tableHandle = metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS);
        assertInstanceOf(tableHandle, RaptorTableHandle.class);
        RaptorTableHandle raptorTableHandle = (RaptorTableHandle) tableHandle;
        assertThat(raptorTableHandle.getTableId()).isEqualTo(1);

        long tableId = raptorTableHandle.getTableId();
        MetadataDao metadataDao = dbi.onDemand(MetadataDao.class);

        // verify sort columns are not set
        List<TableColumn> sortColumns = metadataDao.listSortColumns(tableId);
        assertThat(sortColumns.size()).isEqualTo(0);
        assertThat(sortColumns).isEqualTo(ImmutableList.of());

        // verify temporal column is set
        assertThat(metadataDao.getTemporalColumnId(tableId)).isEqualTo(Long.valueOf(4));
        metadata.dropTable(SESSION, tableHandle);
    }

    @Test
    public void testListTables()
    {
        metadata.createTable(SESSION, getOrdersTable(), false);
        List<SchemaTableName> tables = metadata.listTables(SESSION, Optional.empty());
        assertThat(tables).isEqualTo(ImmutableList.of(DEFAULT_TEST_ORDERS));
    }

    @Test
    public void testListTableColumns()
    {
        metadata.createTable(SESSION, getOrdersTable(), false);
        Map<SchemaTableName, List<ColumnMetadata>> columns = metadata.listTableColumns(SESSION, new SchemaTablePrefix());
        assertThat(columns).isEqualTo(ImmutableMap.of(DEFAULT_TEST_ORDERS, getOrdersTable().getColumns()));
    }

    @Test
    public void testListTableColumnsFiltering()
    {
        metadata.createTable(SESSION, getOrdersTable(), false);
        Map<SchemaTableName, List<ColumnMetadata>> filterCatalog = metadata.listTableColumns(SESSION, new SchemaTablePrefix());
        Map<SchemaTableName, List<ColumnMetadata>> filterSchema = metadata.listTableColumns(SESSION, new SchemaTablePrefix("test"));
        Map<SchemaTableName, List<ColumnMetadata>> filterTable = metadata.listTableColumns(SESSION, new SchemaTablePrefix("test", "orders"));
        assertThat(filterCatalog).isEqualTo(filterSchema);
        assertThat(filterCatalog).isEqualTo(filterTable);
    }

    @Test
    public void testViews()
    {
        SchemaTableName test1 = new SchemaTableName("test", "test_view1");
        SchemaTableName test2 = new SchemaTableName("test", "test_view2");

        // create views
        metadata.createView(SESSION, test1, testingViewDefinition("test1"), false);
        metadata.createView(SESSION, test2, testingViewDefinition("test2"), false);

        // verify listing
        List<SchemaTableName> list = metadata.listViews(SESSION, Optional.of("test"));
        assertEqualsIgnoreOrder(list, ImmutableList.of(test1, test2));

        // verify getting data
        Map<SchemaTableName, ConnectorViewDefinition> views = metadata.getViews(SESSION, Optional.of("test"));
        assertThat(views.keySet()).isEqualTo(ImmutableSet.of(test1, test2));
        assertThat(views.get(test1).getOriginalSql()).isEqualTo("test1");
        assertThat(views.get(test2).getOriginalSql()).isEqualTo("test2");

        // drop first view
        metadata.dropView(SESSION, test1);

        assertThat(metadata.getViews(SESSION, Optional.of("test")))
                .containsOnlyKeys(test2);

        // drop second view
        metadata.dropView(SESSION, test2);

        assertThat(metadata.getViews(SESSION, Optional.of("test")))
                .isEmpty();

        // verify listing everything
        assertThat(metadata.getViews(SESSION, Optional.empty()))
                .isEmpty();
    }

    @Test
    public void testCreateViewWithoutReplace()
    {
        SchemaTableName test = new SchemaTableName("test", "test_view");
        metadata.createView(SESSION, test, testingViewDefinition("test"), false);
        assertThatThrownBy(() -> metadata.createView(SESSION, test, testingViewDefinition("test"), false))
                .isInstanceOf(TrinoException.class)
                .hasMessage("View already exists: test.test_view");
    }

    @Test
    public void testCreateViewWithReplace()
    {
        SchemaTableName test = new SchemaTableName("test", "test_view");

        metadata.createView(SESSION, test, testingViewDefinition("aaa"), true);
        metadata.createView(SESSION, test, testingViewDefinition("bbb"), true);

        assertThat(metadata.getView(SESSION, test))
                .map(ConnectorViewDefinition::getOriginalSql)
                .contains("bbb");
    }

    @Test
    public void testTransactionTableWrite()
    {
        // start table creation
        long transactionId = 1;
        ConnectorOutputTableHandle outputHandle = metadata.beginCreateTable(SESSION, getOrdersTable(), Optional.empty(), NO_RETRIES);

        // transaction is in progress
        assertThat(transactionExists(transactionId)).isTrue();
        assertThat(transactionSuccessful(transactionId)).isNull();

        // commit table creation
        metadata.finishCreateTable(SESSION, outputHandle, ImmutableList.of(), ImmutableList.of());
        assertThat(transactionExists(transactionId)).isTrue();
        assertThat(transactionSuccessful(transactionId)).isTrue();
    }

    @Test
    public void testTransactionInsert()
    {
        // creating a table allocates a transaction
        long transactionId = 1;
        metadata.createTable(SESSION, getOrdersTable(), false);
        assertThat(transactionSuccessful(transactionId)).isTrue();

        // start insert
        transactionId++;
        ConnectorTableHandle tableHandle = metadata.getTableHandle(SESSION, DEFAULT_TEST_ORDERS);
        ConnectorInsertTableHandle insertHandle = metadata.beginInsert(SESSION, tableHandle, ImmutableList.of(), NO_RETRIES);

        // transaction is in progress
        assertThat(transactionExists(transactionId)).isTrue();
        assertThat(transactionSuccessful(transactionId)).isNull();

        // commit insert
        metadata.finishInsert(SESSION, insertHandle, ImmutableList.of(), ImmutableList.of());
        assertThat(transactionExists(transactionId)).isTrue();
        assertThat(transactionSuccessful(transactionId)).isTrue();
    }

    @Test
    public void testTransactionAbort()
    {
        // start table creation
        long transactionId = 1;
        ConnectorOutputTableHandle outputHandle = metadata.beginCreateTable(SESSION, getOrdersTable(), Optional.empty(), NO_RETRIES);

        // transaction is in progress
        assertThat(transactionExists(transactionId)).isTrue();
        assertThat(transactionSuccessful(transactionId)).isNull();

        // force transaction to abort
        shardManager.rollbackTransaction(transactionId);
        assertThat(transactionExists(transactionId)).isTrue();
        assertThat(transactionSuccessful(transactionId)).isFalse();

        // commit table creation
        assertTrinoExceptionThrownBy(() -> metadata.finishCreateTable(SESSION, outputHandle, ImmutableList.of(), ImmutableList.of()))
                .hasErrorCode(TRANSACTION_CONFLICT)
                .hasMessage("Transaction commit failed. Please retry the operation.");
    }

    private boolean transactionExists(long transactionId)
    {
        return dbi.withHandle(handle -> handle
                .select("SELECT count(*) FROM transactions WHERE transaction_id = ?", transactionId)
                .mapTo(boolean.class)
                .one());
    }

    private Boolean transactionSuccessful(long transactionId)
    {
        return dbi.withHandle(handle -> handle
                .select("SELECT successful FROM transactions WHERE transaction_id = ?", transactionId)
                .mapTo(Boolean.class)
                .findFirst()
                .orElse(null));
    }

    private Long getTableDistributionId(long tableId)
    {
        return dbi.withHandle(handle -> handle
                .select("SELECT distribution_id FROM tables WHERE table_id = ?", tableId)
                .mapTo(Long.class)
                .findFirst()
                .orElse(null));
    }

    private static ConnectorTableMetadata getOrdersTable()
    {
        return getOrdersTable(ImmutableMap.of());
    }

    private static ConnectorTableMetadata getOrdersTable(Map<String, Object> properties)
    {
        return buildTable(properties, tableMetadataBuilder(DEFAULT_TEST_ORDERS)
                .column("orderkey", BIGINT)
                .column("custkey", BIGINT)
                .column("totalprice", DOUBLE)
                .column("orderdate", DATE));
    }

    private static ConnectorTableMetadata getLineItemsTable(Map<String, Object> properties)
    {
        return buildTable(properties, tableMetadataBuilder(DEFAULT_TEST_LINEITEMS)
                .column("orderkey", BIGINT)
                .column("partkey", BIGINT)
                .column("quantity", DOUBLE)
                .column("price", DOUBLE));
    }

    private static ConnectorTableMetadata buildTable(Map<String, Object> properties, TableMetadataBuilder builder)
    {
        if (!properties.isEmpty()) {
            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                builder.property(entry.getKey(), entry.getValue());
            }
        }
        return builder.build();
    }

    private static ConnectorViewDefinition testingViewDefinition(String sql)
    {
        return new ConnectorViewDefinition(
                sql,
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(new ViewColumn("test", BIGINT.getTypeId(), Optional.empty())),
                Optional.empty(),
                Optional.empty(),
                true,
                ImmutableList.of());
    }

    private static void assertTableEqual(ConnectorTableMetadata actual, ConnectorTableMetadata expected)
    {
        assertThat(actual.getTable()).isEqualTo(expected.getTable());

        List<ColumnMetadata> actualColumns = actual.getColumns().stream()
                .filter(columnMetadata -> !columnMetadata.isHidden())
                .collect(Collectors.toList());

        List<ColumnMetadata> expectedColumns = expected.getColumns();
        assertThat(actualColumns.size()).isEqualTo(expectedColumns.size());
        for (int i = 0; i < actualColumns.size(); i++) {
            ColumnMetadata actualColumn = actualColumns.get(i);
            ColumnMetadata expectedColumn = expectedColumns.get(i);
            assertThat(actualColumn.getName()).isEqualTo(expectedColumn.getName());
            assertThat(actualColumn.getType()).isEqualTo(expectedColumn.getType());
        }
        assertThat(actual.getProperties()).isEqualTo(expected.getProperties());
    }

    private static void assertTableColumnEqual(TableColumn actual, TableColumn expected)
    {
        assertThat(actual.getTable()).isEqualTo(expected.getTable());
        assertThat(actual.getColumnId()).isEqualTo(expected.getColumnId());
        assertThat(actual.getColumnName()).isEqualTo(expected.getColumnName());
        assertThat(actual.getDataType()).isEqualTo(expected.getDataType());
        assertThat(actual.getOrdinalPosition()).isEqualTo(expected.getOrdinalPosition());
        assertThat(actual.getBucketOrdinal()).isEqualTo(expected.getBucketOrdinal());
        assertThat(actual.getSortOrdinal()).isEqualTo(expected.getSortOrdinal());
        assertThat(actual.isTemporal()).isEqualTo(expected.isTemporal());
    }

    private static void assertTableColumnsEqual(List<TableColumn> actual, List<TableColumn> expected)
    {
        assertThat(actual.size()).isEqualTo(expected.size());
        for (int i = 0; i < actual.size(); i++) {
            assertTableColumnEqual(actual.get(i), expected.get(i));
        }
    }
}
