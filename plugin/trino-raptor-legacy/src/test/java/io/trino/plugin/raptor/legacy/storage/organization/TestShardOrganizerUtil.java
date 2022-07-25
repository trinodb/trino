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
package io.trino.plugin.raptor.legacy.storage.organization;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import io.trino.plugin.raptor.legacy.RaptorMetadata;
import io.trino.plugin.raptor.legacy.metadata.ColumnInfo;
import io.trino.plugin.raptor.legacy.metadata.ColumnStats;
import io.trino.plugin.raptor.legacy.metadata.MetadataDao;
import io.trino.plugin.raptor.legacy.metadata.ShardInfo;
import io.trino.plugin.raptor.legacy.metadata.ShardManager;
import io.trino.plugin.raptor.legacy.metadata.ShardMetadata;
import io.trino.plugin.raptor.legacy.metadata.Table;
import io.trino.plugin.raptor.legacy.metadata.TableColumn;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.trino.plugin.raptor.legacy.DatabaseTesting.createTestingJdbi;
import static io.trino.plugin.raptor.legacy.metadata.SchemaDaoUtil.createTablesWithRetry;
import static io.trino.plugin.raptor.legacy.metadata.TestDatabaseShardManager.createShardManager;
import static io.trino.plugin.raptor.legacy.metadata.TestDatabaseShardManager.shardInfo;
import static io.trino.plugin.raptor.legacy.storage.organization.ShardOrganizerUtil.getOrganizationEligibleShards;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestShardOrganizerUtil
{
    private static final List<ColumnInfo> COLUMNS = ImmutableList.of(
            new ColumnInfo(1, TIMESTAMP_MILLIS),
            new ColumnInfo(2, BIGINT),
            new ColumnInfo(3, VARCHAR));

    private Jdbi dbi;
    private Handle dummyHandle;
    private File dataDir;
    private ShardManager shardManager;
    private MetadataDao metadataDao;
    private ConnectorMetadata metadata;

    @BeforeMethod
    public void setup()
    {
        dbi = createTestingJdbi();
        dummyHandle = dbi.open();
        createTablesWithRetry(dbi);
        dataDir = Files.createTempDir();

        metadata = new RaptorMetadata(dbi, createShardManager(dbi));

        metadataDao = dbi.onDemand(MetadataDao.class);
        shardManager = createShardManager(dbi);
    }

    @AfterMethod(alwaysRun = true)
    public void teardown()
            throws Exception
    {
        dummyHandle.close();
        deleteRecursively(dataDir.toPath(), ALLOW_INSECURE);
    }

    @Test
    public void testGetOrganizationEligibleShards()
    {
        int day1 = 1111;
        int day2 = 2222;

        SchemaTableName tableName = new SchemaTableName("default", "test");
        metadata.createTable(SESSION, tableMetadataBuilder(tableName)
                        .column("orderkey", BIGINT)
                        .column("orderdate", DATE)
                        .column("orderstatus", createVarcharType(3))
                        .property("ordering", ImmutableList.of("orderstatus", "orderkey"))
                        .property("temporal_column", "orderdate")
                        .build(),
                false);
        Table tableInfo = metadataDao.getTableInformation(tableName.getSchemaName(), tableName.getTableName());
        List<TableColumn> tableColumns = metadataDao.listTableColumns(tableInfo.getTableId());
        Map<String, TableColumn> tableColumnMap = Maps.uniqueIndex(tableColumns, TableColumn::getColumnName);

        long orderDate = tableColumnMap.get("orderdate").getColumnId();
        long orderKey = tableColumnMap.get("orderkey").getColumnId();
        long orderStatus = tableColumnMap.get("orderstatus").getColumnId();

        List<ShardInfo> shards = ImmutableList.<ShardInfo>builder()
                .add(shardInfo(
                        UUID.randomUUID(),
                        "node1",
                        ImmutableList.of(
                                new ColumnStats(orderDate, day1, day1 + 10),
                                new ColumnStats(orderKey, 13L, 14L),
                                new ColumnStats(orderStatus, "aaa", "abc"))))
                .add(shardInfo(
                        UUID.randomUUID(),
                        "node1",
                        ImmutableList.of(
                                new ColumnStats(orderDate, day2, day2 + 100),
                                new ColumnStats(orderKey, 2L, 20L),
                                new ColumnStats(orderStatus, "aaa", "abc"))))
                .add(shardInfo(
                        UUID.randomUUID(),
                        "node1",
                        ImmutableList.of(
                                new ColumnStats(orderDate, day1, day2),
                                new ColumnStats(orderKey, 2L, 11L),
                                new ColumnStats(orderStatus, "aaa", "abc"))))
                .add(shardInfo(
                        UUID.randomUUID(),
                        "node1",
                        ImmutableList.of(
                                new ColumnStats(orderDate, day1, day2),
                                new ColumnStats(orderKey, 2L, null),
                                new ColumnStats(orderStatus, "aaa", "abc"))))
                .add(shardInfo(
                        UUID.randomUUID(),
                        "node1",
                        ImmutableList.of(
                                new ColumnStats(orderDate, day1, null),
                                new ColumnStats(orderKey, 2L, 11L),
                                new ColumnStats(orderStatus, "aaa", "abc"))))
                .build();

        long transactionId = shardManager.beginTransaction();
        shardManager.commitShards(transactionId, tableInfo.getTableId(), COLUMNS, shards, Optional.empty(), 0);
        Set<ShardMetadata> shardMetadatas = shardManager.getNodeShards("node1");

        Long temporalColumnId = metadataDao.getTemporalColumnId(tableInfo.getTableId());
        TableColumn temporalColumn = metadataDao.getTableColumn(tableInfo.getTableId(), temporalColumnId);

        Set<ShardIndexInfo> actual = ImmutableSet.copyOf(getOrganizationEligibleShards(dbi, metadataDao, tableInfo, shardMetadatas, false));
        List<ShardIndexInfo> expected = getShardIndexInfo(tableInfo, shards, temporalColumn, Optional.empty());

        assertEquals(actual, expected);

        List<TableColumn> sortColumns = metadataDao.listSortColumns(tableInfo.getTableId());
        Set<ShardIndexInfo> actualSortRange = ImmutableSet.copyOf(getOrganizationEligibleShards(dbi, metadataDao, tableInfo, shardMetadatas, true));
        List<ShardIndexInfo> expectedSortRange = getShardIndexInfo(tableInfo, shards, temporalColumn, Optional.of(sortColumns));

        assertEquals(actualSortRange, expectedSortRange);
    }

    private static List<ShardIndexInfo> getShardIndexInfo(Table tableInfo, List<ShardInfo> shards, TableColumn temporalColumn, Optional<List<TableColumn>> sortColumns)
    {
        long tableId = tableInfo.getTableId();
        Type temporalType = temporalColumn.getDataType();

        ImmutableList.Builder<ShardIndexInfo> builder = ImmutableList.builder();
        for (ShardInfo shard : shards) {
            ColumnStats temporalColumnStats = shard.getColumnStats().stream()
                    .filter(columnStats -> columnStats.getColumnId() == temporalColumn.getColumnId())
                    .findFirst()
                    .get();

            if (temporalColumnStats.getMin() == null || temporalColumnStats.getMax() == null) {
                continue;
            }

            Optional<ShardRange> sortRange = Optional.empty();
            if (sortColumns.isPresent()) {
                Map<Long, ColumnStats> columnIdToStats = Maps.uniqueIndex(shard.getColumnStats(), ColumnStats::getColumnId);
                ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
                ImmutableList.Builder<Object> minBuilder = ImmutableList.builder();
                ImmutableList.Builder<Object> maxBuilder = ImmutableList.builder();
                boolean isShardEligible = true;
                for (TableColumn sortColumn : sortColumns.get()) {
                    ColumnStats columnStats = columnIdToStats.get(sortColumn.getColumnId());
                    typesBuilder.add(sortColumn.getDataType());

                    if (columnStats.getMin() == null || columnStats.getMax() == null) {
                        isShardEligible = false;
                        break;
                    }

                    minBuilder.add(columnStats.getMin());
                    maxBuilder.add(columnStats.getMax());
                }

                if (!isShardEligible) {
                    continue;
                }

                List<Type> types = typesBuilder.build();
                List<Object> minValues = minBuilder.build();
                List<Object> maxValues = maxBuilder.build();
                sortRange = Optional.of(ShardRange.of(new Tuple(types, minValues), new Tuple(types, maxValues)));
            }
            builder.add(new ShardIndexInfo(
                    tableId,
                    OptionalInt.empty(),
                    shard.getShardUuid(),
                    shard.getRowCount(),
                    shard.getUncompressedSize(),
                    sortRange,
                    Optional.of(ShardRange.of(
                            new Tuple(temporalType, temporalColumnStats.getMin()),
                            new Tuple(temporalType, temporalColumnStats.getMax())))));
        }
        return builder.build();
    }
}
