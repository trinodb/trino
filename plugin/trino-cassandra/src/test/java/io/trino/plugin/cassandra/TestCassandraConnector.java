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

import com.datastax.driver.core.utils.Bytes;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.trino.spi.block.SingleRowBlock;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.DateType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.testing.TestingConnectorContext;
import io.trino.testing.TestingConnectorSession;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static io.trino.plugin.cassandra.CassandraTestingUtils.TABLE_ALL_TYPES;
import static io.trino.plugin.cassandra.CassandraTestingUtils.TABLE_DELETE_DATA;
import static io.trino.plugin.cassandra.CassandraTestingUtils.TABLE_TUPLE_TYPE;
import static io.trino.plugin.cassandra.CassandraTestingUtils.createTestTables;
import static io.trino.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.UNGROUPED_SCHEDULING;
import static io.trino.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestCassandraConnector
{
    protected static final String INVALID_DATABASE = "totally_invalid_database";
    private static final Date DATE = new Date();
    private static final ConnectorSession SESSION = TestingConnectorSession.builder()
            .setPropertyMetadata(new CassandraSessionProperties(new CassandraClientConfig()).getSessionProperties())
            .build();
    private CassandraServer server;
    protected String database;
    protected SchemaTableName table;
    protected SchemaTableName tableForDelete;
    protected SchemaTableName tableTuple;
    private ConnectorMetadata metadata;
    private ConnectorSplitManager splitManager;
    private ConnectorRecordSetProvider recordSetProvider;

    @BeforeClass
    public void setup()
            throws Exception
    {
        this.server = new CassandraServer();

        String keyspace = "test_connector";
        createTestTables(server.getSession(), keyspace, DATE);

        CassandraConnectorFactory connectorFactory = new CassandraConnectorFactory();

        Connector connector = connectorFactory.create("test", ImmutableMap.of(
                "cassandra.contact-points", server.getHost(),
                "cassandra.native-protocol-port", Integer.toString(server.getPort())),
                new TestingConnectorContext());

        metadata = connector.getMetadata(CassandraTransactionHandle.INSTANCE);
        assertInstanceOf(metadata, CassandraMetadata.class);

        splitManager = connector.getSplitManager();
        assertInstanceOf(splitManager, CassandraSplitManager.class);

        recordSetProvider = connector.getRecordSetProvider();
        assertInstanceOf(recordSetProvider, CassandraRecordSetProvider.class);

        database = keyspace;
        table = new SchemaTableName(database, TABLE_ALL_TYPES.toLowerCase(ENGLISH));
        tableForDelete = new SchemaTableName(database, TABLE_DELETE_DATA.toLowerCase(ENGLISH));
        tableTuple = new SchemaTableName(database, TABLE_TUPLE_TYPE.toLowerCase(ENGLISH));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        server.close();
    }

    @Test
    public void testGetDatabaseNames()
    {
        List<String> databases = metadata.listSchemaNames(SESSION);
        assertTrue(databases.contains(database.toLowerCase(ENGLISH)));
    }

    @Test
    public void testGetTableNames()
    {
        List<SchemaTableName> tables = metadata.listTables(SESSION, Optional.of(database));
        assertTrue(tables.contains(table));
    }

    // disabled until metadata manager is updated to handle invalid catalogs and schemas
    @Test(enabled = false, expectedExceptions = SchemaNotFoundException.class)
    public void testGetTableNamesException()
    {
        metadata.listTables(SESSION, Optional.of(INVALID_DATABASE));
    }

    @Test
    public void testListUnknownSchema()
    {
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("totally_invalid_database_name", "dual")));
        assertEquals(metadata.listTables(SESSION, Optional.of("totally_invalid_database_name")), ImmutableList.of());
        assertEquals(metadata.listTableColumns(SESSION, new SchemaTablePrefix("totally_invalid_database_name", "dual")), ImmutableMap.of());
    }

    @Test
    public void testGetRecords()
    {
        ConnectorTableHandle tableHandle = getTableHandle(table);
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, tableHandle);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(SESSION, tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        ConnectorTransactionHandle transaction = CassandraTransactionHandle.INSTANCE;

        tableHandle = metadata.applyFilter(SESSION, tableHandle, Constraint.alwaysTrue()).get().getHandle();

        List<ConnectorSplit> splits = getAllSplits(splitManager.getSplits(transaction, SESSION, tableHandle, UNGROUPED_SCHEDULING, DynamicFilter.EMPTY));

        long rowNumber = 0;
        for (ConnectorSplit split : splits) {
            CassandraSplit cassandraSplit = (CassandraSplit) split;

            long completedBytes = 0;
            try (RecordCursor cursor = recordSetProvider.getRecordSet(transaction, SESSION, cassandraSplit, tableHandle, columnHandles).cursor()) {
                while (cursor.advanceNextPosition()) {
                    try {
                        assertReadFields(cursor, tableMetadata.getColumns());
                    }
                    catch (RuntimeException e) {
                        throw new RuntimeException("row " + rowNumber, e);
                    }

                    rowNumber++;

                    String keyValue = cursor.getSlice(columnIndex.get("key")).toStringUtf8();
                    assertTrue(keyValue.startsWith("key "));
                    int rowId = Integer.parseInt(keyValue.substring(4));

                    assertEquals(keyValue, "key " + rowId);

                    assertEquals(Bytes.toHexString(cursor.getSlice(columnIndex.get("typebytes")).getBytes()), format("0x%08X", rowId));

                    // VARINT is returned as a string
                    assertEquals(cursor.getSlice(columnIndex.get("typeinteger")).toStringUtf8(), String.valueOf(rowId));

                    assertEquals(cursor.getLong(columnIndex.get("typelong")), 1000 + rowId);

                    assertEquals(cursor.getSlice(columnIndex.get("typeuuid")).toStringUtf8(), format("00000000-0000-0000-0000-%012d", rowId));

                    assertEquals(cursor.getLong(columnIndex.get("typetimestamp")), packDateTimeWithZone(DATE.getTime(), UTC_KEY));

                    long newCompletedBytes = cursor.getCompletedBytes();
                    assertTrue(newCompletedBytes >= completedBytes);
                    completedBytes = newCompletedBytes;
                }
            }
        }
        assertEquals(rowNumber, 9);
    }

    @Test
    public void testExecuteDelete()
    {
        assertNumberOfRows(tableForDelete, 15);
        CassandraTableHandle handle1 = getTableHandle(Optional.of(List.of(createPartition(1, 1))), "");
        metadata.executeDelete(SESSION, handle1);
        assertNumberOfRows(tableForDelete, 12);

        CassandraTableHandle handle2 = getTableHandle(Optional.of(List.of(createPartition(1, 2))), "clust_one='clust_one_2'");
        metadata.executeDelete(SESSION, handle2);
        assertNumberOfRows(tableForDelete, 11);

        CassandraTableHandle handle3 = getTableHandle(Optional.of(List.of(createPartition(1, 2), createPartition(2, 2))), "");
        metadata.executeDelete(SESSION, handle3);
        assertNumberOfRows(tableForDelete, 7);
    }

    @Test
    public void testGetTupleType()
    {
        // TODO add test with nested tuple types
        ConnectorTableHandle tableHandle = getTableHandle(tableTuple);
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, tableHandle);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(SESSION, tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        ConnectorTransactionHandle transaction = CassandraTransactionHandle.INSTANCE;

        List<ConnectorSplit> splits = getAllSplits(splitManager.getSplits(transaction, SESSION, tableHandle, UNGROUPED_SCHEDULING, DynamicFilter.EMPTY));

        long rowNumber = 0;
        for (ConnectorSplit split : splits) {
            CassandraSplit cassandraSplit = (CassandraSplit) split;

            long completedBytes = 0;
            try (RecordCursor cursor = recordSetProvider.getRecordSet(transaction, SESSION, cassandraSplit, tableHandle, columnHandles).cursor()) {
                while (cursor.advanceNextPosition()) {
                    try {
                        assertReadFields(cursor, tableMetadata.getColumns());
                    }
                    catch (RuntimeException e) {
                        throw new RuntimeException("row " + rowNumber, e);
                    }

                    rowNumber++;

                    String keyValue = cursor.getSlice(columnIndex.get("key")).toStringUtf8();
                    assertEquals(keyValue, Long.toString(rowNumber));

                    SingleRowBlock tupleValueBlock = (SingleRowBlock) cursor.getObject(columnIndex.get("typetuple"));
                    assertThat(tupleValueBlock.getPositionCount()).isEqualTo(3);

                    CassandraColumnHandle tupleColumnHandle = (CassandraColumnHandle) columnHandles.get(columnIndex.get("typetuple"));
                    List<CassandraType> tupleArgumentTypes = tupleColumnHandle.getCassandraType().getArgumentTypes();
                    assertThat(tupleArgumentTypes.get(0).getTrinoType().getLong(tupleValueBlock, 0)).isEqualTo(rowNumber);
                    assertThat(tupleArgumentTypes.get(1).getTrinoType().getSlice(tupleValueBlock, 1).toStringUtf8()).isEqualTo("text-" + rowNumber);
                    assertThat(tupleArgumentTypes.get(2).getTrinoType().getLong(tupleValueBlock, 2)).isEqualTo(Float.floatToRawIntBits(1.11f * rowNumber));

                    long newCompletedBytes = cursor.getCompletedBytes();
                    assertTrue(newCompletedBytes >= completedBytes);
                    completedBytes = newCompletedBytes;
                }
            }
        }
        assertEquals(rowNumber, 2);
    }

    private static void assertReadFields(RecordCursor cursor, List<ColumnMetadata> schema)
    {
        for (int columnIndex = 0; columnIndex < schema.size(); columnIndex++) {
            ColumnMetadata column = schema.get(columnIndex);
            if (!cursor.isNull(columnIndex)) {
                Type type = column.getType();
                if (BOOLEAN.equals(type)) {
                    cursor.getBoolean(columnIndex);
                }
                else if (TINYINT.equals(type)) {
                    SignedBytes.checkedCast(cursor.getLong(columnIndex));
                }
                else if (SMALLINT.equals(type)) {
                    Shorts.checkedCast(cursor.getLong(columnIndex));
                }
                else if (INTEGER.equals(type)) {
                    toIntExact(cursor.getLong(columnIndex));
                }
                else if (BIGINT.equals(type)) {
                    cursor.getLong(columnIndex);
                }
                else if (DateType.DATE.equals(type)) {
                    toIntExact(cursor.getLong(columnIndex));
                }
                else if (TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
                    cursor.getLong(columnIndex);
                }
                else if (DOUBLE.equals(type)) {
                    cursor.getDouble(columnIndex);
                }
                else if (REAL.equals(type)) {
                    cursor.getLong(columnIndex);
                }
                else if (type instanceof VarcharType || VARBINARY.equals(type)) {
                    try {
                        cursor.getSlice(columnIndex);
                    }
                    catch (RuntimeException e) {
                        throw new RuntimeException("column " + column, e);
                    }
                }
                else if (type instanceof RowType) {
                    cursor.getObject(columnIndex);
                }
                else {
                    fail("Unknown primitive type " + type + " for column " + columnIndex);
                }
            }
        }
    }

    private ConnectorTableHandle getTableHandle(SchemaTableName tableName)
    {
        ConnectorTableHandle handle = metadata.getTableHandle(SESSION, tableName);
        checkArgument(handle != null, "table not found: %s", tableName);
        return handle;
    }

    private static List<ConnectorSplit> getAllSplits(ConnectorSplitSource splitSource)
    {
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        while (!splitSource.isFinished()) {
            splits.addAll(getFutureValue(splitSource.getNextBatch(NOT_PARTITIONED, 1000)).getSplits());
        }
        return splits.build();
    }

    private static ImmutableMap<String, Integer> indexColumns(List<ColumnHandle> columnHandles)
    {
        ImmutableMap.Builder<String, Integer> index = ImmutableMap.builder();
        int i = 0;
        for (ColumnHandle columnHandle : columnHandles) {
            String name = ((CassandraColumnHandle) columnHandle).getName();
            index.put(name, i);
            i++;
        }
        return index.build();
    }

    private CassandraTableHandle getTableHandle(Optional<List<CassandraPartition>> partitions, String clusteringKeyPredicates)
    {
        CassandraTableHandle handle = (CassandraTableHandle) getTableHandle(tableForDelete);
        return new CassandraTableHandle(handle.getSchemaName(), handle.getTableName(), partitions, clusteringKeyPredicates);
    }

    private CassandraPartition createPartition(long value1, long value2)
    {
        CassandraColumnHandle column1 = new CassandraColumnHandle("partition_one", 1, CassandraTypes.BIGINT, true, false, false, false);
        CassandraColumnHandle column2 = new CassandraColumnHandle("partition_two", 2, CassandraTypes.INT, true, false, false, false);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        column1, Domain.singleValue(BIGINT, value1),
                        column2, Domain.singleValue(INTEGER, value2)));
        String partitionId = format("partition_one=%d AND partition_two=%d", value1, value2);
        return new CassandraPartition(new byte[0], partitionId, tupleDomain, true);
    }

    private void assertNumberOfRows(SchemaTableName tableName, int rowsCount)
    {
        CassandraSession session = server.getSession();
        assertEquals(session.execute("SELECT COUNT(*) FROM " + tableName).all().get(0).getLong(0), rowsCount);
    }
}
