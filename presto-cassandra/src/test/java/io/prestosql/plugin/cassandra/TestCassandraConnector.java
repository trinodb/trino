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

import com.datastax.driver.core.utils.Bytes;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.block.SingleRowBlock;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorRecordSetProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.SchemaNotFoundException;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import io.prestosql.testing.TestingConnectorContext;
import io.prestosql.testing.TestingConnectorSession;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static io.prestosql.plugin.cassandra.CassandraTestingUtils.TABLE_ALL_TYPES;
import static io.prestosql.plugin.cassandra.CassandraTestingUtils.TABLE_USER_DEFINED_TYPE;
import static io.prestosql.plugin.cassandra.CassandraTestingUtils.createTestTables;
import static io.prestosql.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.UNGROUPED_SCHEDULING;
import static io.prestosql.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.TimeZoneKey.UTC_KEY;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestCassandraConnector
{
    protected static final String INVALID_DATABASE = "totally_invalid_database";
    private static final Date DATE = new Date();
    private static final ConnectorSession SESSION = new TestingConnectorSession(
            "user",
            Optional.of("test"),
            Optional.empty(),
            UTC_KEY,
            ENGLISH,
            System.currentTimeMillis(),
            new CassandraSessionProperties(new CassandraClientConfig()).getSessionProperties(),
            ImmutableMap.of(),
            true);
    protected String database;
    protected SchemaTableName table;
    protected SchemaTableName tableUnpartitioned;
    protected SchemaTableName tableUdt;
    protected SchemaTableName invalidTable;
    private ConnectorMetadata metadata;
    private ConnectorSplitManager splitManager;
    private ConnectorRecordSetProvider recordSetProvider;

    @BeforeClass
    public void setup()
            throws Exception
    {
        EmbeddedCassandra.start();

        String keyspace = "test_connector";
        createTestTables(EmbeddedCassandra.getSession(), keyspace, DATE);

        CassandraConnectorFactory connectorFactory = new CassandraConnectorFactory();

        Connector connector = connectorFactory.create("test", ImmutableMap.of(
                "cassandra.contact-points", EmbeddedCassandra.getHost(),
                "cassandra.native-protocol-port", Integer.toString(EmbeddedCassandra.getPort())),
                new TestingConnectorContext());

        metadata = connector.getMetadata(CassandraTransactionHandle.INSTANCE);
        assertInstanceOf(metadata, CassandraMetadata.class);

        splitManager = connector.getSplitManager();
        assertInstanceOf(splitManager, CassandraSplitManager.class);

        recordSetProvider = connector.getRecordSetProvider();
        assertInstanceOf(recordSetProvider, CassandraRecordSetProvider.class);

        database = keyspace;
        table = new SchemaTableName(database, TABLE_ALL_TYPES.toLowerCase(ENGLISH));
        tableUdt = new SchemaTableName(database, TABLE_USER_DEFINED_TYPE.toLowerCase(ENGLISH));
        tableUnpartitioned = new SchemaTableName(database, "presto_test_unpartitioned");
        invalidTable = new SchemaTableName(database, "totally_invalid_table_name");
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

        List<ConnectorSplit> splits = getAllSplits(splitManager.getSplits(transaction, SESSION, tableHandle, UNGROUPED_SCHEDULING));

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

                    assertEquals(cursor.getSlice(columnIndex.get("typetimestamp")).toStringUtf8(), Long.valueOf(DATE.getTime()).toString());

                    long newCompletedBytes = cursor.getCompletedBytes();
                    assertTrue(newCompletedBytes >= completedBytes);
                    completedBytes = newCompletedBytes;
                }
            }
        }
        assertEquals(rowNumber, 9);
    }

    @Test
    public void testGetUserDefinedType()
    {
        ConnectorTableHandle tableHandle = getTableHandle(tableUdt);
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, tableHandle);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(SESSION, tableHandle).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        ConnectorTransactionHandle transaction = CassandraTransactionHandle.INSTANCE;

        tableHandle = metadata.applyFilter(SESSION, tableHandle, Constraint.alwaysTrue()).get().getHandle();

        List<ConnectorSplit> splits = getAllSplits(splitManager.getSplits(transaction, SESSION, tableHandle, UNGROUPED_SCHEDULING));

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
                    SingleRowBlock udtValue = (SingleRowBlock) cursor.getObject(columnIndex.get("typeudt"));

                    assertEquals(keyValue, "key");
                    assertEquals(VARCHAR.getSlice(udtValue, 0).toStringUtf8(), "text");
                    assertEquals(VARCHAR.getSlice(udtValue, 1).toStringUtf8(), "01234567-0123-0123-0123-0123456789ab");
                    assertEquals(INTEGER.getLong(udtValue, 2), -2147483648);
                    assertEquals(BIGINT.getLong(udtValue, 3), -9223372036854775808L);
                    assertEquals(VARBINARY.getSlice(udtValue, 4).toStringUtf8(), "01234");
                    assertEquals(TIMESTAMP.getLong(udtValue, 5), 54000000);
                    assertEquals(VARCHAR.getSlice(udtValue, 6).toStringUtf8(), "ansi");
                    assertTrue(BOOLEAN.getBoolean(udtValue, 7));
                    assertEquals(DOUBLE.getDouble(udtValue, 8), 99999999999999997748809823456034029568D);
                    assertEquals(DOUBLE.getDouble(udtValue, 9), 4.9407e-324);
                    assertEquals(REAL.getObjectValue(SESSION, udtValue, 10), 1.4E-45f);
                    assertEquals(VARCHAR.getSlice(udtValue, 11).toStringUtf8(), "0.0.0.0");
                    assertEquals(VARCHAR.getSlice(udtValue, 12).toStringUtf8(), "varchar");
                    assertEquals(VARCHAR.getSlice(udtValue, 13).toStringUtf8(), "-9223372036854775808");
                    assertEquals(VARCHAR.getSlice(udtValue, 14).toStringUtf8(), "d2177dd0-eaa2-11de-a572-001b779c76e3");
                    assertEquals(VARCHAR.getSlice(udtValue, 15).toStringUtf8(), "[\"list\"]");
                    assertEquals(VARCHAR.getSlice(udtValue, 16).toStringUtf8(), "{\"map\":1}");
                    assertEquals(VARCHAR.getSlice(udtValue, 17).toStringUtf8(), "[true]");

                    long newCompletedBytes = cursor.getCompletedBytes();
                    assertTrue(newCompletedBytes >= completedBytes);
                    completedBytes = newCompletedBytes;
                }
            }
        }
        assertEquals(rowNumber, 1);
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
                else if (INTEGER.equals(type)) {
                    cursor.getLong(columnIndex);
                }
                else if (BIGINT.equals(type)) {
                    cursor.getLong(columnIndex);
                }
                else if (TIMESTAMP.equals(type)) {
                    cursor.getLong(columnIndex);
                }
                else if (DOUBLE.equals(type)) {
                    cursor.getDouble(columnIndex);
                }
                else if (REAL.equals(type)) {
                    cursor.getLong(columnIndex);
                }
                else if (isVarcharType(type) || VARBINARY.equals(type)) {
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
                    fail("Unknown primitive type " + columnIndex);
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
}
