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
package io.prestosql.plugin.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.spi.connector.SchemaTableName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static io.prestosql.plugin.jdbc.TestingJdbcTypeHandle.JDBC_BIGINT;
import static io.prestosql.plugin.jdbc.TestingJdbcTypeHandle.JDBC_VARCHAR;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

@Test
public class TestJdbcRecordSet
{
    private TestingDatabase database;
    private JdbcClient jdbcClient;
    private JdbcTableHandle table;
    private JdbcSplit split;
    private Map<String, JdbcColumnHandle> columnHandles;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        database = new TestingDatabase();
        jdbcClient = database.getJdbcClient();
        table = database.getTableHandle(SESSION, new SchemaTableName("example", "numbers"));
        split = database.getSplit(SESSION, table);
        columnHandles = database.getColumnHandles(SESSION, table);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        database.close();
    }

    @Test
    public void testGetColumnTypes()
    {
        RecordSet recordSet = new JdbcRecordSet(jdbcClient, SESSION, split, table, ImmutableList.of(
                new JdbcColumnHandle("text", JDBC_VARCHAR, VARCHAR, true),
                new JdbcColumnHandle("text_short", JDBC_VARCHAR, createVarcharType(32), true),
                new JdbcColumnHandle("value", JDBC_BIGINT, BIGINT, true)));
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of(VARCHAR, createVarcharType(32), BIGINT));

        recordSet = new JdbcRecordSet(jdbcClient, SESSION, split, table, ImmutableList.of(
                new JdbcColumnHandle("value", JDBC_BIGINT, BIGINT, true),
                new JdbcColumnHandle("text", JDBC_VARCHAR, VARCHAR, true)));
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of(BIGINT, VARCHAR));

        recordSet = new JdbcRecordSet(jdbcClient, SESSION, split, table, ImmutableList.of(
                new JdbcColumnHandle("value", JDBC_BIGINT, BIGINT, true),
                new JdbcColumnHandle("value", JDBC_BIGINT, BIGINT, true),
                new JdbcColumnHandle("text", JDBC_VARCHAR, VARCHAR, true)));
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of(BIGINT, BIGINT, VARCHAR));

        recordSet = new JdbcRecordSet(jdbcClient, SESSION, split, table, ImmutableList.of());
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of());
    }

    @Test
    public void testCursorSimple()
    {
        RecordSet recordSet = new JdbcRecordSet(jdbcClient, SESSION, split, table, ImmutableList.of(
                columnHandles.get("text"),
                columnHandles.get("text_short"),
                columnHandles.get("value")));

        try (RecordCursor cursor = recordSet.cursor()) {
            assertEquals(cursor.getType(0), VARCHAR);
            assertEquals(cursor.getType(1), createVarcharType(32));
            assertEquals(cursor.getType(2), BIGINT);

            Map<String, Long> data = new LinkedHashMap<>();
            while (cursor.advanceNextPosition()) {
                data.put(cursor.getSlice(0).toStringUtf8(), cursor.getLong(2));
                assertEquals(cursor.getSlice(0), cursor.getSlice(1));
                assertFalse(cursor.isNull(0));
                assertFalse(cursor.isNull(1));
                assertFalse(cursor.isNull(2));
            }

            assertEquals(data, ImmutableMap.<String, Long>builder()
                    .put("one", 1L)
                    .put("two", 2L)
                    .put("three", 3L)
                    .put("ten", 10L)
                    .put("eleven", 11L)
                    .put("twelve", 12L)
                    .build());
        }
    }

    @Test
    public void testCursorMixedOrder()
    {
        RecordSet recordSet = new JdbcRecordSet(jdbcClient, SESSION, split, table, ImmutableList.of(
                columnHandles.get("value"),
                columnHandles.get("value"),
                columnHandles.get("text")));

        try (RecordCursor cursor = recordSet.cursor()) {
            assertEquals(cursor.getType(0), BIGINT);
            assertEquals(cursor.getType(1), BIGINT);
            assertEquals(cursor.getType(2), VARCHAR);

            Map<String, Long> data = new LinkedHashMap<>();
            while (cursor.advanceNextPosition()) {
                assertEquals(cursor.getLong(0), cursor.getLong(1));
                data.put(cursor.getSlice(2).toStringUtf8(), cursor.getLong(0));
            }

            assertEquals(data, ImmutableMap.<String, Long>builder()
                    .put("one", 1L)
                    .put("two", 2L)
                    .put("three", 3L)
                    .put("ten", 10L)
                    .put("eleven", 11L)
                    .put("twelve", 12L)
                    .build());
        }
    }

    @Test
    public void testIdempotentClose()
    {
        RecordSet recordSet = new JdbcRecordSet(jdbcClient, SESSION, split, table, ImmutableList.of(
                columnHandles.get("value"),
                columnHandles.get("value"),
                columnHandles.get("text")));

        RecordCursor cursor = recordSet.cursor();
        cursor.close();
        cursor.close();
    }
}
