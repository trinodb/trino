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
package io.trino.plugin.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.connector.SchemaTableName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.airlift.testing.Closeables.closeAll;
import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_BIGINT;
import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_VARCHAR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestJdbcRecordSet
{
    private TestingDatabase database;
    private JdbcClient jdbcClient;
    private JdbcTableHandle table;
    private JdbcSplit split;
    private Map<String, JdbcColumnHandle> columnHandles;
    private ExecutorService executor;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        database = new TestingDatabase();
        jdbcClient = database.getJdbcClient();
        table = database.getTableHandle(SESSION, new SchemaTableName("example", "numbers"));
        split = database.getSplit(SESSION, table);
        columnHandles = database.getColumnHandles(SESSION, table);
        executor = newDirectExecutorService();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        closeAll(
                database,
                () -> executor.shutdownNow());
        database = null;
        executor = null;
    }

    @Test
    public void testGetColumnTypes()
    {
        RecordSet recordSet = createRecordSet(ImmutableList.of(
                new JdbcColumnHandle("text", JDBC_VARCHAR, VARCHAR),
                new JdbcColumnHandle("text_short", JDBC_VARCHAR, createVarcharType(32)),
                new JdbcColumnHandle("value", JDBC_BIGINT, BIGINT)));
        assertThat(recordSet.getColumnTypes()).containsExactly(VARCHAR, createVarcharType(32), BIGINT);

        recordSet = createRecordSet(ImmutableList.of(
                new JdbcColumnHandle("value", JDBC_BIGINT, BIGINT),
                new JdbcColumnHandle("text", JDBC_VARCHAR, VARCHAR)));
        assertThat(recordSet.getColumnTypes()).containsExactly(BIGINT, VARCHAR);

        recordSet = createRecordSet(ImmutableList.of(
                new JdbcColumnHandle("value", JDBC_BIGINT, BIGINT),
                new JdbcColumnHandle("value", JDBC_BIGINT, BIGINT),
                new JdbcColumnHandle("text", JDBC_VARCHAR, VARCHAR)));
        assertThat(recordSet.getColumnTypes()).containsExactly(BIGINT, BIGINT, VARCHAR);

        recordSet = createRecordSet(ImmutableList.of());
        assertThat(recordSet.getColumnTypes()).isEmpty();
    }

    @Test
    public void testCursorSimple()
    {
        RecordSet recordSet = createRecordSet(ImmutableList.of(
                columnHandles.get("text"),
                columnHandles.get("text_short"),
                columnHandles.get("value")));

        try (RecordCursor cursor = recordSet.cursor()) {
            assertThat(cursor.getType(0)).isEqualTo(VARCHAR);
            assertThat(cursor.getType(1)).isEqualTo(createVarcharType(32));
            assertThat(cursor.getType(2)).isEqualTo(BIGINT);

            Map<String, Long> data = new LinkedHashMap<>();
            while (cursor.advanceNextPosition()) {
                data.put(cursor.getSlice(0).toStringUtf8(), cursor.getLong(2));
                assertThat(cursor.getSlice(0)).isEqualTo(cursor.getSlice(1));
                assertThat(cursor.isNull(0)).isFalse();
                assertThat(cursor.isNull(1)).isFalse();
                assertThat(cursor.isNull(2)).isFalse();
            }

            assertThat(data).isEqualTo(ImmutableMap.<String, Long>builder()
                    .put("one", 1L)
                    .put("two", 2L)
                    .put("three", 3L)
                    .put("ten", 10L)
                    .put("eleven", 11L)
                    .put("twelve", 12L)
                    .buildOrThrow());

            assertThat(cursor.getReadTimeNanos()).isPositive();
        }
    }

    @Test
    public void testCursorMixedOrder()
    {
        RecordSet recordSet = createRecordSet(ImmutableList.of(
                columnHandles.get("value"),
                columnHandles.get("value"),
                columnHandles.get("text")));

        try (RecordCursor cursor = recordSet.cursor()) {
            assertThat(cursor.getType(0)).isEqualTo(BIGINT);
            assertThat(cursor.getType(1)).isEqualTo(BIGINT);
            assertThat(cursor.getType(2)).isEqualTo(VARCHAR);

            Map<String, Long> data = new LinkedHashMap<>();
            while (cursor.advanceNextPosition()) {
                assertThat(cursor.getLong(0)).isEqualTo(cursor.getLong(1));
                data.put(cursor.getSlice(2).toStringUtf8(), cursor.getLong(0));
            }

            assertThat(data).isEqualTo(ImmutableMap.<String, Long>builder()
                    .put("one", 1L)
                    .put("two", 2L)
                    .put("three", 3L)
                    .put("ten", 10L)
                    .put("eleven", 11L)
                    .put("twelve", 12L)
                    .buildOrThrow());

            assertThat(cursor.getReadTimeNanos()).isPositive();
        }
    }

    @Test
    public void testIdempotentClose()
    {
        RecordSet recordSet = createRecordSet(ImmutableList.of(
                columnHandles.get("value"),
                columnHandles.get("value"),
                columnHandles.get("text")));

        RecordCursor cursor = recordSet.cursor();
        cursor.close();
        cursor.close();
    }

    private JdbcRecordSet createRecordSet(List<JdbcColumnHandle> columnHandles)
    {
        return new JdbcRecordSet(jdbcClient, executor, SESSION, split, table, columnHandles);
    }
}
