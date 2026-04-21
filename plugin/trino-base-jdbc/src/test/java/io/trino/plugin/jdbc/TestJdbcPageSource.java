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
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SourcePage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.airlift.testing.Closeables.closeAll;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestJdbcPageSource
{
    private TestingDatabase database;
    private JdbcClient jdbcClient;
    private JdbcTableHandle table;
    private JdbcSplit split;
    private Map<String, JdbcColumnHandle> columnHandles;
    private ExecutorService executor;

    @BeforeAll
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

    @AfterAll
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
    public void testCursorSimple()
    {
        try (JdbcPageSource pageSource = createPageSource(ImmutableList.of(
                columnHandles.get("text"),
                columnHandles.get("text_short"),
                columnHandles.get("value")))) {
            assertThat(pageSource.isBlocked().isDone()).isTrue();
            Map<String, Long> data = new LinkedHashMap<>();
            for (SourcePage page = pageSource.getNextSourcePage(); ; page = pageSource.getNextSourcePage()) {
                if (page == null) {
                    continue;
                }
                for (int position = 0; position < page.getPositionCount(); position++) {
                    assertThat(page.getBlock(0).isNull(position)).isFalse();
                    assertThat(page.getBlock(1).isNull(position)).isFalse();
                    assertThat(page.getBlock(2).isNull(position)).isFalse();
                    assertThat(VARCHAR.getSlice(page.getBlock(0), position))
                            .isEqualTo(VARCHAR.getSlice(page.getBlock(1), position));
                    data.put(
                            VARCHAR.getSlice(page.getBlock(0), position).toStringUtf8(),
                            BIGINT.getLong(page.getBlock(2), position));
                }

                if (pageSource.isFinished()) {
                    break;
                }
            }

            assertThat(data).isEqualTo(ImmutableMap.<String, Long>builder()
                    .put("one", 1L)
                    .put("two", 2L)
                    .put("three", 3L)
                    .put("ten", 10L)
                    .put("eleven", 11L)
                    .put("twelve", 12L)
                    .buildOrThrow());

            assertThat(pageSource.getReadTimeNanos()).isPositive();
        }
    }

    @Test
    public void testCursorMixedOrder()
    {
        try (JdbcPageSource pageSource = createPageSource(ImmutableList.of(
                columnHandles.get("value"),
                columnHandles.get("value"),
                columnHandles.get("text")))) {
            Map<String, Long> data = new LinkedHashMap<>();
            for (SourcePage page = pageSource.getNextSourcePage(); ; page = pageSource.getNextSourcePage()) {
                if (page == null) {
                    continue;
                }
                for (int position = 0; position < page.getPositionCount(); position++) {
                    assertThat(BIGINT.getLong(page.getBlock(0), position))
                            .isEqualTo(BIGINT.getLong(page.getBlock(1), position));
                    data.put(
                            VARCHAR.getSlice(page.getBlock(2), position).toStringUtf8(),
                            BIGINT.getLong(page.getBlock(0), position));
                }

                if (pageSource.isFinished()) {
                    break;
                }
            }

            assertThat(data).isEqualTo(ImmutableMap.<String, Long>builder()
                    .put("one", 1L)
                    .put("two", 2L)
                    .put("three", 3L)
                    .put("ten", 10L)
                    .put("eleven", 11L)
                    .put("twelve", 12L)
                    .buildOrThrow());

            assertThat(pageSource.getReadTimeNanos()).isPositive();
        }
    }

    @Test
    public void testIdempotentClose()
    {
        JdbcPageSource pageSource = createPageSource(ImmutableList.of(
                columnHandles.get("value"),
                columnHandles.get("value"),
                columnHandles.get("text")));

        pageSource.close();
        pageSource.close();
    }

    @Test
    public void testGetNextPageAfterClose()
    {
        JdbcPageSource pageSource = createPageSource(ImmutableList.of(
                columnHandles.get("value"),
                columnHandles.get("value"),
                columnHandles.get("text")));

        pageSource.close();
        assertThatThrownBy(pageSource::getNextSourcePage)
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("page source is closed");
        assertThat(pageSource.isBlocked().isDone()).isTrue();
    }

    @Test
    public void testGetNextPageAfterFinish()
    {
        JdbcPageSource pageSource = createPageSource(ImmutableList.of(
                columnHandles.get("value"),
                columnHandles.get("value"),
                columnHandles.get("text")));

        while (!pageSource.isFinished()) {
            pageSource.getNextSourcePage();
        }
        assertThat(pageSource.getNextSourcePage()).isNull();
        assertThat(pageSource.isBlocked().isDone()).isTrue();
    }

    private JdbcPageSource createPageSource(List<JdbcColumnHandle> columnHandles)
    {
        return new JdbcPageSource(jdbcClient, executor, SESSION, split, table, columnHandles);
    }
}
