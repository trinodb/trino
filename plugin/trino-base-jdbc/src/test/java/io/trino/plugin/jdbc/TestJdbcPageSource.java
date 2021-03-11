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
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static io.trino.operator.PageAssertions.assertPageEquals;
import static io.trino.plugin.jdbc.TestJdbcPageSourceProvider.append;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingConnectorSession.SESSION;

public class TestJdbcPageSource
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
    public void testPageSimple()
    {
        try (JdbcPageSource pageSource = createPageSource(ImmutableList.of(
                columnHandles.get("text"),
                columnHandles.get("text_short"),
                columnHandles.get("value")))) {
            Page page = pageSource.getNextPage();

            List<Type> types = ImmutableList.of(VARCHAR, VARCHAR, BIGINT);
            PageBuilder expected = new PageBuilder(types);
            append(expected, types, "one", "one", 1);
            append(expected, types, "two", "two", 2);
            append(expected, types, "three", "three", 3);
            append(expected, types, "ten", "ten", 10);
            append(expected, types, "eleven", "eleven", 11);
            append(expected, types, "twelve", "twelve", 12);

            assertPageEquals(types, page, expected.build());
        }
    }

    @Test
    public void testCursorMixedOrder()
    {
        try (JdbcPageSource pageSource = createPageSource(ImmutableList.of(
                columnHandles.get("value"),
                columnHandles.get("value"),
                columnHandles.get("text")))) {
            Page page = pageSource.getNextPage();

            List<Type> types = ImmutableList.of(BIGINT, BIGINT, VARCHAR);
            PageBuilder expected = new PageBuilder(types);
            append(expected, types, 1, 1, "one");
            append(expected, types, 2, 2, "two");
            append(expected, types, 3, 3, "three");
            append(expected, types, 10, 10, "ten");
            append(expected, types, 11, 11, "eleven");
            append(expected, types, 12, 12, "twelve");

            assertPageEquals(types, page, expected.build());
        }
    }

    @Test
    public void testIdempotentClose()
    {
        JdbcPageSource pageSource = createPageSource(ImmutableList.of(
                columnHandles.get("value"),
                columnHandles.get("value"),
                columnHandles.get("text")));

        pageSource.getNextPage();
        pageSource.close();
        pageSource.close();
    }

    private JdbcPageSource createPageSource(List<JdbcColumnHandle> columnHandles)
    {
        return new JdbcPageSource(jdbcClient, SESSION, split, table, columnHandles);
    }
}
