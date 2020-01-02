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
package io.prestosql.plugin.cassandra.util;

import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.cassandra.CassandraColumnHandle;
import io.prestosql.plugin.cassandra.CassandraType;
import org.testng.annotations.Test;

import java.util.List;

import static io.prestosql.plugin.cassandra.util.CassandraCqlUtils.appendSelectColumns;
import static io.prestosql.plugin.cassandra.util.CassandraCqlUtils.quoteStringLiteral;
import static io.prestosql.plugin.cassandra.util.CassandraCqlUtils.quoteStringLiteralForJson;
import static io.prestosql.plugin.cassandra.util.CassandraCqlUtils.validColumnName;
import static io.prestosql.plugin.cassandra.util.CassandraCqlUtils.validSchemaName;
import static io.prestosql.plugin.cassandra.util.CassandraCqlUtils.validTableName;
import static org.testng.Assert.assertEquals;

public class TestCassandraCqlUtils
{
    @Test
    public void testValidSchemaName()
    {
        assertEquals(validSchemaName("foo"), "foo");
        assertEquals(validSchemaName("select"), "\"select\"");
    }

    @Test
    public void testValidTableName()
    {
        assertEquals(validTableName("foo"), "foo");
        assertEquals(validTableName("Foo"), "\"Foo\"");
        assertEquals(validTableName("select"), "\"select\"");
    }

    @Test
    public void testValidColumnName()
    {
        assertEquals(validColumnName("foo"), "foo");
        assertEquals(validColumnName(CassandraCqlUtils.EMPTY_COLUMN_NAME), "\"\"");
        assertEquals(validColumnName(""), "\"\"");
        assertEquals(validColumnName("select"), "\"select\"");
    }

    @Test
    public void testQuote()
    {
        assertEquals(quoteStringLiteral("foo"), "'foo'");
        assertEquals(quoteStringLiteral("Presto's"), "'Presto''s'");
    }

    @Test
    public void testQuoteJson()
    {
        assertEquals(quoteStringLiteralForJson("foo"), "\"foo\"");
        assertEquals(quoteStringLiteralForJson("Presto's"), "\"Presto's\"");
        assertEquals(quoteStringLiteralForJson("xx\"xx"), "\"xx\\\"xx\"");
    }

    @Test
    public void testAppendSelectColumns()
    {
        List<CassandraColumnHandle> columns = ImmutableList.of(
                new CassandraColumnHandle("foo", 0, CassandraType.VARCHAR, false, false, false, false),
                new CassandraColumnHandle("bar", 0, CassandraType.VARCHAR, false, false, false, false),
                new CassandraColumnHandle("table", 0, CassandraType.VARCHAR, false, false, false, false));

        StringBuilder sb = new StringBuilder();
        appendSelectColumns(sb, columns);
        String str = sb.toString();

        assertEquals(str, "foo,bar,\"table\"");
    }
}
