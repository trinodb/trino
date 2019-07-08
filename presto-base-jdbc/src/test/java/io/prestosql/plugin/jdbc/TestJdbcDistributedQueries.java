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
import io.airlift.tpch.TpchTable;
import io.prestosql.Session;
import io.prestosql.tests.AbstractTestQueries;
import io.prestosql.tests.sql.JdbcSqlExecutor;
import io.prestosql.tests.sql.TestTable;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static io.prestosql.plugin.jdbc.BaseJdbcSessionProperties.UNSUPPORTED_TYPE_HANDLING_STRATEGY;
import static io.prestosql.plugin.jdbc.JdbcQueryRunner.createJdbcQueryRunner;
import static io.prestosql.plugin.jdbc.UnsupportedTypeHandlingStrategy.IGNORE;
import static io.prestosql.tests.sql.TestTable.TABLE_NAME_PLACEHOLDER;
import static java.lang.String.format;

public class TestJdbcDistributedQueries
        extends AbstractTestQueries
{
    private final Map<String, String> properties;

    public TestJdbcDistributedQueries()
    {
        this(TestingH2JdbcModule.createProperties());
    }

    public TestJdbcDistributedQueries(Map<String, String> properties)
    {
        super(() -> createJdbcQueryRunner(ImmutableList.copyOf(TpchTable.getTables()), properties));
        this.properties = properties;
    }

    @Override
    public void testLargeIn()
    {
    }

    @Test
    public void testFailureOnUnknown()
    {
        try (TestTable table = new TestTable(
                getSqlExecutor(),
                "tpch.test_failure_on_unknown_type",
                format("CREATE TABLE %s (i int, x GEOMETRY)", TABLE_NAME_PLACEHOLDER),
                Optional.of("(1, 'POINT(7 52)')"))) {
            assertQueryFails("SELECT * FROM " + table.getName(), "Unsupported data type for column: X");
            assertQuery(onUnsupportedType(IGNORE), "SELECT * FROM " + table.getName(), "VALUES 1");
        }
    }

    private Session onUnsupportedType(UnsupportedTypeHandlingStrategy unsupportedTypeHandlingStrategy)
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("jdbc", UNSUPPORTED_TYPE_HANDLING_STRATEGY, unsupportedTypeHandlingStrategy.name())
                .build();
    }

    private JdbcSqlExecutor getSqlExecutor()
    {
        return new JdbcSqlExecutor(properties.get("connection-url"), new Properties());
    }
}
