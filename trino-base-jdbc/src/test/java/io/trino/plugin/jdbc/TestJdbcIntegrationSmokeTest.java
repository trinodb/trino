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
import io.prestosql.Session;
import io.prestosql.testing.AbstractTestIntegrationSmokeTest;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.sql.JdbcSqlExecutor;
import io.prestosql.testing.sql.TestTable;
import io.prestosql.tpch.TpchTable;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Properties;

import static io.prestosql.plugin.jdbc.H2QueryRunner.createH2QueryRunner;
import static io.prestosql.plugin.jdbc.TypeHandlingJdbcSessionProperties.UNSUPPORTED_TYPE_HANDLING;
import static io.prestosql.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.prestosql.plugin.jdbc.UnsupportedTypeHandling.IGNORE;
import static java.lang.String.format;

public class TestJdbcIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private final Map<String, String> properties = TestingH2JdbcModule.createProperties();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createH2QueryRunner(ImmutableList.copyOf(TpchTable.getTables()), properties);
    }

    @Test
    public void testUnknownTypeAsIgnored()
    {
        try (TestTable table = new TestTable(
                getSqlExecutor(),
                "tpch.test_failure_on_unknown_type_as_ignored",
                "(int_column int, geometry_column GEOMETRY)",
                ImmutableList.of(
                        "1, NULL",
                        "2, 'POINT(7 52)'"))) {
            Session ignoreUnsupportedType = unsupportedTypeHandling(IGNORE);
            assertQuery(ignoreUnsupportedType, "SELECT int_column FROM " + table.getName(), "VALUES 1, 2");
            assertQuery(ignoreUnsupportedType, "SELECT * FROM " + table.getName(), "VALUES 1, 2");
            assertQuery(
                    ignoreUnsupportedType,
                    "SELECT column_name, data_type FROM information_schema.columns WHERE table_name LIKE 'test_failure_on_unknown_type_as_ignored%'",
                    "VALUES ('int_column', 'integer')");
            assertQuery(
                    ignoreUnsupportedType,
                    "DESCRIBE " + table.getName(),
                    "VALUES ('int_column', 'integer', '', '')");

            assertUpdate(ignoreUnsupportedType, format("INSERT INTO %s (int_column) VALUES (3)", table.getName()), 1);
            assertQuery(ignoreUnsupportedType, "SELECT * FROM " + table.getName(), "VALUES 1, 2, 3");
        }
    }

    @Test
    public void testUnknownTypeAsVarchar()
    {
        try (TestTable table = new TestTable(
                getSqlExecutor(),
                "tpch.test_failure_on_unknown_type_as_varchar",
                "(int_column int, geometry_column GEOMETRY)",
                ImmutableList.of(
                        "1, NULL",
                        "2, 'POINT(7 52)'"))) {
            Session convertToVarcharUnsupportedTypes = unsupportedTypeHandling(CONVERT_TO_VARCHAR);
            assertQuery(convertToVarcharUnsupportedTypes, "SELECT int_column FROM " + table.getName(), "VALUES 1, 2");
            assertQuery(convertToVarcharUnsupportedTypes, "SELECT * FROM " + table.getName(), "VALUES (1, NULL), (2, 'POINT (7 52)')");

            // predicate pushdown
            assertQuery(
                    convertToVarcharUnsupportedTypes,
                    format("SELECT int_column FROM %s WHERE geometry_column = 'POINT (7 52)'", table.getName()),
                    "VALUES 2");
            assertQuery(
                    convertToVarcharUnsupportedTypes,
                    format("SELECT int_column FROM %s WHERE geometry_column = 'invalid data'", table.getName()),
                    "SELECT 1 WHERE false");

            assertQuery(
                    convertToVarcharUnsupportedTypes,
                    "SELECT column_name, data_type FROM information_schema.columns WHERE table_name LIKE 'test_failure_on_unknown_type_as_varchar%'",
                    "VALUES ('int_column', 'integer'), ('geometry_column', 'varchar')");
            assertQuery(
                    convertToVarcharUnsupportedTypes,
                    "DESCRIBE " + table.getName(),
                    "VALUES ('int_column', 'integer', '', ''), ('geometry_column', 'varchar', '','')");

            assertUpdate(
                    convertToVarcharUnsupportedTypes,
                    format("INSERT INTO %s (int_column) VALUES (3)", table.getName()),
                    1);
            assertQueryFails(
                    convertToVarcharUnsupportedTypes,
                    format("INSERT INTO %s (int_column, geometry_column) VALUES (3, 'POINT (7 52)')", table.getName()),
                    "Underlying type that is mapped to VARCHAR is not supported for INSERT: GEOMETRY");

            assertQuery(
                    convertToVarcharUnsupportedTypes,
                    "SELECT * FROM " + table.getName(),
                    "VALUES (1, NULL), (2, 'POINT (7 52)'), (3, NULL)");
        }
    }

    private Session unsupportedTypeHandling(UnsupportedTypeHandling unsupportedTypeHandling)
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("jdbc", UNSUPPORTED_TYPE_HANDLING, unsupportedTypeHandling.name())
                .build();
    }

    private JdbcSqlExecutor getSqlExecutor()
    {
        return new JdbcSqlExecutor(properties.get("connection-url"), new Properties());
    }
}
