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
import io.prestosql.testing.BaseConnectorTest;
import io.prestosql.testing.sql.SqlExecutor;
import io.prestosql.testing.sql.TestTable;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.plugin.jdbc.TypeHandlingJdbcSessionProperties.UNSUPPORTED_TYPE_HANDLING;
import static io.prestosql.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.prestosql.plugin.jdbc.UnsupportedTypeHandling.IGNORE;
import static java.lang.String.format;

public abstract class BaseH2JdbcTest
        extends BaseConnectorTest
{
    protected abstract SqlExecutor getSqlExecutor();

    @Override
    protected boolean supportsDelete()
    {
        return false;
    }

    @Override
    protected boolean supportsViews()
    {
        return false;
    }

    @Override
    protected boolean supportsCommentOnTable()
    {
        throw new SkipException("H2 JDBC connector does not support comments on table");
    }

    @Override
    protected boolean supportsCommentOnColumn()
    {
        throw new SkipException("H2 JDBC connector does not support comments on columnn");
    }

    @Override
    public void testInsertArray()
    {
        throw new SkipException("H2 JDBC connector does not support insert array");
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
            getQueryRunner().execute("CALL system.flush_metadata_cache()");
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
            getQueryRunner().execute("CALL system.flush_metadata_cache()");
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

    @Override
    public void testLargeIn()
    {
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getPrestoTypeName();
        if (typeName.startsWith("time")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                getSqlExecutor(),
                "tpch.table",
                "(col_required BIGINT NOT NULL," +
                        "col_nullable BIGINT," +
                        "col_default BIGINT DEFAULT 43," +
                        "col_nonnull_default BIGINT NOT NULL DEFAULT 42," +
                        "col_required2 BIGINT NOT NULL)");
    }

    private Session unsupportedTypeHandling(UnsupportedTypeHandling unsupportedTypeHandling)
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("jdbc", UNSUPPORTED_TYPE_HANDLING, unsupportedTypeHandling.name())
                .build();
    }
}
