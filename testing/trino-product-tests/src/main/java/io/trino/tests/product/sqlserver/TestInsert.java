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
package io.trino.tests.product.sqlserver;

import io.trino.tempto.AfterMethodWithContext;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.Requirement;
import io.trino.tempto.RequirementsProvider;
import io.trino.tempto.configuration.Configuration;
import io.trino.tempto.query.QueryResult;
import org.testng.annotations.Test;

import java.sql.Date;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.fulfillment.table.TableRequirements.immutableTable;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.TestGroups.SQL_SERVER;
import static io.trino.tests.product.sqlserver.SqlServerDataTypesTableDefinition.SQLSERVER_INSERT;
import static io.trino.tests.product.sqlserver.TestConstants.KEY_SPACE;
import static io.trino.tests.product.utils.QueryExecutors.onSqlServer;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestInsert
        extends ProductTest
        implements RequirementsProvider
{
    @Override
    public Requirement getRequirements(Configuration configuration)
    {
        return immutableTable(SQLSERVER_INSERT);
    }

    private static final String SQLSERVER = "sqlserver";
    private static final String MASTER = "master";
    private static final String INSERT_TABLE_NAME = format("%s.%s", KEY_SPACE, SQLSERVER_INSERT.getName());

    @BeforeMethodWithContext
    @AfterMethodWithContext
    public void dropTestTables()
    {
        onTrino().executeQuery(format("DROP TABLE IF EXISTS %s", INSERT_TABLE_NAME));
    }

    @Test(groups = {SQL_SERVER, PROFILE_SPECIFIC_TESTS})
    public void testInsertMin()
    {
        String sql = format(
                "INSERT INTO %s.%s values (BIGINT '%s', SMALLINT '%s', INTEGER '%s', DOUBLE '%s', " +
                        "CHAR 'a   ', 'aa', DOUBLE '%s', DATE '%s')",
                SQLSERVER, INSERT_TABLE_NAME, Long.MIN_VALUE, Short.MIN_VALUE, Integer.MIN_VALUE,
                Double.MIN_VALUE, Double.MIN_VALUE, Date.valueOf("1970-01-01"));
        onTrino().executeQuery(sql);

        sql = format(
                "SELECT * FROM %s.%s",
                MASTER, INSERT_TABLE_NAME);
        QueryResult queryResult = onSqlServer()
                .executeQuery(sql);

        assertThat(queryResult).contains(
                row(Long.MIN_VALUE, Short.MIN_VALUE, Integer.MIN_VALUE, Double.MIN_VALUE, "a   ", "aa", Double.MIN_VALUE, Date.valueOf("1970-01-01")));
    }

    @Test(groups = {SQL_SERVER, PROFILE_SPECIFIC_TESTS})
    public void testInsertMax()
    {
        String sql = format(
                "INSERT INTO %s.%s values (BIGINT '%s', SMALLINT '%s', INTEGER '%s', DOUBLE '%s', " +
                        "CHAR 'aaaa', 'aaaaaa', DOUBLE '%s', DATE '%s' )",
                SQLSERVER, INSERT_TABLE_NAME, Long.MAX_VALUE, Short.MAX_VALUE, Integer.MAX_VALUE,
                Double.MAX_VALUE, Double.valueOf("12345678912.3456756"), Date.valueOf("9999-12-31"));
        onTrino().executeQuery(sql);

        sql = format(
                "SELECT * FROM %s.%s",
                MASTER, INSERT_TABLE_NAME);
        QueryResult queryResult = onSqlServer()
                .executeQuery(sql);

        assertThat(queryResult).contains(
                row(Long.MAX_VALUE, Short.MAX_VALUE, Integer.MAX_VALUE, Double.MAX_VALUE, "aaaa", "aaaaaa", Double.valueOf("12345678912.3456756"),
                        Date.valueOf("9999-12-31")));
    }

    @Test(groups = {SQL_SERVER, PROFILE_SPECIFIC_TESTS})
    public void testInsertNull()
    {
        String sql = format(
                "INSERT INTO %s.%s values (null, null, null, null, null, null, null, null)",
                SQLSERVER, INSERT_TABLE_NAME);
        onTrino().executeQuery(sql);

        sql = format(
                "SELECT * FROM %s.%s",
                MASTER, INSERT_TABLE_NAME);
        QueryResult queryResult = onSqlServer()
                .executeQuery(sql);

        assertThat(queryResult).contains(row(null, null, null, null, null, null, null, null));
    }
}
