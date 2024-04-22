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
package io.trino.plugin.clickhouse;

import io.trino.testing.QueryRunner;
import io.trino.testing.sql.JdbcSqlExecutor;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.clickhouse.ClickHouseQueryRunner.createClickHouseQueryRunner;
import static io.trino.plugin.clickhouse.TestingClickHouseServer.CLICKHOUSE_LATEST_IMAGE;
import static java.lang.String.format;
import static java.math.RoundingMode.HALF_UP;
import static java.math.RoundingMode.UNNECESSARY;
import static java.util.Arrays.asList;

public class TestClickHouseLatestTypeMapping
        extends BaseClickHouseTypeMapping
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        clickhouseServer = closeAfterClass(new TestingClickHouseServer(CLICKHOUSE_LATEST_IMAGE));
        return createClickHouseQueryRunner(clickhouseServer);
    }

    @Test
    public void testDecimalUnspecifiedPrecisionWithValues()
    {
        JdbcSqlExecutor jse = new JdbcSqlExecutor(clickhouseServer.getJdbcUrl());

        // https://github.com/clickhouse/ClickHouse/pull/53328 restores the support of
        // Decimal with unspecified precision and scale.
        try (TestTable testTable = new TestTable(
                jse,
                "tpch.test_var_decimal",
                "(d_col decimal) Engine=Log",
                asList("1.12", "123456.789", "-1.12", "-123456.789"))) {
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 0),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_name = '%s'", omitDatabasePrefix(testTable.getName())),
                    "VALUES ('d_col','decimal(10,0)')");

            // Excessive digits in a fraction are discarded (not rounded). Excessive digits in integer part will lead to an exception.
            // Danger: Overflow check is not implemented for Decimal128 and Decimal256. In case of overflow incorrect result is returned, no exception is thrown.
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 0),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (1), (123456), (-1), (-123456)");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 0),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (1), (123456), (-1), (-123456)");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 1),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_name = '%s'", omitDatabasePrefix(testTable.getName())),
                    "VALUES ('d_col','decimal(10,0)')");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 1),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (1), (123456), (-1), (-123456)");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(HALF_UP, 2),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (1), (123456), (-1), (-123456)");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 3),
                    format("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tpch' AND table_name = '%s'", omitDatabasePrefix(testTable.getName())),
                    "VALUES ('d_col','decimal(10,0)')");
            assertQuery(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 3),
                    "SELECT d_col FROM " + testTable.getName(),
                    "VALUES (1), (123456), (-1), (-123456)");

            // Check that integer part overflow leads to an exception
            assertQuerySucceeds(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 2),
                    "INSERT INTO " + testTable.getName() + " VALUES (1234567890)");
            assertQueryFails(
                    sessionWithDecimalMappingAllowOverflow(UNNECESSARY, 2),
                    "INSERT INTO " + testTable.getName() + " VALUES (12345678901)",
                    "Cannot cast.*");
        }
    }
}
