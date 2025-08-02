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
package io.trino.plugin.mysql;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestMySqlConnectorTest
        extends BaseMySqlConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        mySqlServer = closeAfterClass(new TestingMySqlServer(false));
        return MySqlQueryRunner.builder(mySqlServer)
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    protected void verifyColumnNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching("(Incorrect column name '.*'|Identifier name '.*' is too long)");
    }

    @Test
    public void testJoinPushdownWithImplicitCast()
    {
        try (TestTable leftTable = new TestTable(
                getQueryRunner()::execute,
                "left_table_",
                "(id int, c_tinyint tinyint, c_smallint smallint, c_integer int, c_bigint bigint, c_real real, c_double double precision, c_decimal_10_0 decimal(10, 0), c_decimal_10_2 decimal(10, 2))",
                ImmutableList.of(
                        "(11, 12, 12, 12, 12, 12.34, 12.34, 12.0, 12.34)",
                        "(12, 123, 123, 123, 123, 123.67, 123.67, 123.0, 123.67)"));
                TestTable rightTable = new TestTable(
                        getQueryRunner()::execute,
                        "right_table_",
                        "(id int, c_tinyint tinyint, c_smallint smallint, c_integer int, c_bigint bigint, c_real real, c_double double precision, c_decimal_10_0 decimal(10, 0), c_decimal_10_2 decimal(10, 2))",
                        ImmutableList.of(
                                "(21, 12, 12, 12, 12, 12.34, 12.34, 12.0, 12.34)",
                                "(22, 127, 234, 234, 234, 234.67, 234.67, 123.0, 123.67)"))) {
            Session session = joinPushdownEnabled(getSession());
            String joinQuery = "SELECT l.id FROM " + leftTable.getName() + " l %s " + rightTable.getName() + " r ON %s";

            // Cast type 'double' is supported in MySQL
            assertThat(query(session, joinQuery.formatted("LEFT JOIN", "l.c_tinyint = r.c_double")))
                    .isFullyPushedDown();
            assertThat(query(session, joinQuery.formatted("RIGHT JOIN", "l.c_tinyint = r.c_double")))
                    .isFullyPushedDown();
            assertThat(query(session, joinQuery.formatted("INNER JOIN", "l.c_tinyint = r.c_double")))
                    .isFullyPushedDown();
            // Full Join pushdown is not supported
            assertThat(query(session, joinQuery.formatted("FULL JOIN", "l.c_tinyint = r.c_double")))
                    .joinIsNotFullyPushedDown();

            // Cast type 'double' is supported in MySQL
            assertThat(query(session, joinQuery.formatted("LEFT JOIN", "l.c_smallint = r.c_double")))
                    .isFullyPushedDown();
            assertThat(query(session, joinQuery.formatted("RIGHT JOIN", "l.c_smallint = r.c_double")))
                    .isFullyPushedDown();
            assertThat(query(session, joinQuery.formatted("INNER JOIN", "l.c_smallint = r.c_double")))
                    .isFullyPushedDown();
            // Full Join pushdown is not supported
            assertThat(query(session, joinQuery.formatted("FULL JOIN", "l.c_smallint = r.c_double")))
                    .joinIsNotFullyPushedDown();

            // Cast type 'double' is supported in MySQL
            assertThat(query(session, joinQuery.formatted("LEFT JOIN", "l.c_integer = r.c_double")))
                    .isFullyPushedDown();
            assertThat(query(session, joinQuery.formatted("RIGHT JOIN", "l.c_integer = r.c_double")))
                    .isFullyPushedDown();
            assertThat(query(session, joinQuery.formatted("INNER JOIN", "l.c_integer = r.c_double")))
                    .isFullyPushedDown();
            // Full Join pushdown is not supported
            assertThat(query(session, joinQuery.formatted("FULL JOIN", "l.c_integer = r.c_double")))
                    .joinIsNotFullyPushedDown();

            // Cast type 'double' is supported in MySQL
            assertThat(query(session, joinQuery.formatted("LEFT JOIN", "l.c_bigint = r.c_double")))
                    .isFullyPushedDown();
            assertThat(query(session, joinQuery.formatted("RIGHT JOIN", "l.c_bigint = r.c_double")))
                    .isFullyPushedDown();
            assertThat(query(session, joinQuery.formatted("INNER JOIN", "l.c_bigint = r.c_double")))
                    .isFullyPushedDown();
            // Full Join pushdown is not supported
            assertThat(query(session, joinQuery.formatted("FULL JOIN", "l.c_bigint = r.c_double")))
                    .joinIsNotFullyPushedDown();

            // Cast type 'decimal' is supported in MySQL
            assertThat(query(session, joinQuery.formatted("LEFT JOIN", "l.c_tinyint = r.c_decimal_10_0")))
                    .isFullyPushedDown();
            assertThat(query(session, joinQuery.formatted("RIGHT JOIN", "l.c_tinyint = r.c_decimal_10_0")))
                    .isFullyPushedDown();
            assertThat(query(session, joinQuery.formatted("INNER JOIN", "l.c_tinyint = r.c_decimal_10_0")))
                    .isFullyPushedDown();
            assertThat(query(session, joinQuery.formatted("FULL JOIN", "l.c_tinyint = r.c_decimal_10_0")))
                    .joinIsNotFullyPushedDown();

            // Cast type 'decimal' is supported in MySQL
            assertThat(query(session, joinQuery.formatted("LEFT JOIN", "l.c_smallint = r.c_decimal_10_0")))
                    .isFullyPushedDown();
            assertThat(query(session, joinQuery.formatted("RIGHT JOIN", "l.c_smallint = r.c_decimal_10_0")))
                    .isFullyPushedDown();
            assertThat(query(session, joinQuery.formatted("INNER JOIN", "l.c_smallint = r.c_decimal_10_0")))
                    .isFullyPushedDown();
            assertThat(query(session, joinQuery.formatted("FULL JOIN", "l.c_smallint = r.c_decimal_10_0")))
                    .joinIsNotFullyPushedDown();

            // Cast type 'decimal' is supported in MySQL
            assertThat(query(session, joinQuery.formatted("LEFT JOIN", "l.c_integer = r.c_decimal_10_2")))
                    .isFullyPushedDown();
            assertThat(query(session, joinQuery.formatted("RIGHT JOIN", "l.c_integer = r.c_decimal_10_2")))
                    .isFullyPushedDown();
            assertThat(query(session, joinQuery.formatted("INNER JOIN", "l.c_integer = r.c_decimal_10_2")))
                    .isFullyPushedDown();
            assertThat(query(session, joinQuery.formatted("FULL JOIN", "l.c_integer = r.c_decimal_10_2")))
                    .joinIsNotFullyPushedDown();

            // Cast type 'decimal' is supported in MySQL
            assertThat(query(session, joinQuery.formatted("LEFT JOIN", "l.c_bigint = r.c_decimal_10_2")))
                    .isFullyPushedDown();
            assertThat(query(session, joinQuery.formatted("RIGHT JOIN", "l.c_bigint = r.c_decimal_10_2")))
                    .isFullyPushedDown();
            assertThat(query(session, joinQuery.formatted("INNER JOIN", "l.c_bigint = r.c_decimal_10_2")))
                    .isFullyPushedDown();
            assertThat(query(session, joinQuery.formatted("FULL JOIN", "l.c_bigint = r.c_decimal_10_2")))
                    .joinIsNotFullyPushedDown();

            // Cast type 'decimal' is supported in MySQL
            assertThat(query(session, joinQuery.formatted("LEFT JOIN", "l.c_double = r.c_decimal_10_0")))
                    .isFullyPushedDown();
            assertThat(query(session, joinQuery.formatted("RIGHT JOIN", "l.c_double = r.c_decimal_10_0")))
                    .isFullyPushedDown();
            assertThat(query(session, joinQuery.formatted("INNER JOIN", "l.c_double = r.c_decimal_10_0")))
                    .isFullyPushedDown();
            assertThat(query(session, joinQuery.formatted("FULL JOIN", "l.c_double = r.c_decimal_10_0")))
                    .joinIsNotFullyPushedDown();

            // Unsupported pushdowns
            // Cast type 'bigint' is not supported (https://dev.mysql.com/doc/refman/8.0/en/cast-functions.html#cast-function-descriptions)
            assertThat(query(session, joinQuery.formatted("LEFT JOIN", "l.c_tinyint = r.c_bigint")))
                    .joinIsNotFullyPushedDown();
            assertThat(query(session, joinQuery.formatted("RIGHT JOIN", "l.c_smallint = r.c_bigint")))
                    .joinIsNotFullyPushedDown();
            assertThat(query(session, joinQuery.formatted("INNER JOIN", "l.c_integer = r.c_bigint")))
                    .joinIsNotFullyPushedDown();
            assertThat(query(session, joinQuery.formatted("FULL JOIN", "l.id = r.c_bigint")))
                    .joinIsNotFullyPushedDown();

            // Cast type 'integer' is not supported (https://dev.mysql.com/doc/refman/8.0/en/cast-functions.html#cast-function-descriptions)
            assertThat(query(session, joinQuery.formatted("LEFT JOIN", "l.c_tinyint = r.c_integer")))
                    .joinIsNotFullyPushedDown();
            assertThat(query(session, joinQuery.formatted("RIGHT JOIN", "l.c_smallint = r.c_integer")))
                    .joinIsNotFullyPushedDown();
            assertThat(query(session, joinQuery.formatted("INNER JOIN", "l.c_bigint = r.c_integer")))
                    .joinIsNotFullyPushedDown();
            assertThat(query(session, joinQuery.formatted("FULL JOIN", "l.id = r.c_integer")))
                    .joinIsNotFullyPushedDown();

            // Cast type 'smallint' is not supported (https://dev.mysql.com/doc/refman/8.0/en/cast-functions.html#cast-function-descriptions)
            assertThat(query(session, joinQuery.formatted("LEFT JOIN", "l.c_tinyint = r.c_smallint")))
                    .joinIsNotFullyPushedDown();
            assertThat(query(session, joinQuery.formatted("RIGHT JOIN", "l.c_integer = r.c_smallint")))
                    .joinIsNotFullyPushedDown();
            assertThat(query(session, joinQuery.formatted("INNER JOIN", "l.c_bigint = r.c_smallint")))
                    .joinIsNotFullyPushedDown();
            assertThat(query(session, joinQuery.formatted("FULL JOIN", "l.id = r.c_smallint")))
                    .joinIsNotFullyPushedDown();
        }
    }
}
