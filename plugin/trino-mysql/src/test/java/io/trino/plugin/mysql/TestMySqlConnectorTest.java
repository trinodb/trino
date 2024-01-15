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

import com.google.common.collect.ImmutableMap;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.mysql.MySqlQueryRunner.createMySqlQueryRunner;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMySqlConnectorTest
        extends BaseMySqlConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        mySqlServer = closeAfterClass(new TestingMySqlServer(false));
        return createMySqlQueryRunner(mySqlServer, ImmutableMap.of(), ImmutableMap.of(), REQUIRED_TPCH_TABLES);
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN -> {
                // TODO remove once super has this set to true
                verify(!super.hasBehavior(connectorBehavior));
                yield true;
            }
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected void verifyColumnNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching("(Incorrect column name '.*'|Identifier name '.*' is too long)");
    }

    @Test
    public void testIsNullPredicatePushdown()
    {
        assertThat(query("SELECT nationkey FROM nation WHERE name IS NULL")).isFullyPushedDown();
        assertThat(query("SELECT nationkey FROM nation WHERE name IS NULL OR regionkey = 4")).isFullyPushedDown();

        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_is_null_predicate_pushdown",
                "(a_int integer, a_varchar varchar(1))",
                List.of(
                        "1, 'A'",
                        "2, 'B'",
                        "1, NULL",
                        "2, NULL"))) {
            assertThat(query("SELECT a_int FROM " + table.getName() + " WHERE a_varchar IS NULL OR a_int = 1")).isFullyPushedDown();
        }
    }

    @Test
    public void testIsNotNullPredicatePushdown()
    {
        assertThat(query("SELECT nationkey FROM nation WHERE name IS NOT NULL OR regionkey = 4")).isFullyPushedDown();

        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_is_not_null_predicate_pushdown",
                "(a_int integer, a_varchar varchar(1))",
                List.of(
                        "1, 'A'",
                        "2, 'B'",
                        "1, NULL",
                        "2, NULL"))) {
            assertThat(query("SELECT a_int FROM " + table.getName() + " WHERE a_varchar IS NOT NULL OR a_int = 1")).isFullyPushedDown();
        }
    }

    @Test
    public void testNullIfPredicatePushdown()
    {
        assertThat(query("SELECT nationkey FROM nation WHERE NULLIF(name, 'ALGERIA') IS NULL"))
                .matches("VALUES BIGINT '0'")
                .isFullyPushedDown();

        assertThat(query("SELECT name FROM nation WHERE NULLIF(nationkey, 0) IS NULL"))
                .matches("VALUES CAST('ALGERIA' AS varchar(255))")
                .isFullyPushedDown();

        assertThat(query("SELECT nationkey FROM nation WHERE NULLIF(name, 'Algeria') IS NULL"))
                .returnsEmptyResult()
                .isFullyPushedDown();

        // NULLIF returns the first argument because arguments aren't the same
        assertThat(query("SELECT nationkey FROM nation WHERE NULLIF(name, 'Name not found') = name"))
                .matches("SELECT nationkey FROM nation")
                .isFullyPushedDown();
    }
}
