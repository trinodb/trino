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
package io.trino.plugin.doris;

import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

@Execution(ExecutionMode.SAME_THREAD)
final class TestDorisConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    private final TestingDorisEnvironment environment = new TestingDorisEnvironment();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DorisQueryRunner.builder(environment).build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_ADD_COLUMN,
                 SUPPORTS_ARRAY,
                 SUPPORTS_COMMENT_ON_COLUMN,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_CREATE_TABLE,
                 SUPPORTS_CREATE_VIEW,
                 SUPPORTS_DELETE,
                 SUPPORTS_DYNAMIC_FILTER_PUSHDOWN,
                 SUPPORTS_INSERT,
                 SUPPORTS_JOIN_PUSHDOWN,
                 SUPPORTS_LIMIT_PUSHDOWN,
                 SUPPORTS_MAP_TYPE,
                 SUPPORTS_MERGE,
                 SUPPORTS_NATIVE_QUERY,
                 SUPPORTS_NOT_NULL_CONSTRAINT,
                 SUPPORTS_PREDICATE_PUSHDOWN,
                 SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN,
                 SUPPORTS_RENAME_COLUMN,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_ROW_TYPE,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_AGGREGATION_PUSHDOWN,
                 SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT,
                 SUPPORTS_TOPN_PUSHDOWN,
                 SUPPORTS_TOPN_PUSHDOWN_WITH_VARCHAR,
                 SUPPORTS_UPDATE,
                 SUPPORTS_AGGREGATION_PUSHDOWN_CORRELATION,
                 SUPPORTS_AGGREGATION_PUSHDOWN_COVARIANCE,
                 SUPPORTS_AGGREGATION_PUSHDOWN_REGRESSION,
                 SUPPORTS_AGGREGATION_PUSHDOWN_STDDEV,
                 SUPPORTS_AGGREGATION_PUSHDOWN_VARIANCE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    void testBasicRead()
    {
        assertQuery(
                "SELECT nationkey, name FROM nation WHERE nationkey <= 2 ORDER BY nationkey",
                "VALUES (0, 'ALGERIA'), (1, 'ARGENTINA'), (2, 'BRAZIL')");
    }

    @Test
    void testCharacterCountDistinctQueryStaysCorrect()
    {
        assertQuery("SELECT count(DISTINCT orderstatus), sum(shippriority) FROM orders", "VALUES (3, 0)");
    }

    @Test
    void testShowColumnsShape()
    {
        assertQuery(
                "SHOW COLUMNS FROM orders",
                """
                VALUES
                    ('orderkey', 'bigint', '', ''),
                    ('custkey', 'bigint', '', ''),
                    ('orderstatus', 'varchar(1)', '', ''),
                    ('totalprice', 'double', '', ''),
                    ('orderdate', 'date', '', ''),
                    ('orderpriority', 'varchar(15)', '', ''),
                    ('clerk', 'varchar(15)', '', ''),
                    ('shippriority', 'integer', '', ''),
                    ('comment', 'varchar(79)', '', '')
                """);
    }

    @Test
    void testColumnsInReverseOrder()
    {
        assertQuery("SELECT shippriority, clerk, totalprice FROM orders");
    }

    @Test
    void testUnsupportedPredicateTypeStaysCorrect()
    {
        assertQuery(
                "SELECT orderkey FROM orders WHERE orderkey IN (1, 2, 3) AND totalprice > 200000 ORDER BY orderkey",
                "VALUES (3)");
    }
}
