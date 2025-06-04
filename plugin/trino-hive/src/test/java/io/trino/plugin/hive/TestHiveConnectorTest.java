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
package io.trino.plugin.hive;

import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;

public class TestHiveConnectorTest
        extends BaseHiveConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createHiveQueryRunner(HiveQueryRunner.builder());
    }

    @Test
    public void testPredicatePushdownWithLambdaExpression()
    {
        String table = "test_predicate_pushdown_" + randomNameSuffix();

        assertUpdate(
                """
                CREATE TABLE %s (v, k)
                WITH (partitioned_by = ARRAY['k'])
                AS (VALUES ('value', 'key'))
                """.formatted(table),
                1);

        try {
            assertQuery(
                    """
                    SELECT *
                    FROM %s
                    WHERE k = 'key' AND regexp_replace(v, '(.*)', x -> x[1]) IS NOT NULL
                    """.formatted(table),
                    "VALUES ('value', 'key')");
        }
        finally {
            assertUpdate("DROP TABLE " + table);
        }
    }
}
