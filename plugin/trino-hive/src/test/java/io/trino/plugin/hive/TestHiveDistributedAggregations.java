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

import io.trino.testing.AbstractTestAggregations;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;

public class TestHiveDistributedAggregations
        extends AbstractTestAggregations
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Test
    public void testDistinctAggregationWithSystemTable()
    {
        String tableName = "test_dist_aggr_" + randomNameSuffix();
        @Language("SQL") String createTable =
                """
                CREATE TABLE %s
                WITH (
                partitioned_by = ARRAY[ 'regionkey', 'nationkey' ]
                ) AS (SELECT name, comment, regionkey, nationkey FROM nation)
                """.formatted(tableName);

        assertUpdate(getSession(), createTable, 25);

        assertQuerySucceeds("SELECT count(distinct regionkey), count(distinct nationkey) FROM \"%s$partitions\"".formatted(tableName));
    }
}
