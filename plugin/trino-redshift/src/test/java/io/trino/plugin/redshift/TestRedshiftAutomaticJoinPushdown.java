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
package io.trino.plugin.redshift;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.jdbc.BaseAutomaticJoinPushdownTest;
import io.trino.testing.QueryRunner;
import org.testng.SkipException;

import static io.trino.plugin.redshift.RedshiftQueryRunner.TEST_SCHEMA;
import static io.trino.plugin.redshift.RedshiftQueryRunner.createRedshiftQueryRunner;
import static io.trino.plugin.redshift.RedshiftQueryRunner.executeInRedshift;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class TestRedshiftAutomaticJoinPushdown
        extends BaseAutomaticJoinPushdownTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createRedshiftQueryRunner(
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableList.of());
    }

    @Override
    public void testJoinPushdownWithEmptyStatsInitially()
    {
        throw new SkipException("Redshift table statistics are automatically populated");
    }

    @Override
    protected void gatherStats(String tableName)
    {
        executeInRedshift(handle -> {
            handle.execute(format("ANALYZE VERBOSE %s.%s", TEST_SCHEMA, tableName));
            for (int i = 0; i < 5; i++) {
                long actualCount = handle.createQuery(format("SELECT count(*) FROM %s.%s", TEST_SCHEMA, tableName))
                        .mapTo(Long.class)
                        .one();
                long estimatedCount = handle.createQuery(
                                "SELECT reltuples FROM pg_class " +
                                        "WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = :schema) " +
                                        "AND relname = :table_name")
                        .bind("schema", TEST_SCHEMA)
                        .bind("table_name", tableName.toLowerCase(ENGLISH).replace("\"", ""))
                        .mapTo(Long.class)
                        .one();
                if (actualCount == estimatedCount) {
                    return;
                }
                handle.execute(format("ANALYZE VERBOSE %s.%s", TEST_SCHEMA, tableName));
            }
            throw new IllegalStateException("Stats not gathered");
        });
    }
}
