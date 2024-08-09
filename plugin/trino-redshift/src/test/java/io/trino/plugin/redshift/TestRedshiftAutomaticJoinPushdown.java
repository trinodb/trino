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

import io.trino.plugin.jdbc.BaseAutomaticJoinPushdownTest;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.redshift.TestingRedshiftServer.TEST_SCHEMA;
import static io.trino.plugin.redshift.TestingRedshiftServer.executeInRedshift;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class TestRedshiftAutomaticJoinPushdown
        extends BaseAutomaticJoinPushdownTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return RedshiftQueryRunner.builder()
                .build();
    }

    @Test
    @Override
    @Disabled
    public void testJoinPushdownWithEmptyStatsInitially()
    {
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
