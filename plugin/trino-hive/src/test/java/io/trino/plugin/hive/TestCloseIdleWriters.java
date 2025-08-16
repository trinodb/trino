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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import static io.trino.SystemSessionProperties.IDLE_WRITER_MIN_DATA_SIZE_THRESHOLD;
import static io.trino.SystemSessionProperties.SCALE_WRITERS;
import static io.trino.SystemSessionProperties.TASK_MAX_WRITER_COUNT;
import static io.trino.SystemSessionProperties.TASK_MIN_WRITER_COUNT;
import static io.trino.SystemSessionProperties.TASK_SCALE_WRITERS_ENABLED;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCloseIdleWriters
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                .setWorkerCount(0)
                // Set the target max file size to 100GB so that we don't close writers due to file size in append
                // page.
                .setHiveProperties(ImmutableMap.of(
                        "hive.target-max-file-size", "100GB",
                        "hive.idle-writer-min-file-size", "0.1MB"))
                .build();
    }

    @Test
    public void testCloseIdleWriters()
    {
        String sourceTable = "tpch.\"sf0.1\".lineitem";
        String targetTable = "task_close_idle_writers_" + randomNameSuffix();
        try {
            String zeroShipModes = "'AIR', 'FOB', 'SHIP', 'TRUCK'";
            String oneShipModes = "'MAIL', 'RAIL', 'REG AIR'";
            String bothShipModes = zeroShipModes + ", " + oneShipModes;

            long expectedCount = (long) computeScalar("SELECT count (*) FROM %s WHERE shipmode IN (%s)".formatted(sourceTable, bothShipModes));

            // Create a table with two partitions (0 and 1). Using the order by trick we will write the partitions in
            // this order 0, 1, and then again 0. This way we are sure that during partition 1 write there will
            // be an idle writer for partition 0. Additionally, during second partition 0 write, there will be an idle
            // writer for partition 1.
            @Language("SQL") String createTableSql =
                    """
                    CREATE TABLE %s WITH (format = 'ORC', partitioned_by = ARRAY['shipmodeVal'])
                    AS SELECT orderkey, partkey, suppkey, linenumber, quantity, extendedprice,
                    discount, tax, returnflag, linestatus, commitdate, receiptdate, shipinstruct,
                    comment, shipdate,
                    CASE
                        WHEN shipmode IN (%s) THEN 0
                        WHEN shipmode IN (%s) THEN 1
                    END AS shipmodeVal
                    FROM %s
                    WHERE shipmode IN (%s)
                    ORDER BY shipmode
                    LIMIT %s
                    """.formatted(targetTable, zeroShipModes, oneShipModes, sourceTable, bothShipModes, expectedCount);

            // Disable all kind of scaling and set low idle writer threshold
            assertUpdate(
                    Session.builder(getSession())
                            .setSystemProperty(SCALE_WRITERS, "false")
                            .setSystemProperty(TASK_SCALE_WRITERS_ENABLED, "false")
                            .setSystemProperty(TASK_MAX_WRITER_COUNT, "1")
                            .setSystemProperty(TASK_MIN_WRITER_COUNT, "1")
                            .setSystemProperty(IDLE_WRITER_MIN_DATA_SIZE_THRESHOLD, "0.1MB")
                            .build(),
                    createTableSql,
                    expectedCount);
            long files = (long) computeScalar("SELECT count(DISTINCT \"$path\") FROM " + targetTable);
            // There should more than 2 files since we triggered close idle writers.
            assertThat(files).isGreaterThan(2);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + targetTable);
        }
    }
}
