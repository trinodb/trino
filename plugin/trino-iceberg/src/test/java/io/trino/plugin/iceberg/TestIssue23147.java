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
package io.trino.plugin.iceberg;

import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static io.trino.SystemSessionProperties.OPTIMIZE_METADATA_QUERIES;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestIssue23147
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder().build();
    }

    @Test
    @Timeout(30)
    public void testCastInOverPartitionedTable()
    {
        String tableName = "test_issue_23147_cast_in";
        assertUpdate("CREATE TABLE %s(id int, part_key bigint) WITH (partitioning = ARRAY ['part_key'])".formatted(tableName));
        try {
            assertUpdate("""
                    INSERT INTO %s
                         VALUES
                                 (1, 1),
                                 (2, 3),
                                 (3, 4),
                                 (4, 5)
                    """.formatted(tableName), 4);

            Session session = testSessionBuilder(getSession())
                    .setSystemProperty(OPTIMIZE_METADATA_QUERIES, "true")
                    .build();
            @Language("SQL") String query = """
                    SELECT t0.*
                    FROM (
                            SELECT cast(part_key as varchar) part FROM %s
                            WHERE part_key IN (SELECT part_key FROM %s)
                    ) t0
                    WHERE t0.part IN ('3', '4')
                    """.formatted(tableName, tableName);
            assertQuery(session, query, "VALUES ('3'), ('4')");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    @Timeout(30)
    public void testInOverContinuousPartitionValues()
    {
        // Mirrors the SQL from issue #23147 as originally reported: an IN expression that
        // SimplifyContinuousInValues rewrites to a BETWEEN, layered with partition pruning that
        // enforces the partition keys as discrete singletons. Without value-aware domain
        // equivalence in RemoveRedundantPredicateAboveTableScan, the planner loops forever.
        String tableA = "test_issue_23147_a";
        String tableB = "test_issue_23147_b";
        assertUpdate("CREATE TABLE %s(stat_month bigint) WITH (partitioning = ARRAY ['stat_month'])".formatted(tableA));
        assertUpdate("CREATE TABLE %s(stat_month bigint) WITH (partitioning = ARRAY ['stat_month'])".formatted(tableB));
        try {
            assertUpdate("INSERT INTO %s VALUES 202403, 202404".formatted(tableA), 2);
            assertUpdate("INSERT INTO %s VALUES 202403, 202404, 202405".formatted(tableB), 3);

            Session session = testSessionBuilder(getSession())
                    .setSystemProperty(OPTIMIZE_METADATA_QUERIES, "true")
                    .build();
            @Language("SQL") String query = """
                    SELECT t0.*
                    FROM (
                            SELECT stat_month
                            FROM %s
                            WHERE stat_month IN (SELECT stat_month FROM %s)
                            UNION ALL
                            SELECT stat_month FROM %s
                    ) t0
                    WHERE t0.stat_month IN (202403, 202404)
                    """.formatted(tableA, tableA, tableB);
            assertQuery(session, query, "VALUES (202403), (202404), (202403), (202404)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableA);
            assertUpdate("DROP TABLE " + tableB);
        }
    }
}
