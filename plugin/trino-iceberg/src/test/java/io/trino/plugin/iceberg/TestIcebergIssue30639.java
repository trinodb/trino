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
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;

public class TestIcebergIssue30639
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder().build();
    }

    @Test
    public void testMergeWithPartitionedJoinAndUnmodifiedRows()
    {
        try (TestTable target = new TestTable(
                getQueryRunner()::execute,
                "test_issue_30639_target",
                "WITH (partitioning = ARRAY['bucket(k1, 16)', 'bucket(k2, 8)']) AS " +
                        "SELECT 'a-' || CAST(i AS varchar) AS k1, 'b-' || CAST(i AS varchar) AS k2, " +
                        "'UPDATED' AS value, TIMESTAMP '2026-01-01 00:00:00.000000' AS updated_at " +
                        "FROM UNNEST(sequence(1, 3000)) t(i)")) {
            try (TestTable source = new TestTable(
                    getQueryRunner()::execute,
                    "test_issue_30639_source",
                    "AS " +
                            "SELECT 'a-' || CAST(i AS varchar) AS k1, 'b-' || CAST(i AS varchar) AS k2, " +
                            "'UPDATED' AS value, TIMESTAMP '2026-01-01 00:00:00.000000' AS updated_at " +
                            "FROM UNNEST(sequence(1, 3000)) t(i) " +
                            "UNION ALL " +
                            "SELECT 'x-' || CAST(a.i AS varchar) || '-' || CAST(b.j AS varchar), " +
                            "'y-' || CAST(a.i AS varchar) || '-' || CAST(b.j AS varchar), " +
                            "'DELETED', TIMESTAMP '2026-01-02 00:00:00.000000' " +
                            "FROM UNNEST(sequence(1, 6000)) a(i) CROSS JOIN UNNEST(sequence(1, 7)) b(j)")) {
                Session session = Session.builder(getSession())
                        .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                        .build();

                assertUpdate(
                        session,
                        "MERGE INTO %s t USING %s s ".formatted(target.getName(), source.getName()) +
                                "ON t.k1 = s.k1 AND t.k2 = s.k2 " +
                                "WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET value = s.value, updated_at = s.updated_at " +
                                "WHEN NOT MATCHED AND s.value <> 'DELETED' THEN INSERT (k1, k2, value, updated_at) VALUES (s.k1, s.k2, s.value, s.updated_at)",
                        0);
                assertQuery("SELECT count(*) FROM " + target.getName(), "VALUES 3000");
            }
        }
    }
}
