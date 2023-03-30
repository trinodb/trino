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
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static java.lang.String.format;

public class TestBucketedQueryWithManySplits
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                .setNodeCount(1)
                .setExtraProperties(ImmutableMap.of(
                        "query.schedule-split-batch-size", "1",
                        "node-scheduler.max-splits-per-node", "1",
                        "node-scheduler.min-pending-splits-per-task", "1"))
                .build();
    }

    @Test(timeOut = 120_000)
    public void testBucketedQueryWithManySplits()
    {
        QueryRunner queryRunner = getQueryRunner();
        queryRunner.execute("CREATE TABLE tbl_a (col bigint, bucket bigint) WITH (bucketed_by=array['bucket'], bucket_count=10)");
        queryRunner.execute("CREATE TABLE tbl_b (col bigint, bucket bigint) WITH (bucketed_by=array['bucket'], bucket_count=10)");

        for (int i = 0; i < 50; i++) {
            queryRunner.execute(format("INSERT INTO tbl_a VALUES (%s, %s)", i, i));
            queryRunner.execute(format("INSERT INTO tbl_b VALUES (%s, %s)", i, i));
        }

        // query should not deadlock
        assertQuery("" +
                        "WITH test_data AS" +
                        "  (SELECT bucket" +
                        "   FROM" +
                        "     (SELECT" +
                        "        bucket" +
                        "      FROM tbl_a" +
                        "      UNION ALL" +
                        "      SELECT" +
                        "        bucket" +
                        "      FROM tbl_b) " +
                        "   GROUP BY bucket) " +
                        "SELECT COUNT(1) FROM test_data",
                "VALUES 50");

        assertUpdate("DROP TABLE tbl_a");
        assertUpdate("DROP TABLE tbl_b");
    }
}
