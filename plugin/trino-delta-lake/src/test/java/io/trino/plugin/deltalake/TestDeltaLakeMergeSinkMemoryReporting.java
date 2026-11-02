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
package io.trino.plugin.deltalake;

import io.trino.operator.OperatorStats;
import io.trino.spi.QueryId;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers memory reporting from {@link DeltaLakeMergeSink}'s parquet reader, used when a DELETE rewrites a file.
 */
public class TestDeltaLakeMergeSinkMemoryReporting
        extends AbstractTestQueryFramework
{
    private Path catalogDir;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        catalogDir = Files.createTempDirectory("catalog-dir");
        closeAfterClass(() -> deleteRecursively(catalogDir, ALLOW_INSECURE));

        return DeltaLakeQueryRunner.builder()
                .addDeltaProperty("fs.hadoop.enabled", "true")
                .addDeltaProperty("hive.metastore.catalog.dir", catalogDir.toUri().toString())
                .addDeltaProperty("delta.enable-non-concurrent-writes", "true")
                .build();
    }

    @Test
    public void testDeleteRewritingFileReportsMergeWriterMemoryUsage()
    {
        String tableName = "test_delete_rewrite_memory_" + randomNameSuffix();
        // both rows share the same partition value and land in one file; deleting only one forces a rewrite
        assertUpdate(
                "CREATE TABLE " + tableName + " (id, part, value) WITH (partitioned_by = ARRAY['part']) " +
                        "AS VALUES (1, 'p', 'a'), (2, 'p', 'b')",
                2);

        MaterializedResultWithPlan result = getDistributedQueryRunner().executeWithPlan(getSession(), "DELETE FROM " + tableName + " WHERE id = 1");
        assertThat(getMergeWriterOperatorStats(result.queryId()).getPeakUserMemoryReservation().toBytes())
                .isGreaterThan(0);

        assertQuery("SELECT * FROM " + tableName, "VALUES (2, 'p', 'b')");
    }

    private OperatorStats getMergeWriterOperatorStats(QueryId queryId)
    {
        return getDistributedQueryRunner().getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(queryId)
                .getQueryStats()
                .getOperatorSummaries()
                .stream()
                .filter(summary -> summary.getOperatorType().equals("MergeWriterOperator"))
                .collect(onlyElement());
    }
}
