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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.trino.filesystem.tracing.CacheFileSystemTraceUtils;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.nio.file.Files;
import java.nio.file.Path;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.deltalake.DeltaLakeAlluxioCacheTestUtils.getCacheOperations;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;

@Execution(ExecutionMode.SAME_THREAD)
public class TestDeltaLakeAlluxioCacheMutableTransactionLog
        extends AbstractTestQueryFramework
{
    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        Path cacheDirectory = Files.createTempDirectory("cache");
        closeAfterClass(() -> deleteRecursively(cacheDirectory, ALLOW_INSECURE));

        return DeltaLakeQueryRunner.builder()
                .setCoordinatorProperties(ImmutableMap.of("node-scheduler.include-coordinator", "false"))
                .setDeltaProperties(ImmutableMap.<String, String>builder()
                        .put("fs.cache.enabled", "true")
                        .put("fs.cache.directories", cacheDirectory.toAbsolutePath().toString())
                        .put("fs.cache.max-sizes", "100MB")
                        .put("delta.enable-non-concurrent-writes", "true")
                        .put("delta.register-table-procedure.enabled", "true")
                        .put("delta.fs.cache.disable-transaction-log-caching", "true")
                        .buildOrThrow())
                .setWorkerCount(1)
                .build();
    }

    /**
     * Tests that querying a table twice results in the table data being cached, while the delta log data remains uncached.
     * <p>
     * This test ensures that when a table is queried multiple times, the underlying data is retrieved from cache, improving performance.
     * At the same time, it verifies that the transaction log is not cached, ensuring that updates or changes to the log are always fetched fresh.
     */
    @Test
    public void testTableDataCachedWhileTransactionLogNotCached()
    {
        assertUpdate("DROP TABLE IF EXISTS test_transaction_log_not_cached");
        assertUpdate("CREATE TABLE test_transaction_log_not_cached(key varchar, data varchar) with (partitioned_by=ARRAY['key'], checkpoint_interval = 2)");
        assertUpdate("INSERT INTO test_transaction_log_not_cached VALUES ('p1', '1-abc')", 1);
        assertUpdate("INSERT INTO test_transaction_log_not_cached VALUES ('p2', '2-xyz')", 1);
        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'test_transaction_log_not_cached')");
        assertFileSystemAccesses(
                "SELECT * FROM test_transaction_log_not_cached",
                ImmutableMultiset.<CacheFileSystemTraceUtils.CacheOperation>builder()
                        .addCopies(new CacheFileSystemTraceUtils.CacheOperation("InputFile.length", "00000000000000000002.checkpoint.parquet"), 2)
                        .addCopies(new CacheFileSystemTraceUtils.CacheOperation("Input.readTail", "00000000000000000002.checkpoint.parquet"), 2)
                        .add(new CacheFileSystemTraceUtils.CacheOperation("InputFile.newStream", "00000000000000000003.json"))
                        .add(new CacheFileSystemTraceUtils.CacheOperation("InputFile.newStream", "_last_checkpoint"))
                        .add(new CacheFileSystemTraceUtils.CacheOperation("Alluxio.readCached", "key=p1/", 0, 220))
                        .add(new CacheFileSystemTraceUtils.CacheOperation("Alluxio.readCached", "key=p2/", 0, 220))
                        .add(new CacheFileSystemTraceUtils.CacheOperation("Input.readFully", "key=p1/", 0, 220))
                        .add(new CacheFileSystemTraceUtils.CacheOperation("Input.readFully", "key=p2/", 0, 220))
                        .add(new CacheFileSystemTraceUtils.CacheOperation("Alluxio.writeCache", "key=p1/", 0, 220))
                        .add(new CacheFileSystemTraceUtils.CacheOperation("Alluxio.writeCache", "key=p2/", 0, 220))
                        .build());
        assertFileSystemAccesses(
                "SELECT * FROM test_transaction_log_not_cached",
                ImmutableMultiset.<CacheFileSystemTraceUtils.CacheOperation>builder()
                        .addCopies(new CacheFileSystemTraceUtils.CacheOperation("InputFile.length", "00000000000000000002.checkpoint.parquet"), 2)
                        .addCopies(new CacheFileSystemTraceUtils.CacheOperation("Input.readTail", "00000000000000000002.checkpoint.parquet"), 2)
                        .add(new CacheFileSystemTraceUtils.CacheOperation("InputFile.newStream", "00000000000000000003.json"))
                        .add(new CacheFileSystemTraceUtils.CacheOperation("InputFile.newStream", "_last_checkpoint"))
                        .add(new CacheFileSystemTraceUtils.CacheOperation("Alluxio.readCached", "key=p1/", 0, 220))
                        .add(new CacheFileSystemTraceUtils.CacheOperation("Alluxio.readCached", "key=p2/", 0, 220))
                        .build());
    }

    private void assertFileSystemAccesses(@Language("SQL") String query, Multiset<CacheFileSystemTraceUtils.CacheOperation> expectedCacheAccesses)
    {
        assertUpdate("CALL system.flush_metadata_cache()");
        QueryRunner queryRunner = getQueryRunner();
        queryRunner.executeWithPlan(getSession(), query);
        assertMultisetsEqual(getCacheOperations(queryRunner), expectedCacheAccesses);
    }
}
