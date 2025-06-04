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

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.filesystem.tracing.CacheFileSystemTraceUtils.CacheOperation;
import static io.trino.filesystem.tracing.CacheFileSystemTraceUtils.getCacheOperationSpans;
import static io.trino.filesystem.tracing.CacheFileSystemTraceUtils.getFileLocation;
import static io.trino.plugin.hive.HiveQueryRunner.HIVE_CATALOG;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static java.util.stream.Collectors.toCollection;

@Execution(ExecutionMode.SAME_THREAD)
public class TestHiveAlluxioCacheFileOperations
        extends AbstractTestQueryFramework
{
    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        Path cacheDirectory = Files.createTempDirectory("cache");
        closeAfterClass(() -> deleteRecursively(cacheDirectory, ALLOW_INSECURE));
        Path metastoreDirectory = Files.createTempDirectory(HIVE_CATALOG);
        closeAfterClass(() -> deleteRecursively(metastoreDirectory, ALLOW_INSECURE));

        Map<String, String> hiveProperties = ImmutableMap.<String, String>builder()
                .put("fs.cache.enabled", "true")
                .put("fs.cache.directories", cacheDirectory.toAbsolutePath().toString())
                .put("fs.cache.max-sizes", "100MB")
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", metastoreDirectory.toUri().toString())
                .buildOrThrow();

        return HiveQueryRunner.builder()
                .setCoordinatorProperties(ImmutableMap.of("node-scheduler.include-coordinator", "false"))
                .setHiveProperties(hiveProperties)
                .setWorkerCount(1)
                .build();
    }

    @Test
    public void testCacheFileOperations()
    {
        assertUpdate("DROP TABLE IF EXISTS test_cache_file_operations");
        assertUpdate("CREATE TABLE test_cache_file_operations(data varchar, key varchar) WITH (partitioned_by=ARRAY['key'], format='parquet')");
        assertUpdate("INSERT INTO test_cache_file_operations VALUES ('1-abc', 'p1')", 1);
        assertUpdate("INSERT INTO test_cache_file_operations VALUES ('2-xyz', 'p2')", 1);
        assertFileSystemAccesses(
                "SELECT * FROM test_cache_file_operations",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readCached", "key=p1/"))
                        .add(new CacheOperation("Alluxio.readCached", "key=p2/"))
                        .add(new CacheOperation("Input.readFully", "key=p1/"))
                        .add(new CacheOperation("Input.readFully", "key=p2/"))
                        .add(new CacheOperation("Alluxio.writeCache", "key=p1/"))
                        .add(new CacheOperation("Alluxio.writeCache", "key=p2/"))
                        .build());
        assertFileSystemAccesses(
                "SELECT * FROM test_cache_file_operations",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readCached", "key=p1/"))
                        .add(new CacheOperation("Alluxio.readCached", "key=p2/"))
                        .build());
        assertUpdate("INSERT INTO test_cache_file_operations VALUES ('3-xyz', 'p3')", 1);
        assertUpdate("INSERT INTO test_cache_file_operations VALUES ('4-xyz', 'p4')", 1);
        assertUpdate("INSERT INTO test_cache_file_operations VALUES ('5-xyz', 'p5')", 1);
        assertFileSystemAccesses(
                "SELECT * FROM test_cache_file_operations",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readCached", "key=p1/"))
                        .add(new CacheOperation("Alluxio.readCached", "key=p2/"))
                        .add(new CacheOperation("Alluxio.readCached", "key=p3/"))
                        .add(new CacheOperation("Alluxio.readCached", "key=p4/"))
                        .add(new CacheOperation("Alluxio.readCached", "key=p5/"))
                        .add(new CacheOperation("Input.readFully", "key=p3/"))
                        .add(new CacheOperation("Input.readFully", "key=p4/"))
                        .add(new CacheOperation("Input.readFully", "key=p5/"))
                        .add(new CacheOperation("Alluxio.writeCache", "key=p3/"))
                        .add(new CacheOperation("Alluxio.writeCache", "key=p4/"))
                        .add(new CacheOperation("Alluxio.writeCache", "key=p5/"))
                        .build());
        assertFileSystemAccesses(
                "SELECT * FROM test_cache_file_operations",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readCached", "key=p1/"))
                        .add(new CacheOperation("Alluxio.readCached", "key=p2/"))
                        .add(new CacheOperation("Alluxio.readCached", "key=p3/"))
                        .add(new CacheOperation("Alluxio.readCached", "key=p4/"))
                        .add(new CacheOperation("Alluxio.readCached", "key=p5/"))
                        .build());
    }

    private void assertFileSystemAccesses(@Language("SQL") String query, Multiset<CacheOperation> expectedCacheAccesses)
    {
        DistributedQueryRunner queryRunner = getDistributedQueryRunner();
        queryRunner.executeWithPlan(queryRunner.getDefaultSession(), query);
        assertMultisetsEqual(getCacheOperations(), expectedCacheAccesses);
    }

    private Multiset<CacheOperation> getCacheOperations()
    {
        return getCacheOperationSpans(getQueryRunner())
                .stream()
                .map(TestHiveAlluxioCacheFileOperations::createCacheOperation)
                .collect(toCollection(HashMultiset::create));
    }

    private static final Pattern DATA_FILE_PATTERN = Pattern.compile(".*?/(?<partition>key=[^/]*/)?(?<queryId>\\d{8}_\\d{6}_\\d{5}_\\w{5})_(?<uuid>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})");

    private static CacheOperation createCacheOperation(SpanData span)
    {
        String operationName = span.getName();
        String path = getFileLocation(span);
        String fileName = path.replaceFirst(".*/", "");

        if (!path.contains("/.trino")) {
            Matcher matcher = DATA_FILE_PATTERN.matcher(path);
            if (matcher.matches()) {
                return new CacheOperation(operationName, matcher.group("partition"));
            }
        }
        else {
            return new CacheOperation(operationName, fileName);
        }
        throw new IllegalArgumentException("File not recognized: " + path);
    }
}
