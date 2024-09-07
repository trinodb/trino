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

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.trino.plugin.iceberg.util.FileOperationUtils;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.filesystem.tracing.CacheFileSystemTraceUtils.getFileLocation;
import static io.trino.filesystem.tracing.CacheFileSystemTraceUtils.isTrinoSchemaOrPermissions;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.util.FileOperationUtils.FileType.DATA;
import static io.trino.plugin.iceberg.util.FileOperationUtils.FileType.MANIFEST;
import static io.trino.plugin.iceberg.util.FileOperationUtils.FileType.METADATA_JSON;
import static io.trino.plugin.iceberg.util.FileOperationUtils.FileType.SNAPSHOT;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static java.util.stream.Collectors.toCollection;

@Execution(ExecutionMode.SAME_THREAD)
public class TestIcebergMemoryCacheFileOperations
        extends AbstractTestQueryFramework
{
    private static final String TEST_SCHEMA = "test_memory_schema";

    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        Path metastoreDirectory = Files.createTempDirectory(ICEBERG_CATALOG);
        closeAfterClass(() -> deleteRecursively(metastoreDirectory, ALLOW_INSECURE));

        Map<String, String> icebergProperties = ImmutableMap.<String, String>builder()
                .put("iceberg.metadata-cache.enabled", "true")
                .put("hive.metastore.catalog.dir", metastoreDirectory.toUri().toString())
                .buildOrThrow();

        DistributedQueryRunner queryRunner = IcebergQueryRunner.builder()
                .setSchemaInitializer(SchemaInitializer.builder()
                        .withSchemaName(TEST_SCHEMA)
                        .build())
                .setIcebergProperties(icebergProperties)
                .setWorkerCount(0)
                .build();
        queryRunner.execute("CREATE SCHEMA IF NOT EXISTS " + TEST_SCHEMA);
        return queryRunner;
    }

    @Test
    public void testCacheFileOperations()
    {
        assertUpdate("DROP TABLE IF EXISTS test_cache_file_operations");
        assertUpdate("CREATE TABLE test_cache_file_operations(key varchar, data varchar) with (partitioning=ARRAY['key'])");
        assertUpdate("INSERT INTO test_cache_file_operations VALUES ('p1', '1-abc')", 1);
        assertUpdate("INSERT INTO test_cache_file_operations VALUES ('p2', '2-xyz')", 1);
        assertFileSystemAccesses(
                "SELECT * FROM test_cache_file_operations",
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation("Input.readTail", DATA), 2)
                        .addCopies(new CacheOperation("FileSystemCache.cacheInput", DATA), 2)
                        .add(new CacheOperation("Input.readTail", METADATA_JSON))
                        .add(new CacheOperation("InputFile.length", METADATA_JSON))
                        .add(new CacheOperation("FileSystemCache.cacheStream", METADATA_JSON))
                        .add(new CacheOperation("FileSystemCache.cacheLength", SNAPSHOT))
                        .add(new CacheOperation("FileSystemCache.cacheStream", SNAPSHOT))
                        .add(new CacheOperation("Input.readTail", MANIFEST))
                        .addCopies(new CacheOperation("FileSystemCache.cacheStream", MANIFEST), 2)
                        .build());

        assertFileSystemAccesses(
                "SELECT * FROM test_cache_file_operations",
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation("FileSystemCache.cacheInput", DATA), 2)
                        .add(new CacheOperation("FileSystemCache.cacheStream", METADATA_JSON))
                        .add(new CacheOperation("FileSystemCache.cacheLength", SNAPSHOT))
                        .add(new CacheOperation("FileSystemCache.cacheStream", SNAPSHOT))
                        .addCopies(new CacheOperation("FileSystemCache.cacheStream", MANIFEST), 2)
                        .build());

        assertUpdate("INSERT INTO test_cache_file_operations VALUES ('p3', '3-xyz')", 1);
        assertUpdate("INSERT INTO test_cache_file_operations VALUES ('p4', '4-xyz')", 1);
        assertUpdate("INSERT INTO test_cache_file_operations VALUES ('p5', '5-xyz')", 1);

        assertFileSystemAccesses(
                "SELECT * FROM test_cache_file_operations",
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation("Input.readTail", DATA), 3)
                        .addCopies(new CacheOperation("FileSystemCache.cacheInput", DATA), 5)
                        .add(new CacheOperation("Input.readTail", METADATA_JSON))
                        .add(new CacheOperation("InputFile.length", METADATA_JSON))
                        .add(new CacheOperation("FileSystemCache.cacheStream", METADATA_JSON))
                        .add(new CacheOperation("FileSystemCache.cacheLength", SNAPSHOT))
                        .add(new CacheOperation("FileSystemCache.cacheStream", SNAPSHOT))
                        .add(new CacheOperation("Input.readTail", MANIFEST))
                        .addCopies(new CacheOperation("FileSystemCache.cacheStream", MANIFEST), 5)
                        .build());

        assertFileSystemAccesses(
                "SELECT * FROM test_cache_file_operations",
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation("FileSystemCache.cacheInput", DATA), 5)
                        .add(new CacheOperation("FileSystemCache.cacheStream", METADATA_JSON))
                        .add(new CacheOperation("FileSystemCache.cacheLength", SNAPSHOT))
                        .add(new CacheOperation("FileSystemCache.cacheStream", SNAPSHOT))
                        .addCopies(new CacheOperation("FileSystemCache.cacheStream", MANIFEST), 5)
                        .build());
    }

    @Test
    public void testSelectWithFilter()
    {
        assertUpdate("CREATE TABLE test_select_with_filter AS SELECT 1 col_name", 1);
        assertFileSystemAccesses(
                "SELECT * FROM test_select_with_filter WHERE col_name = 1",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("FileSystemCache.cacheStream", METADATA_JSON))
                        .add(new CacheOperation("Input.readTail", METADATA_JSON))
                        .add(new CacheOperation("InputFile.length", METADATA_JSON))
                        .add(new CacheOperation("FileSystemCache.cacheStream", SNAPSHOT))
                        .add(new CacheOperation("FileSystemCache.cacheLength", SNAPSHOT))
                        .add(new CacheOperation("FileSystemCache.cacheStream", MANIFEST))
                        .add(new CacheOperation("Input.readTail", MANIFEST))
                        .add(new CacheOperation("FileSystemCache.cacheInput", DATA))
                        .add(new CacheOperation("Input.readTail", DATA))
                        .build());

        assertFileSystemAccesses(
                "SELECT * FROM test_select_with_filter WHERE col_name = 1",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("FileSystemCache.cacheStream", METADATA_JSON))
                        .add(new CacheOperation("FileSystemCache.cacheStream", SNAPSHOT))
                        .add(new CacheOperation("FileSystemCache.cacheLength", SNAPSHOT))
                        .add(new CacheOperation("FileSystemCache.cacheStream", MANIFEST))
                        .add(new CacheOperation("FileSystemCache.cacheInput", DATA))
                        .build());
    }

    @Test
    public void testJoin()
    {
        assertUpdate("CREATE TABLE test_join_t1 AS SELECT 2 AS age, 'id1' AS id", 1);
        assertUpdate("CREATE TABLE test_join_t2 AS SELECT 'name1' AS name, 'id1' AS id", 1);

        assertFileSystemAccesses("SELECT name, age FROM test_join_t1 JOIN test_join_t2 ON test_join_t2.id = test_join_t1.id",
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation("Input.readTail", METADATA_JSON), 2)
                        .addCopies(new CacheOperation("InputFile.length", METADATA_JSON), 2)
                        .addCopies(new CacheOperation("FileSystemCache.cacheStream", METADATA_JSON), 2)
                        .addCopies(new CacheOperation("FileSystemCache.cacheStream", SNAPSHOT), 2)
                        .addCopies(new CacheOperation("FileSystemCache.cacheLength", SNAPSHOT), 2)
                        .addCopies(new CacheOperation("Input.readTail", MANIFEST), 2)
                        .addCopies(new CacheOperation("FileSystemCache.cacheStream", MANIFEST), 4)
                        .addCopies(new CacheOperation("Input.readTail", DATA), 2)
                        .addCopies(new CacheOperation("FileSystemCache.cacheInput", DATA), 2)
                        .build());

        assertFileSystemAccesses("SELECT name, age FROM test_join_t1 JOIN test_join_t2 ON test_join_t2.id = test_join_t1.id",
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation("FileSystemCache.cacheStream", METADATA_JSON), 2)
                        .addCopies(new CacheOperation("FileSystemCache.cacheStream", SNAPSHOT), 2)
                        .addCopies(new CacheOperation("FileSystemCache.cacheLength", SNAPSHOT), 2)
                        .addCopies(new CacheOperation("FileSystemCache.cacheStream", MANIFEST), 4)
                        .addCopies(new CacheOperation("FileSystemCache.cacheInput", DATA), 2)
                        .build());
    }

    private void assertFileSystemAccesses(@Language("SQL") String query, Multiset<CacheOperation> expectedCacheAccesses)
    {
        DistributedQueryRunner queryRunner = getDistributedQueryRunner();
        queryRunner.executeWithPlan(queryRunner.getDefaultSession(), query);
        assertMultisetsEqual(expectedCacheAccesses, getCacheOperations());
    }

    private Multiset<CacheOperation> getCacheOperations()
    {
        return getQueryRunner().getSpans().stream()
                .filter(span -> span.getName().startsWith("Input.") || span.getName().startsWith("InputFile.") || span.getName().startsWith("FileSystemCache."))
                .filter(span -> !span.getName().startsWith("InputFile.newInput"))
                .filter(span -> !isTrinoSchemaOrPermissions(getFileLocation(span)))
                .map(CacheOperation::create)
                .collect(toCollection(HashMultiset::create));
    }

    private record CacheOperation(String operationName, FileOperationUtils.FileType fileType)
    {
        public static CacheOperation create(SpanData span)
        {
            String path = getFileLocation(span);
            return new CacheOperation(span.getName(), FileOperationUtils.FileType.fromFilePath(path));
        }
    }
}
