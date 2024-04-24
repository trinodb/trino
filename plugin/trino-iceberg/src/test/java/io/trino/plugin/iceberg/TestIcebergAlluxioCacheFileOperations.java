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
import io.opentelemetry.api.common.Attributes;
import io.trino.plugin.iceberg.TestIcebergFileOperations.FileType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_LOCATION;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.TestIcebergFileOperations.FileType.DATA;
import static io.trino.plugin.iceberg.TestIcebergFileOperations.FileType.MANIFEST;
import static io.trino.plugin.iceberg.TestIcebergFileOperations.FileType.METADATA_JSON;
import static io.trino.plugin.iceberg.TestIcebergFileOperations.FileType.SNAPSHOT;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

// single-threaded as DistributedQueryRunner.spans is shared mutable state
@Execution(ExecutionMode.SAME_THREAD)
public class TestIcebergAlluxioCacheFileOperations
        extends AbstractTestQueryFramework
{
    public static final String TEST_SCHEMA = "test_alluxio_schema";

    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        Path cacheDirectory = Files.createTempDirectory("cache");
        cacheDirectory.toFile().deleteOnExit();
        Path metastoreDirectory = Files.createTempDirectory(ICEBERG_CATALOG);
        metastoreDirectory.toFile().deleteOnExit();

        Map<String, String> icebergProperties = ImmutableMap.<String, String>builder()
                .put("fs.cache.enabled", "true")
                .put("fs.cache.directories", cacheDirectory.toAbsolutePath().toString())
                .put("fs.cache.max-sizes", "100MB")
                .put("hive.metastore.catalog.dir", metastoreDirectory.toUri().toString())
                .buildOrThrow();

        DistributedQueryRunner queryRunner = IcebergQueryRunner.builder()
                .setSchemaInitializer(SchemaInitializer.builder()
                        .withSchemaName(TEST_SCHEMA)
                        .build())
                .setIcebergProperties(icebergProperties)
                .setNodeCount(1)
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
                        .addCopies(new CacheOperation("Alluxio.readExternal", DATA), 2)
                        .addCopies(new CacheOperation("Alluxio.readCached", DATA), 2)
                        .addCopies(new CacheOperation("Alluxio.writeCache", DATA), 2)
                        .add(new CacheOperation("Alluxio.readExternal", METADATA_JSON))
                        .add(new CacheOperation("Alluxio.readCached", METADATA_JSON))
                        .add(new CacheOperation("Alluxio.writeCache", METADATA_JSON))
                        .addCopies(new CacheOperation("Alluxio.readCached", SNAPSHOT), 2)
                        .add(new CacheOperation("Alluxio.readExternal", MANIFEST))
                        .addCopies(new CacheOperation("Alluxio.readCached", MANIFEST), 4)
                        .add(new CacheOperation("Alluxio.writeCache", MANIFEST))
                        .build());

        assertFileSystemAccesses(
                "SELECT * FROM test_cache_file_operations",
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation("Alluxio.readCached", DATA), 2)
                        .add(new CacheOperation("Alluxio.readCached", METADATA_JSON))
                        .addCopies(new CacheOperation("Alluxio.readCached", SNAPSHOT), 2)
                        .addCopies(new CacheOperation("Alluxio.readCached", MANIFEST), 4)
                        .build());

        assertUpdate("INSERT INTO test_cache_file_operations VALUES ('p3', '3-xyz')", 1);
        assertUpdate("INSERT INTO test_cache_file_operations VALUES ('p4', '4-xyz')", 1);
        assertUpdate("INSERT INTO test_cache_file_operations VALUES ('p5', '5-xyz')", 1);

        assertFileSystemAccesses(
                "SELECT * FROM test_cache_file_operations",
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation("Alluxio.readExternal", DATA), 3)
                        .addCopies(new CacheOperation("Alluxio.readCached", DATA), 5)
                        .addCopies(new CacheOperation("Alluxio.writeCache", DATA), 3)
                        .add(new CacheOperation("Alluxio.readExternal", METADATA_JSON))
                        .addCopies(new CacheOperation("Alluxio.readCached", METADATA_JSON), 2)
                        .add(new CacheOperation("Alluxio.writeCache", METADATA_JSON))
                        .addCopies(new CacheOperation("Alluxio.readCached", SNAPSHOT), 2)
                        .add(new CacheOperation("Alluxio.readExternal", MANIFEST))
                        .addCopies(new CacheOperation("Alluxio.readCached", MANIFEST), 10)
                        .add(new CacheOperation("Alluxio.writeCache", MANIFEST))
                        .build());
        assertFileSystemAccesses(
                "SELECT * FROM test_cache_file_operations",
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation("Alluxio.readCached", DATA), 5)
                        .addCopies(new CacheOperation("Alluxio.readCached", METADATA_JSON), 2)
                        .addCopies(new CacheOperation("Alluxio.readCached", SNAPSHOT), 2)
                        .addCopies(new CacheOperation("Alluxio.readCached", MANIFEST), 10)
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
        return getQueryRunner().getSpans().stream()
                .filter(span -> span.getName().startsWith("Alluxio."))
                .filter(span -> !isTrinoSchemaOrPermissions(requireNonNull(span.getAttributes().get(CACHE_FILE_LOCATION))))
                .map(span -> CacheOperation.create(span.getName(), span.getAttributes()))
                .collect(toCollection(HashMultiset::create));
    }

    private record CacheOperation(String operationName, FileType fileType)
    {
        public static CacheOperation create(String operationName, Attributes attributes)
        {
            String path = requireNonNull(attributes.get(CACHE_FILE_LOCATION));
            return new CacheOperation(operationName, FileType.fromFilePath(path));
        }
    }

    private static boolean isTrinoSchemaOrPermissions(String path)
    {
        return path.endsWith(".trinoSchema") || path.contains(".trinoPermissions");
    }
}
