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
import com.google.common.io.Closer;
import io.opentelemetry.api.common.Attributes;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
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
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_LOCATION;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_READ_POSITION;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_READ_SIZE;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_WRITE_POSITION;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_WRITE_SIZE;
import static io.trino.plugin.hive.HiveQueryRunner.HIVE_CATALOG;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

@Execution(ExecutionMode.SAME_THREAD)
public class TestHiveAlluxioCacheFileOperations
        extends AbstractTestQueryFramework
{
    private final Closer closer = Closer.create();

    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        Path cacheDirectory = Files.createTempDirectory("cache");
        closer.register(() -> deleteRecursively(cacheDirectory, ALLOW_INSECURE));
        Path metastoreDirectory = Files.createTempDirectory(HIVE_CATALOG);
        closer.register(() -> deleteRecursively(metastoreDirectory, ALLOW_INSECURE));

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
                .setNodeCount(2)
                .build();
    }

    @AfterAll
    public void destroy()
            throws Exception
    {
        closer.close();
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
                        .add(new CacheOperation("Alluxio.readCached", "key=p1/", 0, 279))
                        .add(new CacheOperation("Alluxio.readCached", "key=p2/", 0, 279))
                        .add(new CacheOperation("Alluxio.readExternal", "key=p1/", 0, 279))
                        .add(new CacheOperation("Alluxio.readExternal", "key=p2/", 0, 279))
                        .add(new CacheOperation("Alluxio.writeCache", "key=p1/", 0, 279))
                        .add(new CacheOperation("Alluxio.writeCache", "key=p2/", 0, 279))
                        .build());
        assertFileSystemAccesses(
                "SELECT * FROM test_cache_file_operations",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readCached", "key=p1/", 0, 279))
                        .add(new CacheOperation("Alluxio.readCached", "key=p2/", 0, 279))
                        .build());
        assertUpdate("INSERT INTO test_cache_file_operations VALUES ('3-xyz', 'p3')", 1);
        assertUpdate("INSERT INTO test_cache_file_operations VALUES ('4-xyz', 'p4')", 1);
        assertUpdate("INSERT INTO test_cache_file_operations VALUES ('5-xyz', 'p5')", 1);
        assertFileSystemAccesses(
                "SELECT * FROM test_cache_file_operations",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readCached", "key=p1/", 0, 279))
                        .add(new CacheOperation("Alluxio.readCached", "key=p2/", 0, 279))
                        .add(new CacheOperation("Alluxio.readCached", "key=p3/", 0, 279))
                        .add(new CacheOperation("Alluxio.readCached", "key=p4/", 0, 279))
                        .add(new CacheOperation("Alluxio.readCached", "key=p5/", 0, 279))
                        .add(new CacheOperation("Alluxio.readExternal", "key=p3/", 0, 279))
                        .add(new CacheOperation("Alluxio.readExternal", "key=p4/", 0, 279))
                        .add(new CacheOperation("Alluxio.readExternal", "key=p5/", 0, 279))
                        .add(new CacheOperation("Alluxio.writeCache", "key=p3/", 0, 279))
                        .add(new CacheOperation("Alluxio.writeCache", "key=p4/", 0, 279))
                        .add(new CacheOperation("Alluxio.writeCache", "key=p5/", 0, 279))
                        .build());
        assertFileSystemAccesses(
                "SELECT * FROM test_cache_file_operations",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readCached", "key=p1/", 0, 279))
                        .add(new CacheOperation("Alluxio.readCached", "key=p2/", 0, 279))
                        .add(new CacheOperation("Alluxio.readCached", "key=p3/", 0, 279))
                        .add(new CacheOperation("Alluxio.readCached", "key=p4/", 0, 279))
                        .add(new CacheOperation("Alluxio.readCached", "key=p5/", 0, 279))
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

    private static final Pattern DATA_FILE_PATTERN = Pattern.compile(".*?/(?<partition>key=[^/]*/)?(?<queryId>\\d{8}_\\d{6}_\\d{5}_\\w{5})_(?<uuid>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})");

    private record CacheOperation(String operationName, String fileId, long position, long length)
    {
        public static CacheOperation create(String operationName, Attributes attributes)
        {
            String path = requireNonNull(attributes.get(CACHE_FILE_LOCATION));
            String fileName = path.replaceFirst(".*/", "");

            long position = switch (operationName) {
                case "Alluxio.readCached" -> requireNonNull(attributes.get(CACHE_FILE_READ_POSITION));
                case "Alluxio.readExternal" -> requireNonNull(attributes.get(CACHE_FILE_READ_POSITION));
                case "Alluxio.writeCache" -> requireNonNull(attributes.get(CACHE_FILE_WRITE_POSITION));
                default -> throw new IllegalArgumentException("Unexpected operation name: " + operationName);
            };

            long length = switch (operationName) {
                case "Alluxio.readCached" -> requireNonNull(attributes.get(CACHE_FILE_READ_SIZE));
                case "Alluxio.readExternal" -> requireNonNull(attributes.get(CACHE_FILE_READ_SIZE));
                case "Alluxio.writeCache" -> requireNonNull(attributes.get(CACHE_FILE_WRITE_SIZE));
                default -> throw new IllegalArgumentException("Unexpected operation name: " + operationName);
            };

            if (!path.contains("/.trino")) {
                Matcher matcher = DATA_FILE_PATTERN.matcher(path);
                if (matcher.matches()) {
                    return new CacheOperation(operationName, matcher.group("partition"), position, length);
                }
            }
            else {
                return new CacheOperation(operationName, fileName, position, length);
            }
            throw new IllegalArgumentException("File not recognized: " + path);
        }
    }

    private static boolean isTrinoSchemaOrPermissions(String path)
    {
        return path.endsWith(".trinoSchema") || path.contains(".trinoPermissions");
    }
}
