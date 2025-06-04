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
import io.trino.filesystem.Location;
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

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.filesystem.tracing.FileSystemAttributes.FILE_LOCATION;
import static io.trino.plugin.hive.HiveQueryRunner.HIVE_CATALOG;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

@Execution(ExecutionMode.SAME_THREAD)
public class TestHiveFileOperations
        extends AbstractTestQueryFramework
{
    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        Path metastoreDirectory = Files.createTempDirectory(HIVE_CATALOG);
        closeAfterClass(() -> deleteRecursively(metastoreDirectory, ALLOW_INSECURE));

        Map<String, String> hiveProperties = ImmutableMap.<String, String>builder()
                .put("hive.file-status-cache-tables", "*")
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
    public void testCacheDirectoryListingOperations()
    {
        assertQuerySucceeds("CALL system.flush_metadata_cache()");
        assertUpdate("DROP TABLE IF EXISTS test_directory_listing_partitioned_a");
        assertUpdate("DROP TABLE IF EXISTS test_directory_listing_unpartitioned");

        assertUpdate("CREATE TABLE test_directory_listing_partitioned_a(data varchar, key varchar) WITH (partitioned_by=ARRAY['key'], format='parquet')");
        assertUpdate("INSERT INTO test_directory_listing_partitioned_a VALUES ('1-abc', 'p1')", 1);

        assertUpdate("CREATE TABLE test_directory_listing_unpartitioned(data varchar, key varchar) WITH (format='parquet')");
        assertUpdate("INSERT INTO test_directory_listing_unpartitioned VALUES ('1-abc', 'p1')", 1);
        assertUpdate("INSERT INTO test_directory_listing_unpartitioned VALUES ('2-xyz', 'p2')", 1);
        // FileSystem.listFiles, test_directory_listing_partitioned_a is from listing partitions in FileHiveMetastore, so can be ignored
        assertFileSystemAccesses(
                "SELECT * FROM test_directory_listing_partitioned_a",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation("Input.readTail", "key=p1/"))
                        .add(new FileOperation("FileSystem.listFiles", "test_directory_listing_partitioned_a"))
                        .add(new FileOperation("FileSystem.listFiles", "key=p1"))
                        .build());
        assertFileSystemAccesses(
                "SELECT * FROM test_directory_listing_partitioned_a",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation("Input.readTail", "key=p1/"))
                        .add(new FileOperation("FileSystem.listFiles", "test_directory_listing_partitioned_a"))
                        .build());
        assertFileSystemAccesses(
                "SELECT * FROM test_directory_listing_unpartitioned",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation("Input.readTail", "no partition"), 2)
                        .add(new FileOperation("FileSystem.listFiles", "test_directory_listing_unpartitioned"))
                        .build());
        assertFileSystemAccesses(
                "SELECT * FROM test_directory_listing_unpartitioned",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation("Input.readTail", "no partition"), 2)
                        .build());

        assertUpdate("INSERT INTO test_directory_listing_partitioned_a VALUES ('2-xyz', 'p2')", 1);
        assertFileSystemAccesses(
                "SELECT * FROM test_directory_listing_partitioned_a",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation("Input.readTail", "key=p1/"))
                        .add(new FileOperation("Input.readTail", "key=p2/"))
                        .add(new FileOperation("FileSystem.listFiles", "test_directory_listing_partitioned_a"))
                        .add(new FileOperation("FileSystem.listFiles", "key=p2"))
                        .build());
        assertFileSystemAccesses(
                "SELECT * FROM test_directory_listing_partitioned_a",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation("Input.readTail", "key=p1/"))
                        .add(new FileOperation("Input.readTail", "key=p2/"))
                        .add(new FileOperation("FileSystem.listFiles", "test_directory_listing_partitioned_a"))
                        .build());

        assertQuerySucceeds("CALL system.flush_metadata_cache(" +
                "schema_name => CURRENT_SCHEMA, " +
                "table_name => 'test_directory_listing_partitioned_a', " +
                "partition_columns => ARRAY['key']," +
                "partition_values => ARRAY['p1'])");
        assertFileSystemAccesses(
                "SELECT * FROM test_directory_listing_partitioned_a",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation("Input.readTail", "key=p1/"))
                        .add(new FileOperation("Input.readTail", "key=p2/"))
                        .add(new FileOperation("FileSystem.listFiles", "test_directory_listing_partitioned_a"))
                        .add(new FileOperation("FileSystem.listFiles", "key=p1"))
                        .build());
        assertFileSystemAccesses(
                "SELECT * FROM test_directory_listing_unpartitioned",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation("Input.readTail", "no partition"), 2)
                        .build());

        assertQuerySucceeds("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'test_directory_listing_partitioned_a')");
        assertFileSystemAccesses(
                "SELECT * FROM test_directory_listing_partitioned_a",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation("Input.readTail", "key=p1/"))
                        .add(new FileOperation("Input.readTail", "key=p2/"))
                        .add(new FileOperation("FileSystem.listFiles", "test_directory_listing_partitioned_a"))
                        .add(new FileOperation("FileSystem.listFiles", "key=p1"))
                        .add(new FileOperation("FileSystem.listFiles", "key=p2"))
                        .build());
        assertFileSystemAccesses(
                "SELECT * FROM test_directory_listing_unpartitioned",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation("Input.readTail", "no partition"), 2)
                        .build());

        assertQuerySucceeds("CALL system.flush_metadata_cache()");
        assertFileSystemAccesses(
                "SELECT * FROM test_directory_listing_partitioned_a",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation("Input.readTail", "key=p1/"))
                        .add(new FileOperation("Input.readTail", "key=p2/"))
                        .add(new FileOperation("FileSystem.listFiles", "test_directory_listing_partitioned_a"))
                        .add(new FileOperation("FileSystem.listFiles", "key=p1"))
                        .add(new FileOperation("FileSystem.listFiles", "key=p2"))
                        .build());
        assertFileSystemAccesses(
                "SELECT * FROM test_directory_listing_unpartitioned",
                ImmutableMultiset.<FileOperation>builder()
                        .addCopies(new FileOperation("Input.readTail", "no partition"), 2)
                        .add(new FileOperation("FileSystem.listFiles", "test_directory_listing_unpartitioned"))
                        .build());

        assertUpdate("DROP TABLE test_directory_listing_partitioned_a");
        assertUpdate("DROP TABLE test_directory_listing_unpartitioned");
    }

    private void assertFileSystemAccesses(@Language("SQL") String query, Multiset<FileOperation> expectedAccesses)
    {
        DistributedQueryRunner queryRunner = getDistributedQueryRunner();
        queryRunner.executeWithPlan(queryRunner.getDefaultSession(), query);
        assertMultisetsEqual(getFileOperations(), expectedAccesses);
    }

    private Multiset<FileOperation> getFileOperations()
    {
        return getQueryRunner().getSpans().stream()
                .filter(span -> span.getName().startsWith("Input.") || span.getName().startsWith("InputFile.") || span.getName().startsWith("FileSystem.list"))
                .filter(span -> !span.getName().startsWith("InputFile.newInput"))
                .filter(span -> !isTrinoSchemaOrPermissions(requireNonNull(span.getAttributes().get(FILE_LOCATION))))
                .map(FileOperation::createFileOperation)
                .collect(toCollection(HashMultiset::create));
    }

    private static final Pattern DATA_FILE_PATTERN = Pattern.compile(".*?/(?<partition>key=[^/]*/)?(?<queryId>\\d{8}_\\d{6}_\\d{5}_\\w{5})_(?<uuid>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})");

    private record FileOperation(String operationName, String fileName)
    {
        private static FileOperation createFileOperation(SpanData span)
        {
            String operationName = span.getName();
            String path = requireNonNull(span.getAttributes().get(FILE_LOCATION));
            String fileName = Location.of(path).fileName();

            if (!path.contains("/.trino")) {
                Matcher matcher = DATA_FILE_PATTERN.matcher(path);
                if (matcher.matches()) {
                    return new FileOperation(operationName, firstNonNull(matcher.group("partition"), "no partition"));
                }
            }
            return new FileOperation(operationName, fileName);
        }

        public FileOperation
        {
            requireNonNull(operationName, "operationName is null");
            requireNonNull(fileName, "fileName is null");
        }
    }

    private static boolean isTrinoSchemaOrPermissions(String path)
    {
        return path.endsWith(".trinoSchema") || path.contains(".trinoPermissions") || path.contains(".roleGrants") || path.contains(".roles");
    }
}
