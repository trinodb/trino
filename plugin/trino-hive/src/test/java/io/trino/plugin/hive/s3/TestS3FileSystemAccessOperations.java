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
package io.trino.plugin.hive.s3;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.airlift.units.DataSize;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.trino.Session;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.metastore.HiveMetastoreConfig;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.plugin.hive.metastore.tracing.TracingHiveMetastore;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.Minio;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.trino.plugin.hive.HiveQueryRunner.TPCH_SCHEMA;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static java.util.stream.Collectors.toCollection;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD) // S3 request counters shares mutable state so can't be run from many threads simultaneously
public class TestS3FileSystemAccessOperations
        extends AbstractTestQueryFramework
{
    private static final String BUCKET = "test-bucket";

    private Minio minio;
    private InMemorySpanExporter spanExporter;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        minio = closeAfterClass(Minio.builder().build());
        minio.start();
        minio.createBucket(BUCKET);

        spanExporter = closeAfterClass(InMemorySpanExporter.create());

        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                .build();

        OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .build();

        return HiveQueryRunner.builder()
                .setMetastore(ignored -> new TracingHiveMetastore(
                        openTelemetry.getTracer("test"),
                        new FileHiveMetastore(
                                new NodeVersion("testversion"),
                                new S3FileSystemFactory(openTelemetry, new S3FileSystemConfig()
                                        .setAwsAccessKey(MINIO_ACCESS_KEY)
                                        .setAwsSecretKey(MINIO_SECRET_KEY)
                                        .setRegion(MINIO_REGION)
                                        .setEndpoint(minio.getMinioAddress())
                                        .setPathStyleAccess(true)),
                                new HiveMetastoreConfig().isHideDeltaLakeTables(),
                                new FileHiveMetastoreConfig()
                                        .setCatalogDirectory("s3://%s/catalog".formatted(BUCKET))
                                        .setDisableLocationChecks(true) // matches Glue behavior
                                        .setMetastoreUser("test"))))
                .setHiveProperties(ImmutableMap.<String, String>builder()
                        .put("fs.hadoop.enabled", "false")
                        .put("fs.native-s3.enabled", "true")
                        .put("s3.aws-access-key", MINIO_ACCESS_KEY)
                        .put("s3.aws-secret-key", MINIO_SECRET_KEY)
                        .put("s3.region", MINIO_REGION)
                        .put("s3.endpoint", minio.getMinioAddress())
                        .put("s3.path-style-access", "true")
                        .put("hive.non-managed-table-writes-enabled", "true")
                        .buildOrThrow())
                .setOpenTelemetry(openTelemetry)
                .setInitialSchemasLocationBase("s3://" + BUCKET)
                .build();
    }

    @AfterAll
    public void tearDown()
    {
        // closed by closeAfterClass
        spanExporter = null;
        minio = null;
    }

    @Test
    public void testSelectWithFilter()
    {
        for (StorageFormat format : StorageFormat.values()) {
            assertUpdate("DROP TABLE IF EXISTS test_select_from_where");
            String tableLocation = randomTableLocation("test_select_from_where");

            assertUpdate("CREATE TABLE test_select_from_where WITH (format = '" + format + "', external_location = '" + tableLocation + "') AS SELECT 2 AS age", 1);

            assertFileSystemAccesses(
                    withSmallFileThreshold(getSession(), DataSize.valueOf("1MB")), // large enough threshold for single request of small file
                    "SELECT * FROM test_select_from_where WHERE age = 2",
                    ImmutableMultiset.<String>builder()
                            .add("S3.GetObject")
                            .add("S3.ListObjectsV2")
                            .build());

            assertFileSystemAccesses(
                    withSmallFileThreshold(getSession(), DataSize.valueOf("10B")), // disables single request for small file
                    "SELECT * FROM test_select_from_where WHERE age = 2",
                    ImmutableMultiset.<String>builder()
                            .addCopies("S3.GetObject", occurrences(format, 3, 2))
                            .add("S3.ListObjectsV2")
                            .build());

            assertUpdate("DROP TABLE test_select_from_where");
        }
    }

    @Test
    public void testSelectPartitionTable()
    {
        for (StorageFormat format : StorageFormat.values()) {
            assertUpdate("DROP TABLE IF EXISTS test_select_from_partition");
            String tableLocation = randomTableLocation("test_select_from_partition");

            assertUpdate("CREATE TABLE test_select_from_partition (data int, key varchar)" +
                    "WITH (partitioned_by = ARRAY['key'], format = '" + format + "', external_location = '" + tableLocation + "')");
            assertUpdate("INSERT INTO test_select_from_partition VALUES (1, 'part1'), (2, 'part2')", 2);

            assertFileSystemAccesses("SELECT * FROM test_select_from_partition",
                    ImmutableMultiset.<String>builder()
                            .addCopies("S3.GetObject", 2)
                            .addCopies("S3.ListObjectsV2", 2)
                            .build());

            assertFileSystemAccesses("SELECT * FROM test_select_from_partition WHERE key = 'part1'",
                    ImmutableMultiset.<String>builder()
                            .add("S3.GetObject")
                            .add("S3.ListObjectsV2")
                            .build());

            assertUpdate("INSERT INTO test_select_from_partition VALUES (11, 'part1')", 1);
            assertFileSystemAccesses("SELECT * FROM test_select_from_partition WHERE key = 'part1'",
                    ImmutableMultiset.<String>builder()
                            .addCopies("S3.GetObject", 2)
                            .addCopies("S3.ListObjectsV2", 1)
                            .build());

            assertUpdate("DROP TABLE test_select_from_partition");
        }
    }

    private static String randomTableLocation(String tableName)
    {
        return "s3://%s/%s/%s-%s".formatted(BUCKET, TPCH_SCHEMA, tableName, randomNameSuffix());
    }

    private void assertFileSystemAccesses(@Language("SQL") String query, Multiset<String> expectedAccesses)
    {
        assertFileSystemAccesses(getDistributedQueryRunner().getDefaultSession(), query, expectedAccesses);
    }

    private void assertFileSystemAccesses(Session session, @Language("SQL") String query, Multiset<String> expectedAccesses)
    {
        DistributedQueryRunner queryRunner = getDistributedQueryRunner();
        spanExporter.reset();
        queryRunner.executeWithQueryId(session, query);
        assertMultisetsEqual(getOperations(), expectedAccesses);
    }

    private Multiset<String> getOperations()
    {
        List<SpanData> items = spanExporter.getFinishedSpanItems();
        Map<String, SpanData> spansById = uniqueIndex(items, SpanData::getSpanId);
        return items.stream()
                .filter(span -> span.getName().startsWith("S3."))
                .filter(span -> Optional.ofNullable(span.getParentSpanId())
                        .map(spansById::get)
                        .map(parent -> !parent.getName().startsWith("HiveMetastore."))
                        .orElse(true))
                .map(SpanData::getName)
                .collect(toCollection(HashMultiset::create));
    }

    private static int occurrences(StorageFormat tableType, int orcValue, int parquetValue)
    {
        checkArgument(!(orcValue == parquetValue), "No need to use Occurrences when ORC and Parquet");
        return switch (tableType) {
            case ORC -> orcValue;
            case PARQUET -> parquetValue;
        };
    }

    private static Session withSmallFileThreshold(Session session, DataSize sizeThreshold)
    {
        String catalog = session.getCatalog().orElseThrow();
        return Session.builder(session)
                .setCatalogSessionProperty(catalog, "parquet_small_file_threshold", sizeThreshold.toString())
                .setCatalogSessionProperty(catalog, "orc_tiny_stripe_threshold", sizeThreshold.toString())
                .build();
    }

    enum StorageFormat
    {
        ORC,
        PARQUET,
    }
}
