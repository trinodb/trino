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
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.metastore.HiveMetastoreConfig;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.Minio;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.hive.HiveQueryRunner.TPCH_SCHEMA;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.testing.DataProviders.toDataProvider;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static java.util.stream.Collectors.toCollection;

@Test(singleThreaded = true) // S3 request counters shares mutable state so can't be run from many threads simultaneously
public class TestTrinoS3FileSystemAccessOperations
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
                .setMetastore(distributedQueryRunner -> {
                    File baseDir = distributedQueryRunner.getCoordinator().getBaseDataDir().resolve("hive_data").toFile();
                    return new FileHiveMetastore(
                            new NodeVersion("testversion"),
                            HDFS_FILE_SYSTEM_FACTORY,
                            new HiveMetastoreConfig().isHideDeltaLakeTables(),
                            new FileHiveMetastoreConfig()
                                    .setCatalogDirectory(baseDir.toURI().toString())
                                    .setDisableLocationChecks(true) // matches Glue behavior
                                    .setMetastoreUser("test"));
                })
                .setHiveProperties(ImmutableMap.<String, String>builder()
                        .put("hive.s3.aws-access-key", MINIO_ACCESS_KEY)
                        .put("hive.s3.aws-secret-key", MINIO_SECRET_KEY)
                        .put("hive.s3.endpoint", minio.getMinioAddress())
                        .put("hive.s3.path-style-access", "true")
                        .put("hive.non-managed-table-writes-enabled", "true")
                        .buildOrThrow())
                .setOpenTelemetry(openTelemetry)
                .setInitialSchemasLocationBase("s3://" + BUCKET)
                .build();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        // closed by closeAfterClass
        spanExporter = null;
        minio = null;
    }

    @Test(dataProvider = "storageFormats")
    public void testSelectWithFilter(StorageFormat format)
    {
        assertUpdate("DROP TABLE IF EXISTS test_select_from_where");
        String tableLocation = randomTableLocation("test_select_from_where");

        assertUpdate("CREATE TABLE test_select_from_where WITH (format = '" + format + "', external_location = '" + tableLocation + "') AS SELECT 2 AS age", 1);

        assertFileSystemAccesses("SELECT * FROM test_select_from_where WHERE age = 2",
                ImmutableMultiset.<String>builder()
                        // TODO https://github.com/trinodb/trino/issues/18334 Reduce GetObject call for Parquet format
                        .addCopies("S3.GetObject", occurrences(format, 1, 2))
                        .add("S3.ListObjectsV2")
                        .addCopies("S3.GetObjectMetadata", occurrences(format, 1, 0))
                        .build());

        assertUpdate("DROP TABLE test_select_from_where");
    }

    @Test(dataProvider = "storageFormats")
    public void testSelectPartitionTable(StorageFormat format)
    {
        assertUpdate("DROP TABLE IF EXISTS test_select_from_partition");
        String tableLocation = randomTableLocation("test_select_from_partition");

        assertUpdate("CREATE TABLE test_select_from_partition (data int, key varchar)" +
                "WITH (partitioned_by = ARRAY['key'], format = '" + format + "', external_location = '" + tableLocation + "')");
        assertUpdate("INSERT INTO test_select_from_partition VALUES (1, 'part1'), (2, 'part2')", 2);

        assertFileSystemAccesses("SELECT * FROM test_select_from_partition",
                ImmutableMultiset.<String>builder()
                        // TODO https://github.com/trinodb/trino/issues/18334 Reduce GetObject call for Parquet format
                        .addCopies("S3.GetObject", occurrences(format, 2, 4))
                        .addCopies("S3.ListObjectsV2", 2)
                        .addCopies("S3.GetObjectMetadata", occurrences(format, 2, 0))
                        .build());

        assertFileSystemAccesses("SELECT * FROM test_select_from_partition WHERE key = 'part1'",
                ImmutableMultiset.<String>builder()
                        // TODO https://github.com/trinodb/trino/issues/18334 Reduce GetObject call for Parquet format
                        .addCopies("S3.GetObject", occurrences(format, 1, 2))
                        .add("S3.ListObjectsV2")
                        .addCopies("S3.GetObjectMetadata", occurrences(format, 1, 0))
                        .build());

        assertUpdate("INSERT INTO test_select_from_partition VALUES (11, 'part1')", 1);
        assertFileSystemAccesses("SELECT * FROM test_select_from_partition WHERE key = 'part1'",
                ImmutableMultiset.<String>builder()
                        // TODO https://github.com/trinodb/trino/issues/18334 Reduce GetObject call for Parquet format
                        .addCopies("S3.GetObject", occurrences(format, 2, 4))
                        .addCopies("S3.ListObjectsV2", 1)
                        .addCopies("S3.GetObjectMetadata", occurrences(format, 2, 0))
                        .build());

        assertUpdate("DROP TABLE test_select_from_partition");
    }

    private static String randomTableLocation(String tableName)
    {
        return "s3://%s/%s/%s-%s".formatted(BUCKET, TPCH_SCHEMA, tableName, randomNameSuffix());
    }

    private void assertFileSystemAccesses(@Language("SQL") String query, Multiset<String> expectedAccesses)
    {
        DistributedQueryRunner queryRunner = getDistributedQueryRunner();
        spanExporter.reset();
        queryRunner.executeWithQueryId(queryRunner.getDefaultSession(), query);
        assertMultisetsEqual(getOperations(), expectedAccesses);
    }

    private Multiset<String> getOperations()
    {
        return spanExporter.getFinishedSpanItems().stream()
                .map(SpanData::getName)
                .collect(toCollection(HashMultiset::create));
    }

    @DataProvider
    public static Object[][] storageFormats()
    {
        return Arrays.stream(StorageFormat.values())
                .collect(toDataProvider());
    }

    private static int occurrences(StorageFormat tableType, int orcValue, int parquetValue)
    {
        checkArgument(!(orcValue == parquetValue), "No need to use Occurrences when ORC and Parquet");
        return switch (tableType) {
            case ORC -> orcValue;
            case PARQUET -> parquetValue;
        };
    }

    enum StorageFormat
    {
        ORC,
        PARQUET,
    }
}
