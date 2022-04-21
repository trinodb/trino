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

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.FileFormat;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.plugin.hive.containers.HiveMinioDataLake.ACCESS_KEY;
import static io.trino.plugin.hive.containers.HiveMinioDataLake.SECRET_KEY;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertTrue;

public abstract class BaseIcebergMinioConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private final String schemaName;
    private final String bucketName;

    private HiveMinioDataLake hiveMinioDataLake;

    public BaseIcebergMinioConnectorSmokeTest(FileFormat format)
    {
        super(format);
        this.schemaName = "tpch_" + format.name().toLowerCase(Locale.ENGLISH);
        this.bucketName = "test-iceberg-minio-smoke-test-" + randomTableSuffix();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.hiveMinioDataLake = closeAfterClass(new HiveMinioDataLake(bucketName, ImmutableMap.of()));
        this.hiveMinioDataLake.start();

        return IcebergQueryRunner.builder()
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.file-format", format.name())
                                .put("iceberg.catalog.type", "HIVE_METASTORE")
                                .put("hive.metastore.uri", "thrift://" + hiveMinioDataLake.getHiveHadoop().getHiveMetastoreEndpoint())
                                .put("hive.s3.aws-access-key", ACCESS_KEY)
                                .put("hive.s3.aws-secret-key", SECRET_KEY)
                                .put("hive.s3.endpoint", "http://" + hiveMinioDataLake.getMinio().getMinioApiEndpoint())
                                .put("hive.s3.path-style-access", "true")
                                .put("hive.s3.streaming.part-size", "5MB")
                                .buildOrThrow())
                .setSchemaInitializer(
                        SchemaInitializer.builder()
                                .withSchemaName(schemaName)
                                .withClonedTpchTables(REQUIRED_TPCH_TABLES)
                                .withSchemaProperties(Map.of("location", "'s3://" + bucketName + "/" + schemaName + "'"))
                                .build())
                .build();
    }

    @Override
    protected String createSchemaSql(String schemaName)
    {
        return "CREATE SCHEMA IF NOT EXISTS " + schemaName + " WITH (location = 's3://" + bucketName + "/" + schemaName + "')";
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        assertQueryFails(
                format("ALTER SCHEMA %s RENAME TO %s", schemaName, schemaName + randomTableSuffix()),
                "Hive metastore does not support renaming schemas");
    }

    // TODO: https://github.com/trinodb/trino/issues/11713 Run this test against Glue
    // Repeat test with invocationCount for better test coverage, since the tested aspect is inherently non-deterministic.
    @Test(timeOut = 60_000, invocationCount = 4)
    public void testDeleteRowsConcurrently()
            throws Exception
    {
        int threads = 4;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_concurrent_delete",
                "(col0 INTEGER, col1 INTEGER, col2 INTEGER, col3 INTEGER)")) {
            String tableName = table.getName();
            assertUpdate("INSERT INTO " + tableName + " VALUES (0, 0, 0, 0)", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 0, 0, 0)", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (0, 1, 0, 0)", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (0, 0, 1, 0)", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (0, 0, 0, 1)", 1);

            List<Future<Boolean>> futures = IntStream.range(0, threads)
                    .mapToObj(threadNumber -> executor.submit(() -> {
                        barrier.await(10, SECONDS);
                        try {
                            String columnName = "col" + threadNumber;
                            getQueryRunner().execute(format("DELETE FROM %s WHERE %s = 1", tableName, columnName));
                            return true;
                        }
                        catch (Exception e) {
                            return false;
                        }
                    }))
                    .collect(toImmutableList());

            futures.forEach(future -> assertTrue(getFutureValue(future)));
            assertThat(query("SELECT max(col0), max(col1), max(col2), max(col3) FROM " + tableName)).matches("VALUES (0, 0, 0, 0)");
        }
        finally {
            executor.shutdownNow();
            executor.awaitTermination(10, SECONDS);
        }
    }
}
