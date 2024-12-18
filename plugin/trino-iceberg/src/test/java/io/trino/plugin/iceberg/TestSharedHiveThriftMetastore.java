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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.nio.file.Path;
import java.util.Map;

import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static java.lang.String.format;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestSharedHiveThriftMetastore
        extends BaseSharedMetastoreTest
{
    private static final String HIVE_CATALOG = "hive";
    private String bucketName;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        bucketName = "test-iceberg-shared-metastore" + randomNameSuffix();
        HiveMinioDataLake hiveMinioDataLake = closeAfterClass(new HiveMinioDataLake(bucketName));
        hiveMinioDataLake.start();

        Session icebergSession = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema(tpchSchema)
                .build();
        Session hiveSession = testSessionBuilder()
                .setCatalog(HIVE_CATALOG)
                .setSchema(tpchSchema)
                .build();

        QueryRunner queryRunner = DistributedQueryRunner.builder(icebergSession).build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        Path dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data");
        dataDirectory.toFile().deleteOnExit();

        queryRunner.installPlugin(new IcebergPlugin());
        queryRunner.createCatalog(
                ICEBERG_CATALOG,
                "iceberg",
                ImmutableMap.<String, String>builder()
                        .put("iceberg.catalog.type", "HIVE_METASTORE")
                        .put("hive.metastore.uri", hiveMinioDataLake.getHiveHadoop().getHiveMetastoreEndpoint().toString())
                        .put("hive.metastore.thrift.client.read-timeout", "1m") // read timed out sometimes happens with the default timeout
                        .put("fs.hadoop.enabled", "false")
                        .put("fs.native-s3.enabled", "true")
                        .put("s3.aws-access-key", MINIO_ACCESS_KEY)
                        .put("s3.aws-secret-key", MINIO_SECRET_KEY)
                        .put("s3.region", MINIO_REGION)
                        .put("s3.endpoint", hiveMinioDataLake.getMinio().getMinioAddress())
                        .put("s3.path-style-access", "true")
                        .put("s3.streaming.part-size", "5MB") // minimize memory usage
                        .put("s3.max-connections", "2") // verify no leaks
                        .put("iceberg.register-table-procedure.enabled", "true")
                        .put("iceberg.writer-sort-buffer-size", "1MB")
                        .buildOrThrow());
        queryRunner.createCatalog(
                "iceberg_with_redirections",
                "iceberg",
                ImmutableMap.<String, String>builder()
                        .put("iceberg.catalog.type", "HIVE_METASTORE")
                        .put("hive.metastore.uri", hiveMinioDataLake.getHiveHadoop().getHiveMetastoreEndpoint().toString())
                        .put("hive.metastore.thrift.client.read-timeout", "1m") // read timed out sometimes happens with the default timeout
                        .put("fs.hadoop.enabled", "false")
                        .put("fs.native-s3.enabled", "true")
                        .put("s3.aws-access-key", MINIO_ACCESS_KEY)
                        .put("s3.aws-secret-key", MINIO_SECRET_KEY)
                        .put("s3.region", MINIO_REGION)
                        .put("s3.endpoint", hiveMinioDataLake.getMinio().getMinioAddress())
                        .put("s3.path-style-access", "true")
                        .put("s3.streaming.part-size", "5MB") // minimize memory usage
                        .put("s3.max-connections", "2") // verify no leaks
                        .put("iceberg.register-table-procedure.enabled", "true")
                        .put("iceberg.writer-sort-buffer-size", "1MB")
                        .put("iceberg.hive-catalog-name", "hive")
                        .buildOrThrow());

        queryRunner.installPlugin(new TestingHivePlugin(dataDirectory));
        Map<String, String> hiveProperties = ImmutableMap.<String, String>builder()
                .put("hive.metastore", "thrift")
                .put("hive.metastore.uri", hiveMinioDataLake.getHiveHadoop().getHiveMetastoreEndpoint().toString())
                .put("fs.hadoop.enabled", "false")
                .put("fs.native-s3.enabled", "true")
                .put("s3.aws-access-key", MINIO_ACCESS_KEY)
                .put("s3.aws-secret-key", MINIO_SECRET_KEY)
                .put("s3.region", MINIO_REGION)
                .put("s3.endpoint", hiveMinioDataLake.getMinio().getMinioAddress())
                .put("s3.path-style-access", "true")
                .put("s3.streaming.part-size", "5MB")
                .put("hive.max-partitions-per-scan", "1000")
                .put("hive.max-partitions-for-eager-load", "1000")
                .put("hive.security", "allow-all")
                .buildOrThrow();
        queryRunner.createCatalog(HIVE_CATALOG, "hive", hiveProperties);
        queryRunner.createCatalog(
                "hive_with_redirections",
                "hive",
                ImmutableMap.<String, String>builder()
                        .putAll(hiveProperties).put("hive.iceberg-catalog-name", "iceberg")
                        .buildOrThrow());

        queryRunner.execute("CREATE SCHEMA " + tpchSchema + " WITH (location = 's3://" + bucketName + "/" + tpchSchema + "')");
        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, icebergSession, ImmutableList.of(TpchTable.NATION));
        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, hiveSession, ImmutableList.of(TpchTable.REGION));
        queryRunner.execute("CREATE SCHEMA " + testSchema + " WITH (location = 's3://" + bucketName + "/" + testSchema + "')");

        return queryRunner;
    }

    @AfterAll
    public void cleanup()
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS hive." + tpchSchema + ".region");
        assertQuerySucceeds("DROP TABLE IF EXISTS iceberg." + tpchSchema + ".nation");
        assertQuerySucceeds("DROP SCHEMA IF EXISTS hive." + tpchSchema);
        assertQuerySucceeds("DROP SCHEMA IF EXISTS hive." + testSchema);
    }

    @Override
    protected String getExpectedHiveCreateSchema(String catalogName)
    {
        return """
               CREATE SCHEMA %s.%s
               WITH (
                  location = 's3://%s/%s'
               )"""
                .formatted(catalogName, tpchSchema, bucketName, tpchSchema);
    }

    @Override
    protected String getExpectedIcebergCreateSchema(String catalogName)
    {
        String expectedIcebergCreateSchema = "CREATE SCHEMA %s.%s\n" +
                "AUTHORIZATION USER user\n" +
                "WITH (\n" +
                "   location = 's3://%s/%s'\n" +
                ")";
        return format(expectedIcebergCreateSchema, catalogName, tpchSchema, bucketName, tpchSchema);
    }
}
