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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.deltalake.util.DockerizedMinioDataLake;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.testng.annotations.AfterClass;

import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.deltalake.DeltaLakeDockerizedMinioDataLake.createDockerizedMinioDataLakeForDeltaLake;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.createS3DeltaLakeQueryRunner;
import static io.trino.plugin.deltalake.util.MinioContainer.MINIO_ACCESS_KEY;
import static io.trino.plugin.deltalake.util.MinioContainer.MINIO_SECRET_KEY;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;

public class TestDeltaLakeSharedHiveMetastore
        extends BaseDeltaLakeSharedMetastoreTest
{
    private final String bucketName = "delta-lake-shared-hive-" + randomTableSuffix();

    private DockerizedMinioDataLake dockerizedMinioDataLake;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session deltaLakeSession = testSessionBuilder()
                .setCatalog("delta")
                .setSchema(schema)
                .build();
        Session hiveSession = testSessionBuilder()
                .setCatalog("hive")
                .setSchema(schema)
                .build();

        this.dockerizedMinioDataLake = closeAfterClass(createDockerizedMinioDataLakeForDeltaLake(bucketName, Optional.empty()));

        DistributedQueryRunner queryRunner = createS3DeltaLakeQueryRunner(
                "delta",
                schema,
                ImmutableMap.<String, String>builder()
                        .put("delta.enable-non-concurrent-writes", "true")
                        .buildOrThrow(),
                dockerizedMinioDataLake.getMinioAddress(),
                dockerizedMinioDataLake.getTestingHadoop());
        queryRunner.execute("CREATE SCHEMA " + schema + " WITH (location = 's3://" + bucketName + "/" + schema + "')");

        queryRunner.installPlugin(new TestingHivePlugin());
        Map<String, String> s3Properties = ImmutableMap.<String, String>builder()
                .put("hive.s3.aws-access-key", MINIO_ACCESS_KEY)
                .put("hive.s3.aws-secret-key", MINIO_SECRET_KEY)
                .put("hive.s3.endpoint", dockerizedMinioDataLake.getMinioAddress())
                .put("hive.s3.path-style-access", "true")
                .buildOrThrow();
        queryRunner.createCatalog(
                "hive",
                "hive",
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore.uri", dockerizedMinioDataLake.getTestingHadoop().getMetastoreAddress())
                        .put("hive.allow-drop-table", "true")
                        .putAll(s3Properties)
                        .buildOrThrow());

        queryRunner.createCatalog(
                "delta_with_redirections",
                "delta-lake",
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore.uri", dockerizedMinioDataLake.getTestingHadoop().getMetastoreAddress())
                        .putAll(s3Properties)
                        .put("delta.hive-catalog-name", "hive")
                        .buildOrThrow());

        queryRunner.createCatalog(
                "hive_with_redirections",
                "hive",
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore.uri", dockerizedMinioDataLake.getTestingHadoop().getMetastoreAddress())
                        .putAll(s3Properties)
                        .put("hive.delta-lake-catalog-name", "delta")
                        .buildOrThrow());

        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, deltaLakeSession, ImmutableList.of(TpchTable.NATION));
        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, hiveSession, ImmutableList.of(TpchTable.REGION));

        return queryRunner;
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS hive." + schema + ".region");
        assertQuerySucceeds("DROP TABLE IF EXISTS delta." + schema + ".nation");
        assertQuerySucceeds("DROP SCHEMA IF EXISTS hive." + schema);
    }

    @Override
    protected String getExpectedHiveCreateSchema(String catalogName)
    {
        String expectedHiveCreateSchema = "CREATE SCHEMA %s.%s\n" +
                "AUTHORIZATION USER user\n" +
                "WITH (\n" +
                "   location = 's3://%s/%s'\n" +
                ")";
        return format(expectedHiveCreateSchema, catalogName, schema, bucketName, schema);
    }

    @Override
    protected String getExpectedDeltaLakeCreateSchema(String catalogName)
    {
        String expectedDeltaLakeCreateSchema = "CREATE SCHEMA %s.%s\n" +
                "WITH (\n" +
                "   location = 's3://%s/%s'\n" +
                ")";
        return format(expectedDeltaLakeCreateSchema, catalogName, schema, bucketName, schema);
    }
}
