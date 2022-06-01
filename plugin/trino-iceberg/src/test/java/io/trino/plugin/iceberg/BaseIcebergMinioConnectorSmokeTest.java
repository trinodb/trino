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
import org.apache.iceberg.FileFormat;
import org.testng.annotations.Test;

import java.util.Locale;
import java.util.Map;

import static io.trino.plugin.hive.containers.HiveMinioDataLake.ACCESS_KEY;
import static io.trino.plugin.hive.containers.HiveMinioDataLake.SECRET_KEY;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;

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
}
