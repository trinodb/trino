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
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import static io.trino.plugin.hive.containers.HiveMinioDataLake.MINIO_ACCESS_KEY;
import static io.trino.plugin.hive.containers.HiveMinioDataLake.MINIO_SECRET_KEY;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

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
        this.hiveMinioDataLake = closeAfterClass(new HiveMinioDataLake(bucketName));
        this.hiveMinioDataLake.start();

        return IcebergQueryRunner.builder()
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.file-format", format.name())
                                .put("iceberg.catalog.type", "HIVE_METASTORE")
                                .put("hive.metastore.uri", "thrift://" + hiveMinioDataLake.getHiveHadoop().getHiveMetastoreEndpoint())
                                .put("hive.metastore-timeout", "1m") // read timed out sometimes happens with the default timeout
                                .put("hive.s3.aws-access-key", MINIO_ACCESS_KEY)
                                .put("hive.s3.aws-secret-key", MINIO_SECRET_KEY)
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

    @Test
    public void testS3LocationWithTrailingSlash()
    {
        // Verify data and metadata files' uri don't contain fragments
        String schemaName = getSession().getSchema().orElseThrow();
        String tableName = "test_s3_location_with_trailing_slash_" + randomTableSuffix();
        String location = "s3://%s/%s/%s/".formatted(bucketName, schemaName, tableName);
        assertThat(location).doesNotContain("#");

        assertUpdate("CREATE TABLE " + tableName + " WITH (location='" + location + "') AS SELECT 1 col", 1);

        List<String> dataFiles = hiveMinioDataLake.getMinioClient().listObjects(bucketName, "/%s/%s/data".formatted(schemaName, tableName));
        assertThat(dataFiles).isNotEmpty().filteredOn(filePath -> filePath.contains("#")).isEmpty();

        List<String> metadataFiles = hiveMinioDataLake.getMinioClient().listObjects(bucketName, "/%s/%s/metadata".formatted(schemaName, tableName));
        assertThat(metadataFiles).isNotEmpty().filteredOn(filePath -> filePath.contains("#")).isEmpty();

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testMetadataLocationWithDoubleSlash()
    {
        // Regression test for https://github.com/trinodb/trino/issues/14299
        String schemaName = getSession().getSchema().orElseThrow();
        String tableName = "test_meatdata_location_with_double_slash_" + randomTableSuffix();

        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 col", 1);

        // Update metadata location to contain double slash
        String tableId = onMetastore("SELECT tbl_id FROM TBLS t INNER JOIN DBS db ON t.db_id = db.db_id WHERE db.name = '" + schemaName + "' and t.tbl_name = '" + tableName + "'");
        String metadataLocation = onMetastore("SELECT param_value FROM TABLE_PARAMS WHERE param_key = 'metadata_location' AND tbl_id = " + tableId);

        // Simulate corrupted metadata location as Trino 393-394 was doing
        String newMetadataLocation = metadataLocation.replace("/metadata/", "//metadata/");
        onMetastore("UPDATE TABLE_PARAMS SET param_value = '" + newMetadataLocation + "' WHERE tbl_id = " + tableId + " AND param_key = 'metadata_location'");

        // Confirm read and write operations succeed
        assertQuery("SELECT * FROM " + tableName, "VALUES 1");
        assertUpdate("INSERT INTO " + tableName + " VALUES 2", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES (1), (2)");

        assertUpdate("DROP TABLE " + tableName);
    }

    private String onMetastore(@Language("SQL") String sql)
    {
        return hiveMinioDataLake.getHiveHadoop().runOnMetastore(sql);
    }
}
