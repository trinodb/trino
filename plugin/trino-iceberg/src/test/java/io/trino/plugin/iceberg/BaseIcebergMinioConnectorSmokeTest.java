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
import com.google.common.collect.ImmutableSet;
import io.minio.messages.Event;
import io.trino.Session;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.hdfs.ConfigurationInitializer;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfiguration;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.trino.plugin.hive.s3.HiveS3Config;
import io.trino.plugin.hive.s3.TrinoS3ConfigurationInitializer;
import io.trino.testing.QueryRunner;
import io.trino.testing.minio.MinioClient;
import org.apache.iceberg.FileFormat;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.hive.TestingThriftHiveMetastoreBuilder.testingThriftHiveMetastoreBuilder;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseIcebergMinioConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    protected final TrinoFileSystemFactory fileSystemFactory;

    private final String schemaName;
    private final String bucketName;

    private HiveMinioDataLake hiveMinioDataLake;

    public BaseIcebergMinioConnectorSmokeTest(FileFormat format)
    {
        super(format);
        this.schemaName = "tpch_" + format.name().toLowerCase(ENGLISH);
        this.bucketName = "test-iceberg-minio-smoke-test-" + randomNameSuffix();

        ConfigurationInitializer s3Config = new TrinoS3ConfigurationInitializer(new HiveS3Config()
                .setS3AwsAccessKey(MINIO_ACCESS_KEY)
                .setS3AwsSecretKey(MINIO_SECRET_KEY));
        HdfsConfigurationInitializer initializer = new HdfsConfigurationInitializer(new HdfsConfig(), ImmutableSet.of(s3Config));
        HdfsConfiguration hdfsConfiguration = new DynamicHdfsConfiguration(initializer, ImmutableSet.of());
        this.fileSystemFactory = new HdfsFileSystemFactory(new HdfsEnvironment(hdfsConfiguration, new HdfsConfig(), new NoHdfsAuthentication()));
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
                                .put("iceberg.register-table-procedure.enabled", "true")
                                .put("iceberg.writer-sort-buffer-size", "1MB")
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
                format("ALTER SCHEMA %s RENAME TO %s", schemaName, schemaName + randomNameSuffix()),
                "Hive metastore does not support renaming schemas");
    }

    @Test
    public void testS3LocationWithTrailingSlash()
    {
        // Verify data and metadata files' uri don't contain fragments
        String schemaName = getSession().getSchema().orElseThrow();
        String tableName = "test_s3_location_with_trailing_slash_" + randomNameSuffix();
        String location = "s3://%s/%s/%s/".formatted(bucketName, schemaName, tableName);
        assertThat(location).doesNotContain("#");

        assertUpdate("CREATE TABLE " + tableName + " WITH (location='" + location + "') AS SELECT 1 col", 1);

        List<String> dataFiles = hiveMinioDataLake.getMinioClient().listObjects(bucketName, "/%s/%s/data".formatted(schemaName, tableName));
        assertThat(dataFiles).isNotEmpty().filteredOn(filePath -> filePath.contains("#")).isEmpty();

        List<String> metadataFiles = hiveMinioDataLake.getMinioClient().listObjects(bucketName, "/%s/%s/metadata".formatted(schemaName, tableName));
        assertThat(metadataFiles).isNotEmpty().filteredOn(filePath -> filePath.contains("#")).isEmpty();

        // Verify ALTER TABLE succeeds https://github.com/trinodb/trino/issues/14552
        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN new_col int");
        assertTableColumnNames(tableName, "col", "new_col");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testMetadataLocationWithDoubleSlash()
    {
        // Regression test for https://github.com/trinodb/trino/issues/14299
        String schemaName = getSession().getSchema().orElseThrow();
        String tableName = "test_meatdata_location_with_double_slash_" + randomNameSuffix();

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

    @Test
    public void testExpireSnapshotsBatchDeletes()
    {
        String tableName = "test_expiring_snapshots_" + randomNameSuffix();
        Session sessionWithShortRetentionUnlocked = prepareCleanUpSession();
        String location = "s3://%s/%s/%s/".formatted(bucketName, schemaName, tableName);
        Queue<Event> events = new ConcurrentLinkedQueue<>();
        hiveMinioDataLake.getMinioClient().captureBucketNotifications(bucketName, event -> {
            if (event.eventType().toString().toLowerCase(ENGLISH).contains("remove")) {
                events.add(event);
            }
        });

        assertUpdate("CREATE TABLE " + tableName + " (key varchar, value integer) WITH (location='" + location + "')");
        assertUpdate("INSERT INTO " + tableName + " VALUES ('one', 1)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES ('two', 2)", 1);
        assertThat(query("SELECT * FROM " + tableName)).matches("VALUES (VARCHAR 'one', 1), (VARCHAR 'two', 2)");

        List<String> initialMetadataFiles = hiveMinioDataLake.getMinioClient().listObjects(bucketName, "/%s/%s/metadata".formatted(schemaName, tableName));
        assertThat(initialMetadataFiles).isNotEmpty();

        List<Long> initialSnapshots = getSnapshotIds(tableName);
        assertThat(initialSnapshots).hasSizeGreaterThan(1);

        assertQuerySucceeds(sessionWithShortRetentionUnlocked, "ALTER TABLE " + tableName + " EXECUTE EXPIRE_SNAPSHOTS (retention_threshold => '0s')");

        List<String> updatedMetadataFiles = hiveMinioDataLake.getMinioClient().listObjects(bucketName, "/%s/%s/metadata".formatted(schemaName, tableName));
        assertThat(updatedMetadataFiles).isNotEmpty().hasSizeLessThan(initialMetadataFiles.size());

        List<Long> updatedSnapshots = getSnapshotIds(tableName);
        assertThat(updatedSnapshots).hasSize(1);

        assertThat(query("SELECT * FROM " + tableName))
                .matches("VALUES (VARCHAR 'one', 1), (VARCHAR 'two', 2)");
        assertThat(events).hasSize(2);
        // if files were deleted in batch there should be only one request id because there was one request only
        assertThat(events.stream()
                .map(event -> event.responseElements().get("x-amz-request-id"))
                .collect(toImmutableSet())).hasSize(1);

        assertUpdate("DROP TABLE " + tableName);
    }

    private String onMetastore(@Language("SQL") String sql)
    {
        return hiveMinioDataLake.getHiveHadoop().runOnMetastore(sql);
    }

    private Session prepareCleanUpSession()
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", "expire_snapshots_min_retention", "0s")
                .build();
    }

    private List<Long> getSnapshotIds(String tableName)
    {
        return getQueryRunner().execute(format("SELECT snapshot_id FROM \"%s$snapshots\"", tableName))
                .getOnlyColumn()
                .map(Long.class::cast)
                .collect(toImmutableList());
    }

    @Override
    protected void dropTableFromMetastore(String tableName)
    {
        HiveMetastore metastore = new BridgingHiveMetastore(
                testingThriftHiveMetastoreBuilder()
                        .metastoreClient(hiveMinioDataLake.getHiveHadoop().getHiveMetastoreEndpoint())
                        .build());
        metastore.dropTable(schemaName, tableName, false);
        assertThat(metastore.getTable(schemaName, tableName)).isEmpty();
    }

    @Override
    protected String getMetadataLocation(String tableName)
    {
        HiveMetastore metastore = new BridgingHiveMetastore(
                testingThriftHiveMetastoreBuilder()
                        .metastoreClient(hiveMinioDataLake.getHiveHadoop().getHiveMetastoreEndpoint())
                        .build());
        return metastore
                .getTable(schemaName, tableName).orElseThrow()
                .getParameters().get("metadata_location");
    }

    @Override
    protected String schemaPath()
    {
        return format("s3://%s/%s", bucketName, schemaName);
    }

    @Override
    protected boolean locationExists(String location)
    {
        String prefix = "s3://" + bucketName + "/";
        return !hiveMinioDataLake.listFiles(location.substring(prefix.length())).isEmpty();
    }

    @Override
    protected void deleteDirectory(String location)
    {
        String prefix = "s3://" + bucketName + "/";
        String key = location.substring(prefix.length());

        MinioClient minio = hiveMinioDataLake.getMinioClient();
        for (String file : minio.listObjects(bucketName, key)) {
            minio.removeObject(bucketName, file);
        }
        assertThat(minio.listObjects(bucketName, key)).isEmpty();
    }
}
