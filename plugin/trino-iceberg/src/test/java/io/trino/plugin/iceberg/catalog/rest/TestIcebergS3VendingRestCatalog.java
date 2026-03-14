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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.common.collect.ImmutableMap;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.ServerFeature;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.node.NodeInfo;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.Minio;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.RestCatalogServlet;
import org.apache.iceberg.rest.S3CredentialVendingCatalogAdapter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_ROOT_PASSWORD;
import static io.trino.testing.containers.Minio.MINIO_ROOT_USER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
class TestIcebergS3VendingRestCatalog
        extends AbstractTestQueryFramework
{
    private static final String BUCKET_NAME = "test-iceberg-s3-vending-" + randomNameSuffix();

    private Minio minio;
    private JdbcCatalog backendCatalog;
    private TestingHttpServer restServer;

    @SuppressWarnings("resource")
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        minio = Minio.builder().build();
        minio.start();
        minio.createBucket(BUCKET_NAME);

        String minioAddress = minio.getMinioAddress();

        backendCatalog = createBackendCatalog(minioAddress);

        // Get real STS session credentials from Minio (fake tokens are rejected)
        Credentials stsCredentials;
        try (StsClient stsClient = StsClient.builder()
                .endpointOverride(URI.create(minioAddress))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(MINIO_ROOT_USER, MINIO_ROOT_PASSWORD)))
                .region(Region.of(MINIO_REGION))
                .build()) {
            AssumeRoleResponse assumeRoleResponse = stsClient.assumeRole(AssumeRoleRequest.builder().build());
            stsCredentials = assumeRoleResponse.credentials();
        }

        // S3 properties that the REST catalog will vend to Trino
        Map<String, String> vendedS3Properties = ImmutableMap.<String, String>builder()
                .put("s3.access-key-id", stsCredentials.accessKeyId())
                .put("s3.secret-access-key", stsCredentials.secretAccessKey())
                .put("s3.session-token", stsCredentials.sessionToken())
                .put("client.region", MINIO_REGION)
                .put("s3.endpoint", minioAddress)
                .put("s3.cross-region-access-enabled", "true")
                .buildOrThrow();

        S3CredentialVendingCatalogAdapter adapter = new S3CredentialVendingCatalogAdapter(backendCatalog, vendedS3Properties);
        RestCatalogServlet servlet = new RestCatalogServlet(adapter);

        NodeInfo nodeInfo = new NodeInfo("test");
        HttpServerConfig httpConfig = new HttpServerConfig()
                .setHttpPort(0)
                .setHttpEnabled(true);
        HttpServerInfo httpServerInfo = new HttpServerInfo(httpConfig, nodeInfo);
        restServer = new TestingHttpServer("s3-vending-rest-catalog", httpServerInfo, nodeInfo, httpConfig, servlet, ServerFeature.builder()
                .withLegacyUriCompliance(true)
                .build());
        restServer.start();

        return IcebergQueryRunner.builder()
                .setIcebergProperties(ImmutableMap.<String, String>builder()
                        .put("iceberg.catalog.type", "rest")
                        .put("iceberg.rest-catalog.uri", restServer.getBaseUrl().toString())
                        .put("iceberg.rest-catalog.vended-credentials-enabled", "true")
                        .put("fs.native-s3.enabled", "true")
                        .put("s3.region", MINIO_REGION)
                        .put("s3.endpoint", minioAddress)
                        .put("s3.path-style-access", "true")
                        .buildOrThrow())
                .build();
    }

    @AfterAll
    public void tearDown()
    {
        if (restServer != null) {
            try {
                restServer.stop();
            }
            catch (Exception e) {
                // ignore
            }
        }
        if (backendCatalog != null) {
            try {
                backendCatalog.close();
            }
            catch (Exception e) {
                // ignore
            }
        }
        if (minio != null) {
            minio.close();
        }
    }

    @Test
    void testCreateTableInsertAndSelect()
    {
        String tableName = "test_s3_vending_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, name VARCHAR)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'alice'), (2, 'bob')", 2);
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'alice'), (2, 'bob')");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testCreateTableAsSelect()
    {
        String tableName = "test_s3_ctas_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 AS id, VARCHAR 'hello' AS greeting", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'hello')");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testTableLocationIsOnS3()
    {
        String tableName = "test_s3_location_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 AS id", 1);

        String filePath = (String) computeScalar("SELECT file_path FROM \"" + tableName + "$files\" LIMIT 1");
        assertThat(filePath).startsWith("s3://" + BUCKET_NAME + "/");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testSchemaOperations()
    {
        String schemaName = "test_s3_schema_" + randomNameSuffix();
        assertUpdate("CREATE SCHEMA " + schemaName);
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).contains(schemaName);

        String tableName = schemaName + ".test_table_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1)", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES 1");

        assertUpdate("DROP TABLE " + tableName);
        assertUpdate("DROP SCHEMA " + schemaName);
    }

    @Test
    void testUpdateAndDelete()
    {
        String tableName = "test_s3_update_delete_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, value VARCHAR)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'original'), (2, 'keep'), (3, 'remove')", 3);

        assertUpdate("UPDATE " + tableName + " SET value = 'updated' WHERE id = 1", 1);
        assertQuery("SELECT value FROM " + tableName + " WHERE id = 1", "VALUES 'updated'");

        assertUpdate("DELETE FROM " + tableName + " WHERE id = 3", 1);
        assertQuery("SELECT * FROM " + tableName + " ORDER BY id", "VALUES (1, 'updated'), (2, 'keep')");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testPartitionedTable()
    {
        String tableName = "test_s3_partitioned_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, region VARCHAR) WITH (partitioning = ARRAY['region'])");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'us'), (2, 'eu'), (3, 'us')", 3);

        assertQuery("SELECT id FROM " + tableName + " WHERE region = 'us' ORDER BY id", "VALUES 1, 3");
        assertQuery("SELECT id FROM " + tableName + " WHERE region = 'eu'", "VALUES 2");

        long partitionCount = (long) computeScalar("SELECT count(*) FROM \"" + tableName + "$partitions\"");
        assertThat(partitionCount).isEqualTo(2L);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testSnapshotsAndTimeTravel()
    {
        String tableName = "test_s3_snapshots_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER)");

        assertUpdate("INSERT INTO " + tableName + " VALUES (1)", 1);
        long snapshotAfterFirstInsert = (long) computeScalar("SELECT snapshot_id FROM \"" + tableName + "$snapshots\" ORDER BY committed_at DESC LIMIT 1");

        assertUpdate("INSERT INTO " + tableName + " VALUES (2)", 1);
        assertQuery("SELECT * FROM " + tableName + " ORDER BY id", "VALUES 1, 2");

        // Time travel to first snapshot
        assertQuery("SELECT * FROM " + tableName + " FOR VERSION AS OF " + snapshotAfterFirstInsert, "VALUES 1");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testMetadataTables()
    {
        String tableName = "test_s3_metadata_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, part VARCHAR) WITH (partitioning = ARRAY['part'])");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'b')", 1);

        long snapshotCount = (long) computeScalar("SELECT count(*) FROM \"" + tableName + "$snapshots\"");
        assertThat(snapshotCount).isGreaterThanOrEqualTo(2L);

        long fileCount = (long) computeScalar("SELECT count(*) FROM \"" + tableName + "$files\"");
        assertThat(fileCount).isEqualTo(2L);

        long historyCount = (long) computeScalar("SELECT count(*) FROM \"" + tableName + "$history\"");
        assertThat(historyCount).isGreaterThanOrEqualTo(2L);

        long partitionCount = (long) computeScalar("SELECT count(*) FROM \"" + tableName + "$partitions\"");
        assertThat(partitionCount).isEqualTo(2L);

        // Verify file paths are on S3
        String filePath = (String) computeScalar("SELECT file_path FROM \"" + tableName + "$files\" LIMIT 1");
        assertThat(filePath).startsWith("s3://" + BUCKET_NAME + "/");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testMultipleDataTypes()
    {
        String tableName = "test_s3_types_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (" +
                "c_integer INTEGER, " +
                "c_bigint BIGINT, " +
                "c_real REAL, " +
                "c_double DOUBLE, " +
                "c_decimal DECIMAL(10,2), " +
                "c_varchar VARCHAR, " +
                "c_boolean BOOLEAN, " +
                "c_date DATE, " +
                "c_timestamp TIMESTAMP(6))");

        assertUpdate("INSERT INTO " + tableName + " VALUES (" +
                "42, " +
                "BIGINT '9999999999', " +
                "REAL '3.14', " +
                "DOUBLE '2.718281828', " +
                "DECIMAL '12345.67', " +
                "VARCHAR 'hello world', " +
                "true, " +
                "DATE '2024-01-15', " +
                "TIMESTAMP '2024-01-15 10:30:00.123456')", 1);

        assertQuery("SELECT c_integer, c_bigint, c_varchar, c_boolean FROM " + tableName,
                "VALUES (42, 9999999999, 'hello world', true)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testCreateOrReplaceTable()
    {
        String tableName = "test_s3_create_or_replace_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 AS id, VARCHAR 'v1' AS version", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'v1')");

        long v1SnapshotId = (long) computeScalar("SELECT snapshot_id FROM \"" + tableName + "$snapshots\" ORDER BY committed_at DESC LIMIT 1");

        assertUpdate("CREATE OR REPLACE TABLE " + tableName + " AS SELECT 2 AS id, VARCHAR 'v2' AS version", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES (2, 'v2')");

        // Time travel to original version
        assertQuery("SELECT * FROM " + tableName + " FOR VERSION AS OF " + v1SnapshotId, "VALUES (1, 'v1')");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testAlterTable()
    {
        String tableName = "test_s3_alter_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1)", 1);

        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN name VARCHAR");
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'alice')", 1);

        assertQuery("SELECT * FROM " + tableName + " ORDER BY id", "VALUES (1, NULL), (2, 'alice')");

        assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN name TO full_name");
        assertQuery("SELECT id, full_name FROM " + tableName + " WHERE id = 2", "VALUES (2, 'alice')");

        assertUpdate("DROP TABLE " + tableName);
    }

    private static JdbcCatalog createBackendCatalog(String minioAddress)
            throws IOException
    {
        Path tempFile = Files.createTempFile("iceberg-s3-test-jdbc", null);
        tempFile.toFile().deleteOnExit();

        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        properties.put(CatalogProperties.URI, "jdbc:h2:file:" + tempFile.toAbsolutePath());
        properties.put(JdbcCatalog.PROPERTY_PREFIX + "username", "user");
        properties.put(JdbcCatalog.PROPERTY_PREFIX + "password", "password");
        properties.put(JdbcCatalog.PROPERTY_PREFIX + "schema-version", "V1");
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, "s3://" + BUCKET_NAME + "/warehouse");
        properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
        properties.put("s3.access-key-id", MINIO_ROOT_USER);
        properties.put("s3.secret-access-key", MINIO_ROOT_PASSWORD);
        properties.put("s3.endpoint", minioAddress);
        properties.put("s3.path-style-access", "true");
        properties.put("client.region", MINIO_REGION);

        JdbcCatalog catalog = new JdbcCatalog();
        catalog.setConf(new Configuration());
        catalog.initialize("s3_backend", properties.buildOrThrow());

        return catalog;
    }
}
