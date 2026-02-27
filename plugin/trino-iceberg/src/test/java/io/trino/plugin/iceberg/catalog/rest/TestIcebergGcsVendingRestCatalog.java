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
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.GcsCredentialVendingCatalogAdapter;
import org.apache.iceberg.rest.RestCatalogServlet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
class TestIcebergGcsVendingRestCatalog
        extends AbstractTestQueryFramework
{
    private static final String BUCKET_NAME = "test-iceberg-gcs-vending-" + randomNameSuffix();
    private static final int FAKE_GCS_PORT = 4443;

    private GenericContainer<?> fakeGcsServer;
    private JdbcCatalog backendCatalog;
    private TestingHttpServer restServer;

    @SuppressWarnings("resource")
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        fakeGcsServer = new GenericContainer<>("fsouza/fake-gcs-server:1.49.3")
                .withExposedPorts(FAKE_GCS_PORT)
                .withCommand("-scheme", "http", "-port", String.valueOf(FAKE_GCS_PORT))
                .waitingFor(new HttpWaitStrategy()
                        .forPort(FAKE_GCS_PORT)
                        .forPath("/storage/v1/b")
                        .forStatusCode(200));
        fakeGcsServer.start();

        String fakeGcsUrl = "http://%s:%d".formatted(fakeGcsServer.getHost(), fakeGcsServer.getMappedPort(FAKE_GCS_PORT));

        updateExternalUrl(fakeGcsUrl);
        createBucket(fakeGcsUrl, BUCKET_NAME);

        backendCatalog = createBackendCatalog(fakeGcsUrl);

        // GCS properties that the REST catalog will vend to Trino
        Map<String, String> vendedGcsProperties = ImmutableMap.of(
                "gcs.no-auth", "true",
                "gcs.project-id", "test-project",
                "gcs.service.host", fakeGcsUrl);

        GcsCredentialVendingCatalogAdapter adapter = new GcsCredentialVendingCatalogAdapter(backendCatalog, vendedGcsProperties);
        RestCatalogServlet servlet = new RestCatalogServlet(adapter);

        NodeInfo nodeInfo = new NodeInfo("test");
        HttpServerConfig httpConfig = new HttpServerConfig()
                .setHttpPort(0)
                .setHttpEnabled(true);
        HttpServerInfo httpServerInfo = new HttpServerInfo(httpConfig, nodeInfo);
        restServer = new TestingHttpServer("gcs-vending-rest-catalog", httpServerInfo, nodeInfo, httpConfig, servlet, ServerFeature.builder()
                .withLegacyUriCompliance(true)
                .build());
        restServer.start();

        return IcebergQueryRunner.builder()
                .setIcebergProperties(ImmutableMap.<String, String>builder()
                        .put("iceberg.catalog.type", "rest")
                        .put("iceberg.rest-catalog.uri", restServer.getBaseUrl().toString())
                        .put("iceberg.rest-catalog.vended-credentials-enabled", "true")
                        .put("fs.native-gcs.enabled", "true")
                        .put("gcs.project-id", "test-project")
                        .put("gcs.endpoint", fakeGcsUrl)
                        .put("gcs.auth-type", "APPLICATION_DEFAULT")
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
        if (fakeGcsServer != null) {
            fakeGcsServer.close();
        }
    }

    @Test
    void testCreateTableInsertAndSelect()
    {
        String tableName = "test_gcs_vending_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, name VARCHAR)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'alice'), (2, 'bob')", 2);
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'alice'), (2, 'bob')");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testCreateTableAsSelect()
    {
        String tableName = "test_gcs_ctas_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 AS id, VARCHAR 'hello' AS greeting", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'hello')");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testTableLocationIsOnGcs()
    {
        String tableName = "test_gcs_location_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 AS id", 1);

        String filePath = (String) computeScalar("SELECT file_path FROM \"" + tableName + "$files\" LIMIT 1");
        assertThat(filePath).startsWith("gs://" + BUCKET_NAME + "/");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testSchemaOperations()
    {
        String schemaName = "test_gcs_schema_" + randomNameSuffix();
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
        String tableName = "test_gcs_update_delete_" + randomNameSuffix();
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
        String tableName = "test_gcs_partitioned_" + randomNameSuffix();
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
        String tableName = "test_gcs_snapshots_" + randomNameSuffix();
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
        String tableName = "test_gcs_metadata_" + randomNameSuffix();
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

        // Verify file paths are on GCS
        String filePath = (String) computeScalar("SELECT file_path FROM \"" + tableName + "$files\" LIMIT 1");
        assertThat(filePath).startsWith("gs://" + BUCKET_NAME + "/");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testMultipleDataTypes()
    {
        String tableName = "test_gcs_types_" + randomNameSuffix();
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
        String tableName = "test_gcs_create_or_replace_" + randomNameSuffix();
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
        String tableName = "test_gcs_alter_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INTEGER)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1)", 1);

        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN name VARCHAR");
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'alice')", 1);

        assertQuery("SELECT * FROM " + tableName + " ORDER BY id", "VALUES (1, NULL), (2, 'alice')");

        assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN name TO full_name");
        assertQuery("SELECT id, full_name FROM " + tableName + " WHERE id = 2", "VALUES (2, 'alice')");

        assertUpdate("DROP TABLE " + tableName);
    }

    private static void updateExternalUrl(String fakeGcsUrl)
            throws IOException, InterruptedException
    {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(fakeGcsUrl + "/_internal/config"))
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString("{\"externalUrl\": \"" + fakeGcsUrl + "\"}"))
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
    }

    private static void createBucket(String fakeGcsUrl, String bucketName)
            throws IOException, InterruptedException
    {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(fakeGcsUrl + "/storage/v1/b?project=test-project"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"name\": \"" + bucketName + "\"}"))
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
    }

    private static JdbcCatalog createBackendCatalog(String fakeGcsUrl)
            throws IOException
    {
        Path tempFile = Files.createTempFile("iceberg-gcs-test-jdbc", null);
        tempFile.toFile().deleteOnExit();

        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        properties.put(CatalogProperties.URI, "jdbc:h2:file:" + tempFile.toAbsolutePath());
        properties.put(JdbcCatalog.PROPERTY_PREFIX + "username", "user");
        properties.put(JdbcCatalog.PROPERTY_PREFIX + "password", "password");
        properties.put(JdbcCatalog.PROPERTY_PREFIX + "schema-version", "V1");
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, "gs://" + BUCKET_NAME + "/warehouse");
        properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.gcp.gcs.GCSFileIO");
        properties.put("gcs.project-id", "test-project");
        properties.put("gcs.service.host", fakeGcsUrl);
        properties.put("gcs.no-auth", "true");

        JdbcCatalog catalog = new JdbcCatalog();
        catalog.setConf(new Configuration());
        catalog.initialize("gcs_backend", properties.buildOrThrow());

        return catalog;
    }
}
