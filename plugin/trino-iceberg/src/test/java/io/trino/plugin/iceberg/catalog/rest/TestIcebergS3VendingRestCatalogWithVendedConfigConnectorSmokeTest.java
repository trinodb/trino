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
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.Location;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemStats;
import io.trino.plugin.iceberg.BaseIcebergConnectorSmokeTest;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.containers.Minio;
import io.trino.testing.minio.MinioClient;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.RESTCatalogServlet;
import org.apache.iceberg.rest.S3CredentialVendingCatalogAdapter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.trino.plugin.iceberg.IcebergTestUtils.checkOrcFileSorting;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkParquetFileSorting;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_ROOT_PASSWORD;
import static io.trino.testing.containers.Minio.MINIO_ROOT_USER;
import static java.lang.String.format;
import static org.apache.iceberg.FileFormat.PARQUET;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Smoke test that verifies vended region, endpoint, and cross-region access
 * properties from the Iceberg REST catalog are consumed correctly.
 * <p>
 * Unlike {@link TestIcebergS3VendingRestCatalogConnectorSmokeTest}, this test
 * does NOT set {@code s3.region} or {@code s3.endpoint} in the static Trino
 * catalog config. The S3 client must get these from the vended properties via
 * {@link VendedCredentialsS3SecurityMappingProvider}. If vended properties
 * are not consumed, all S3 operations will fail because the S3 client won't
 * know which region/endpoint to connect to.
 */
public class TestIcebergS3VendingRestCatalogWithVendedConfigConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private final String bucketName;
    private String warehouseLocation;
    private Minio minio;
    private JdbcCatalog backendCatalog;
    private TestingHttpServer restServer;

    public TestIcebergS3VendingRestCatalogWithVendedConfigConnectorSmokeTest()
    {
        super(new IcebergConfig().getFileFormat().toIceberg());
        this.bucketName = "test-s3-vended-config-smoke-" + randomNameSuffix();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_RENAME_MATERIALIZED_VIEW,
                 SUPPORTS_RENAME_SCHEMA -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @SuppressWarnings("resource")
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        minio = closeAfterClass(Minio.builder().build());
        minio.start();
        minio.createBucket(bucketName);

        this.warehouseLocation = "s3://%s/warehouse/".formatted(bucketName);
        String minioAddress = minio.getMinioAddress();

        backendCatalog = closeAfterClass(createBackendCatalog(minioAddress));

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

        // Vend region, endpoint, and cross-region access along with credentials
        Map<String, String> vendedS3Properties = ImmutableMap.<String, String>builder()
                .put(S3FileIOProperties.ACCESS_KEY_ID, stsCredentials.accessKeyId())
                .put(S3FileIOProperties.SECRET_ACCESS_KEY, stsCredentials.secretAccessKey())
                .put(S3FileIOProperties.SESSION_TOKEN, stsCredentials.sessionToken())
                .put(AwsClientProperties.CLIENT_REGION, MINIO_REGION)
                .put(S3FileIOProperties.ENDPOINT, minioAddress)
                .put(S3FileIOProperties.CROSS_REGION_ACCESS_ENABLED, "true")
                .put(S3FileIOProperties.PATH_STYLE_ACCESS, "true")
                .buildOrThrow();

        S3CredentialVendingCatalogAdapter adapter = new S3CredentialVendingCatalogAdapter(backendCatalog, vendedS3Properties);
        RESTCatalogServlet servlet = new RESTCatalogServlet(adapter);

        NodeInfo nodeInfo = new NodeInfo("test");
        HttpServerConfig httpConfig = new HttpServerConfig()
                .setHttpPort(0)
                .setHttpEnabled(true);
        HttpServerInfo httpServerInfo = new HttpServerInfo(httpConfig, nodeInfo);
        restServer = new TestingHttpServer("s3-vended-config-smoke", httpServerInfo, nodeInfo, httpConfig, servlet, ServerFeature.builder()
                .withLegacyUriCompliance(true)
                .build());
        restServer.start();
        closeAfterClass(restServer::stop);

        // s3.region is set as a baseline default so the S3 client can initialize
        // in environments without AWS_REGION (e.g., CI). The vended endpoint and
        // cross-region access properties are still consumed from the REST catalog
        // response via VendedCredentialsS3SecurityMappingProvider.
        return IcebergQueryRunner.builder()
                .setIcebergProperties(ImmutableMap.<String, String>builder()
                        .put("iceberg.file-format", format.name())
                        .put("iceberg.catalog.type", "rest")
                        .put("iceberg.rest-catalog.uri", restServer.getBaseUrl().toString())
                        .put("iceberg.rest-catalog.vended-credentials-enabled", "true")
                        .put("iceberg.writer-sort-buffer-size", "1MB")
                        .put("fs.s3.enabled", "true")
                        .put("s3.region", MINIO_REGION)
                        .buildOrThrow())
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    @BeforeAll
    public void initFileSystem()
    {
        this.fileSystem = new S3FileSystemFactory(
                OpenTelemetry.noop(),
                new S3FileSystemConfig()
                        .setRegion(MINIO_REGION)
                        .setEndpoint(minio.getMinioAddress())
                        .setPathStyleAccess(true)
                        .setAwsAccessKey(MINIO_ROOT_USER)
                        .setAwsSecretKey(MINIO_ROOT_PASSWORD),
                new S3FileSystemStats()).create(SESSION);
    }

    @Test
    @Override
    public void testMaterializedView()
    {
        assertThatThrownBy(super::testMaterializedView)
                .hasMessageContaining("createMaterializedView is not supported for Iceberg REST catalog");
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        assertThatThrownBy(super::testRenameSchema)
                .hasMessageContaining("renameNamespace is not supported for Iceberg REST catalog");
    }

    @Override
    protected void dropTableFromCatalog(String tableName)
    {
        // register_table is not supported for REST catalog
    }

    @Override
    protected String getMetadataLocation(String tableName)
    {
        return ((BaseTable) backendCatalog.loadTable(toIdentifier(tableName)))
                .operations().current().metadataFileLocation();
    }

    @Override
    protected String schemaPath()
    {
        return format("%s%s", warehouseLocation, getSession().getSchema().orElseThrow());
    }

    @Override
    protected boolean locationExists(String location)
    {
        try {
            return fileSystem.directoryExists(Location.of(location)).orElse(false);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    @Override
    public void testRegisterTableWithTableLocation()
    {
        assertThatThrownBy(super::testRegisterTableWithTableLocation)
                .hasMessageContaining("register_table procedure is disabled");
    }

    @Test
    @Override
    public void testRegisterTableWithComments()
    {
        assertThatThrownBy(super::testRegisterTableWithComments)
                .hasMessageContaining("register_table procedure is disabled");
    }

    @Test
    @Override
    public void testRegisterTableWithShowCreateTable()
    {
        assertThatThrownBy(super::testRegisterTableWithShowCreateTable)
                .hasMessageContaining("register_table procedure is disabled");
    }

    @Test
    @Override
    public void testRegisterTableWithReInsert()
    {
        assertThatThrownBy(super::testRegisterTableWithReInsert)
                .hasMessageContaining("register_table procedure is disabled");
    }

    @Test
    @Override
    public void testRegisterTableWithDroppedTable()
    {
        assertThatThrownBy(super::testRegisterTableWithDroppedTable)
                .hasMessageContaining("register_table procedure is disabled");
    }

    @Test
    @Override
    public void testRegisterTableWithDifferentTableName()
    {
        assertThatThrownBy(super::testRegisterTableWithDifferentTableName)
                .hasMessageContaining("register_table procedure is disabled");
    }

    @Test
    @Override
    public void testRegisterTableWithMetadataFile()
    {
        assertThatThrownBy(super::testRegisterTableWithMetadataFile)
                .hasMessageContaining("register_table procedure is disabled");
    }

    @Test
    @Override
    public void testRegisterTableWithTrailingSpaceInLocation()
    {
        assertThatThrownBy(super::testRegisterTableWithTrailingSpaceInLocation)
                .hasMessageContaining("register_table procedure is disabled");
    }

    @Test
    @Override
    public void testUnregisterTable()
    {
        assertThatThrownBy(super::testUnregisterTable)
                .hasMessageContaining("register_table procedure is disabled");
    }

    @Test
    @Override
    public void testRepeatUnregisterTable()
    {
        assertThatThrownBy(super::testRepeatUnregisterTable)
                .hasMessageContaining("register_table procedure is disabled");
    }

    @Test
    @Override
    public void testDropTableWithMissingMetadataFile()
    {
        assertThatThrownBy(super::testDropTableWithMissingMetadataFile)
                .hasMessageMatching("Failed to load table: (.*)");
    }

    @Test
    @Override
    public void testDropTableWithMissingSnapshotFile()
    {
        assertThatThrownBy(super::testDropTableWithMissingSnapshotFile)
                .isInstanceOf(QueryFailedException.class)
                .cause()
                .hasMessageContaining("Failed to drop table")
                .hasNoCause();
    }

    @Test
    @Override
    public void testDropTableWithMissingManifestListFile()
    {
        assertThatThrownBy(super::testDropTableWithMissingManifestListFile)
                .hasMessageContaining("Table location should not exist");
    }

    @Test
    @Override
    public void testDropTableWithNonExistentTableLocation()
    {
        assertThatThrownBy(super::testDropTableWithNonExistentTableLocation)
                .hasMessageMatching("Failed to load table: (.*)");
    }

    @Override
    protected boolean isFileSorted(Location path, String sortColumnName)
    {
        if (format == PARQUET) {
            return checkParquetFileSorting(fileSystem.newInputFile(path), sortColumnName);
        }
        return checkOrcFileSorting(fileSystem, path, sortColumnName);
    }

    @Override
    protected void deleteDirectory(String location)
    {
        try (MinioClient minioClient = minio.createMinioClient()) {
            String prefix = "s3://" + bucketName + "/";
            String key = location.substring(prefix.length());

            for (String file : minioClient.listObjects(bucketName, key)) {
                minioClient.removeObject(bucketName, file);
            }
            assertThat(minioClient.listObjects(bucketName, key)).isEmpty();
        }
    }

    private TableIdentifier toIdentifier(String tableName)
    {
        return TableIdentifier.of(getSession().getSchema().orElseThrow(), tableName);
    }

    private JdbcCatalog createBackendCatalog(String minioAddress)
            throws IOException
    {
        Path tempFile = Files.createTempFile("iceberg-s3-vended-config-smoke-jdbc", null);
        tempFile.toFile().deleteOnExit();

        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        properties.put(CatalogProperties.URI, "jdbc:h2:file:" + tempFile.toAbsolutePath());
        properties.put(JdbcCatalog.PROPERTY_PREFIX + "username", "user");
        properties.put(JdbcCatalog.PROPERTY_PREFIX + "password", "password");
        properties.put(JdbcCatalog.PROPERTY_PREFIX + "schema-version", "V1");
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);
        properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
        properties.put(S3FileIOProperties.ACCESS_KEY_ID, MINIO_ROOT_USER);
        properties.put(S3FileIOProperties.SECRET_ACCESS_KEY, MINIO_ROOT_PASSWORD);
        properties.put(S3FileIOProperties.ENDPOINT, minioAddress);
        properties.put(S3FileIOProperties.PATH_STYLE_ACCESS, "true");
        properties.put(AwsClientProperties.CLIENT_REGION, MINIO_REGION);

        JdbcCatalog catalog = new JdbcCatalog();
        catalog.initialize("s3_vended_config_smoke_backend", properties.buildOrThrow());

        return catalog;
    }
}
