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
import io.airlift.http.server.HttpConfig;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.ServerFeature;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.node.NodeInfo;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemStats;
import io.trino.plugin.iceberg.BaseIcebergConnectorSmokeTest;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.containers.Floci;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.QuotedETagRestCatalogServlet;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.Credentials;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.Optional;

import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Floci.FLOCI_ACCESS_KEY;
import static io.trino.testing.containers.Floci.FLOCI_REGION;
import static io.trino.testing.containers.Floci.FLOCI_SECRET_KEY;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestIcebergS3VendingRestCatalogConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private final String bucketName;
    private String warehouseLocation;
    private JdbcCatalog backend;
    private Floci floci;

    public TestIcebergS3VendingRestCatalogConnectorSmokeTest()
    {
        super(new IcebergConfig().getFileFormat().toIceberg());
        this.bucketName = "test-iceberg-vending-rest-connector-smoke-test-" + randomNameSuffix();
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

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        floci = closeAfterClass(new Floci());
        floci.start();
        floci.createBucket(bucketName);

        this.warehouseLocation = "s3://%s/default/".formatted(bucketName);

        backend = closeAfterClass(buildBackendCatalog());

        Credentials sessionCredentials = getSessionCredentials();

        VendedCredentialsRestCatalogAdapter adapter = new VendedCredentialsRestCatalogAdapter(backend)
        {
            @Override
            public Map<String, String> getVendedCredentialsConfig(String restServerUri)
            {
                return ImmutableMap.<String, String>builder()
                        .put(AwsClientProperties.CLIENT_REGION, FLOCI_REGION)
                        .put(S3FileIOProperties.ACCESS_KEY_ID, sessionCredentials.accessKeyId())
                        .put(S3FileIOProperties.SECRET_ACCESS_KEY, sessionCredentials.secretAccessKey())
                        .put(S3FileIOProperties.SESSION_TOKEN, sessionCredentials.sessionToken())
                        .buildOrThrow();
            }
        };

        QuotedETagRestCatalogServlet servlet = new QuotedETagRestCatalogServlet(adapter);

        NodeInfo nodeInfo = new NodeInfo("test");
        HttpServerConfig config = new HttpServerConfig()
                .setHttpEnabled(true);
        HttpServerInfo httpServerInfo = new HttpServerInfo(config, Optional.of(new HttpConfig().setHttpPort(0)), Optional.empty(), nodeInfo);
        TestingHttpServer testServer = new TestingHttpServer("rest-catalog", httpServerInfo, nodeInfo, config, servlet, ServerFeature.builder()
                .withLegacyUriCompliance(true)
                .build());
        testServer.start();
        closeAfterClass(testServer::stop);

        return IcebergQueryRunner.builder()
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.file-format", format.name())
                                .put("iceberg.catalog.type", "rest")
                                .put("iceberg.rest-catalog.uri", testServer.getBaseUrl().toString())
                                .put("iceberg.rest-catalog.vended-credentials-enabled", "true")
                                .put("iceberg.writer-sort-buffer-size", "1MB")
                                .put("fs.s3.enabled", "true")
                                .put("s3.endpoint", floci.endpoint().toString())
                                .put("s3.region", FLOCI_REGION)
                                .put("s3.path-style-access", "true")
                                .buildOrThrow())
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    private JdbcCatalog buildBackendCatalog()
            throws IOException
    {
        JdbcCatalog catalog = new JdbcCatalog();
        catalog.initialize("backend_jdbc", ImmutableMap.<String, String>builder()
                .put(CatalogProperties.URI, "jdbc:h2:file:" + Files.createTempFile(null, null).toAbsolutePath())
                .put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation)
                .put(CatalogProperties.FILE_IO_IMPL, S3FileIO.class.getName())
                .put(JdbcCatalog.PROPERTY_PREFIX + "username", "user")
                .put(JdbcCatalog.PROPERTY_PREFIX + "password", "password")
                .put(JdbcCatalog.PROPERTY_PREFIX + "schema-version", "V1")
                .put(S3FileIOProperties.PATH_STYLE_ACCESS, "true")
                .put(S3FileIOProperties.ACCESS_KEY_ID, FLOCI_ACCESS_KEY)
                .put(S3FileIOProperties.SECRET_ACCESS_KEY, FLOCI_SECRET_KEY)
                .put(S3FileIOProperties.ENDPOINT, floci.endpoint().toString())
                .put(AwsClientProperties.CLIENT_REGION, FLOCI_REGION)
                .buildOrThrow());
        return catalog;
    }

    private Credentials getSessionCredentials()
    {
        try (StsClient stsClient = StsClient.builder().applyMutation(floci::updateClient).build()) {
            return stsClient.assumeRole(AssumeRoleRequest.builder()
                    .roleArn("arn:aws:iam::000000000000:role/test")
                    .roleSessionName("test")
                    .build()).credentials();
        }
    }

    @Override
    @BeforeAll
    public void initFileSystem()
    {
        this.fileSystem = new S3FileSystemFactory(
                OpenTelemetry.noop(),
                new S3FileSystemConfig()
                        .setEndpoint(floci.endpoint().toString())
                        .setRegion(FLOCI_REGION)
                        .setPathStyleAccess(true)
                        .setAwsAccessKey(FLOCI_ACCESS_KEY)
                        .setAwsSecretKey(FLOCI_SECRET_KEY),
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
        // TODO: Get register table tests working
    }

    @Override
    protected String getMetadataLocation(String tableName)
    {
        TableIdentifier identifier = TableIdentifier.of(getSession().getSchema().orElseThrow(), tableName);
        return ((BaseTable) backend.loadTable(identifier)).operations().current().metadataFileLocation();
    }

    @Override
    protected String schemaPath()
    {
        return format("%s%s", warehouseLocation, getSession().getSchema().orElseThrow());
    }

    @Override
    protected boolean locationExists(String location)
    {
        return !floci.listObjects(bucketName, keyFromLocation(location)).isEmpty();
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
                .hasMessageContaining("Failed to load table");
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
                .hasMessageContaining("Failed to load table");
    }

    @Override
    protected void deleteDirectory(String location)
    {
        String key = keyFromLocation(location);
        floci.deleteObjects(bucketName, key);
        assertThat(floci.listObjects(bucketName, key)).isEmpty();
    }

    private String keyFromLocation(String location)
    {
        return location.substring(("s3://" + bucketName + "/").length());
    }

    private TableIdentifier toIdentifier(String tableName)
    {
        return TableIdentifier.of(getSession().getSchema().orElseThrow(), tableName);
    }
}
