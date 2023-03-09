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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.hdfs.ConfigurationInitializer;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfiguration;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsConfigurationProvider;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.s3.HiveS3Config;
import io.trino.plugin.hive.s3.TrinoS3ConfigurationInitializer;
import io.trino.plugin.iceberg.BaseIcebergConnectorSmokeTest;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.SchemaInitializer;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.containers.Minio;
import io.trino.tpch.TpchTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.DelegatingRestSessionCatalog;
import org.assertj.core.util.Files;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import static io.trino.plugin.hive.containers.HiveMinioDataLake.MINIO_DEFAULT_REGION;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkOrcFileSorting;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestIcebergRestCatalogConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private final String bucketName = "iceberg-rest-smoke-test-" + randomNameSuffix();
    private final String schemaName;

    private Minio minio;
    private TrinoFileSystemFactory fileSystemFactory;

    public TestIcebergRestCatalogConnectorSmokeTest()
    {
        super(new IcebergConfig().getFileFormat().toIceberg());
        this.schemaName = "tpch_" + format.name().toLowerCase(ENGLISH);
    }

    @SuppressWarnings("DuplicateBranchesInSwitch")
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_RENAME_SCHEMA -> false;
            case SUPPORTS_CREATE_VIEW, SUPPORTS_COMMENT_ON_VIEW, SUPPORTS_COMMENT_ON_VIEW_COLUMN -> false;
            case SUPPORTS_CREATE_MATERIALIZED_VIEW, SUPPORTS_RENAME_MATERIALIZED_VIEW -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.minio = closeAfterClass(
                Minio.builder()
                        .withEnvVars(ImmutableMap.<String, String>builder()
                                .put("MINIO_ACCESS_KEY", MINIO_ACCESS_KEY)
                                .put("MINIO_SECRET_KEY", MINIO_SECRET_KEY)
                                .put("MINIO_REGION", MINIO_DEFAULT_REGION)
                                .buildOrThrow())
                        .build());
        this.minio.start();
        this.minio.createBucket(bucketName);

        ConfigurationInitializer s3Config = new TrinoS3ConfigurationInitializer(new HiveS3Config()
                .setS3AwsAccessKey(MINIO_ACCESS_KEY)
                .setS3AwsSecretKey(MINIO_SECRET_KEY)
                .setS3Endpoint("http://" + minio.getMinioApiEndpoint())
                .setS3PathStyleAccess(true));
        HdfsConfig hdfsConfig = new HdfsConfig().setFileSystemMaxCacheSize(0);
        HdfsConfigurationInitializer initializer = new HdfsConfigurationInitializer(hdfsConfig, ImmutableSet.of(s3Config));
        HdfsConfiguration hdfsConfiguration = new DynamicHdfsConfiguration(initializer, ImmutableSet.of(new HdfsConfigurationProvider(hdfsConfig)));
        this.fileSystemFactory = new HdfsFileSystemFactory(new HdfsEnvironment(hdfsConfiguration, hdfsConfig, new NoHdfsAuthentication()));

        Catalog backend = jdbcBackedCatalog(hdfsConfiguration);

        DelegatingRestSessionCatalog delegatingCatalog = DelegatingRestSessionCatalog.builder()
                .delegate(backend)
                .build();

        TestingHttpServer testServer = delegatingCatalog.testServer();
        testServer.start();
        closeAfterClass(testServer::stop);

        return IcebergQueryRunner.builder()
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                // Disabling the fs cache is necessary to demonstrate that some FileSystems are not initialized properly
                                .put("hive.fs.cache.max-size", "0")
                                .put("iceberg.file-format", format.name())
                                .put("iceberg.catalog.type", "REST")
                                .put("iceberg.rest-catalog.uri", testServer.getBaseUrl().toString())
                                .put("iceberg.rest-catalog.warehouse", "s3://" + bucketName)
                                .put("hive.s3.aws-access-key", MINIO_ACCESS_KEY)
                                .put("hive.s3.aws-secret-key", MINIO_SECRET_KEY)
                                .put("hive.s3.endpoint", "http://" + minio.getMinioApiEndpoint())
                                .put("hive.s3.path-style-access", "true")
                                .put("hive.s3.streaming.part-size", "5MB")
                                .put("iceberg.register-table-procedure.enabled", "true")
                                .put("iceberg.writer-sort-buffer-size", "1MB")
                                .buildOrThrow())
                .setSchemaInitializer(
                        SchemaInitializer.builder()
                                .withSchemaName(schemaName)
                                .withClonedTpchTables(ImmutableList.<TpchTable<?>>builder()
                                        .addAll(REQUIRED_TPCH_TABLES)
                                        .add(LINE_ITEM)
                                        .build())
                                .withSchemaProperties(Map.of("location", "'s3://" + bucketName + "/" + schemaName + "'"))
                                .build())
                .build();
    }

    @Override
    public void testView()
    {
        assertThatThrownBy(super::testView)
                .hasMessageContaining("createView is not supported for Iceberg REST catalog");
    }

    @Override
    public void testMaterializedView()
    {
        assertThatThrownBy(super::testMaterializedView)
                .hasMessageContaining("createMaterializedView is not supported for Iceberg REST catalog");
    }

    @Override
    public void testRenameSchema()
    {
        assertThatThrownBy(super::testRenameSchema)
                .hasMessageContaining("renameNamespace is not supported for Iceberg REST catalog");
    }

    @Override
    protected void dropTableFromMetastore(String tableName)
    {
        // used when registering a table, which is not supported by the REST catalog
    }

    @Override
    protected String getMetadataLocation(String tableName)
    {
        // used when registering a table, which is not supported by the REST catalog
        throw new UnsupportedOperationException("metadata location for register_table is not supported");
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
        return !minio.createMinioClient().listObjects(bucketName, location.substring(prefix.length())).isEmpty();
    }

    @Override
    public void testRegisterTableWithTableLocation()
    {
        assertThatThrownBy(super::testRegisterTableWithTableLocation)
                .hasMessageContaining("registerTable is not supported for Iceberg REST catalog");
    }

    @Override
    public void testRegisterTableWithComments()
    {
        assertThatThrownBy(super::testRegisterTableWithComments)
                .hasMessageContaining("registerTable is not supported for Iceberg REST catalog");
    }

    @Override
    public void testRegisterTableWithShowCreateTable()
    {
        assertThatThrownBy(super::testRegisterTableWithShowCreateTable)
                .hasMessageContaining("registerTable is not supported for Iceberg REST catalog");
    }

    @Override
    public void testRegisterTableWithReInsert()
    {
        assertThatThrownBy(super::testRegisterTableWithReInsert)
                .hasMessageContaining("registerTable is not supported for Iceberg REST catalog");
    }

    @Override
    public void testRegisterTableWithDifferentTableName()
    {
        assertThatThrownBy(super::testRegisterTableWithDifferentTableName)
                .hasMessageContaining("registerTable is not supported for Iceberg REST catalog");
    }

    @Override
    public void testRegisterTableWithMetadataFile()
    {
        assertThatThrownBy(super::testRegisterTableWithMetadataFile)
                .hasMessageContaining("metadata location for register_table is not supported");
    }

    @Override
    public void testRegisterTableWithTrailingSpaceInLocation()
    {
        assertThatThrownBy(super::testRegisterTableWithTrailingSpaceInLocation)
                .hasMessageContaining("registerTable is not supported for Iceberg REST catalog");
    }

    @Override
    public void testUnregisterTable()
    {
        assertThatThrownBy(super::testUnregisterTable)
                .hasMessageContaining("unregisterTable is not supported for Iceberg REST catalogs");
    }

    @Override
    public void testUnregisterBrokenTable()
    {
        assertThatThrownBy(super::testUnregisterBrokenTable)
                .hasMessageContaining("unregisterTable is not supported for Iceberg REST catalogs");
    }

    @Override
    public void testUnregisterTableNotExistingTable()
    {
        assertThatThrownBy(super::testUnregisterTableNotExistingTable)
                .hasMessageContaining("unregisterTable is not supported for Iceberg REST catalogs");
    }

    @Override
    public void testRepeatUnregisterTable()
    {
        assertThatThrownBy(super::testRepeatUnregisterTable)
                .hasMessageContaining("unregisterTable is not supported for Iceberg REST catalogs");
    }

    @Override
    protected boolean isFileSorted(String path, String sortColumnName)
    {
        return checkOrcFileSorting(fileSystemFactory, path, sortColumnName);
    }

    @Override
    protected void deleteDirectory(String location)
    {
        // used when unregistering a table, which is not supported by the REST catalog
    }

    private Catalog jdbcBackedCatalog(HdfsConfiguration hdfsConfiguration)
            throws URISyntaxException
    {
        String warehouseLocation = "s3://" + bucketName;

        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        properties.put(CatalogProperties.URI, "jdbc:h2:file:" + Files.newTemporaryFile().getAbsolutePath());
        properties.put(JdbcCatalog.PROPERTY_PREFIX + "username", "user");
        properties.put(JdbcCatalog.PROPERTY_PREFIX + "password", "password");
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);

        JdbcCatalog catalog = new JdbcCatalog();
        catalog.setConf(hdfsConfiguration.getConfiguration(new HdfsContext(SESSION.getIdentity()), new URI(warehouseLocation)));
        catalog.initialize("backend_jdbc", properties.buildOrThrow());

        return catalog;
    }
}
