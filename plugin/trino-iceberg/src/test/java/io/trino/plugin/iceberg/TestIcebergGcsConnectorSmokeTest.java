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

import com.google.cloud.NoCredentials;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import com.google.cloud.storage.BucketInfo;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.gcs.GcsAuth;
import io.trino.filesystem.gcs.GcsFileSystemConfig;
import io.trino.filesystem.gcs.GcsFileSystemFactory;
import io.trino.filesystem.gcs.GcsStorageFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.hive.containers.HiveHadoop;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.containers.FlociGcp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Optional;

import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.hive.TestingThriftHiveMetastoreBuilder.testingThriftHiveMetastoreBuilder;
import static io.trino.plugin.hive.containers.HiveHadoop.HIVE3_IMAGE;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkOrcFileSorting;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.FlociGcp.FLOCI_GCP_PROJECT_ID;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.iceberg.FileFormat.ORC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.testcontainers.containers.Network.newNetwork;

@TestInstance(PER_CLASS)
public class TestIcebergGcsConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private static final Logger LOG = Logger.get(TestIcebergGcsConnectorSmokeTest.class);

    private static final FileAttribute<?> READ_ONLY_PERMISSIONS = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-r--r--"));
    private static final String HADOOP_GCS_CONNECTOR = "/usr/hdp/3.1.0.0-78/hadoop-mapreduce/gcs-connector-1.9.10.3.1.0.0-78-shaded.jar";
    private static final String TEZ_GCS_CONNECTOR = "/usr/hdp/3.1.0.0-78/tez/lib/gcs-connector-1.9.10.3.1.0.0-78-shaded.jar";

    private final String bucketName;
    private final String schema;

    private FlociGcp flociGcp;
    private HiveHadoop hiveHadoop;
    private Network network;

    public TestIcebergGcsConnectorSmokeTest()
    {
        super(ORC);
        this.bucketName = "test-iceberg-gcs-" + randomNameSuffix();
        this.schema = "test_iceberg_gcs_connector_smoke_test_" + randomNameSuffix();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        network = closeAfterClass(newNetwork());
        flociGcp = closeAfterClass(new FlociGcp().withNetwork(network));
        flociGcp.start();

        GcsFileSystemConfig config = new GcsFileSystemConfig()
                .setEndpoint(Optional.of(flociGcp.getEndpoint().toString()))
                .setProjectId(FLOCI_GCP_PROJECT_ID);
        GcsStorageFactory storageFactory = new GcsStorageFactory(
                config,
                (builder, _) -> builder.setCredentials(NoCredentials.getInstance()));
        storageFactory.create(ConnectorIdentity.ofUser("test")).create(BucketInfo.of(bucketName));

        Path containerCoreSite = createCoreSite(flociGcp.getContainerEndpoint().toString());
        String gcsConnector = Path.of(GoogleHadoopFileSystem.class.getProtectionDomain().getCodeSource().getLocation().toURI())
                .toAbsolutePath()
                .toString();

        this.hiveHadoop = closeAfterClass(HiveHadoop.builder()
                .withImage(HIVE3_IMAGE)
                .withNetwork(network)
                .withFilesToMount(ImmutableMap.of(
                        "/etc/hadoop/conf/core-site.xml", containerCoreSite.normalize().toAbsolutePath().toString(),
                        HADOOP_GCS_CONNECTOR, gcsConnector,
                        TEZ_GCS_CONNECTOR, gcsConnector))
                .build());
        this.hiveHadoop.start();

        return IcebergQueryRunner.builder()
                .setIcebergProperties(ImmutableMap.<String, String>builder()
                        .put("iceberg.catalog.type", "hive_metastore")
                        .put("gcs.endpoint", flociGcp.getEndpoint().toString())
                        .put("gcs.project-id", FLOCI_GCP_PROJECT_ID)
                        .put("hive.metastore.uri", hiveHadoop.getHiveMetastoreEndpoint().toString())
                        .put("iceberg.file-format", format.name())
                        .put("iceberg.register-table-procedure.enabled", "true")
                        .put("iceberg.writer-sort-buffer-size", "1MB")
                        .buildOrThrow())
                .setAdditionalOverrideModule(gcsModule())
                .setSchemaInitializer(
                        SchemaInitializer.builder()
                                .withClonedTpchTables(REQUIRED_TPCH_TABLES)
                                .withSchemaName(schema)
                                .withSchemaProperties(ImmutableMap.of("location", "'" + schemaPath() + "'"))
                                .build())
                .build();
    }

    private static Path createCoreSite(String endpoint)
            throws IOException
    {
        String content = Resources.toString(Resources.getResource("hdp3.1-core-site.xml.gcs-template"), UTF_8)
                .replace("%GCS_ENDPOINT%", endpoint + "/")
                .replace("%GCP_PROJECT_ID%", FLOCI_GCP_PROJECT_ID);
        Path coreSite = Files.createTempFile("core-site", ".xml", READ_ONLY_PERMISSIONS);
        coreSite.toFile().deleteOnExit();
        Files.writeString(coreSite, content);
        return coreSite;
    }

    private static Module gcsModule()
    {
        return binder -> {
            configBinder(binder).bindConfig(GcsFileSystemConfig.class);
            binder.bind(GcsAuth.class)
                    .toInstance((builder, _) -> builder.setCredentials(NoCredentials.getInstance()));
            binder.bind(GcsStorageFactory.class).in(Scopes.SINGLETON);
            binder.bind(GcsFileSystemFactory.class).in(Scopes.SINGLETON);
            newMapBinder(binder, String.class, TrinoFileSystemFactory.class)
                    .addBinding("gs")
                    .to(GcsFileSystemFactory.class);
        };
    }

    @AfterAll
    public void removeTestData()
    {
        try {
            fileSystem.deleteDirectory(Location.of(schemaPath()));
        }
        catch (IOException e) {
            // The GCS bucket should be configured to expire objects automatically. Clean up issues do not need to fail the test.
            LOG.warn(e, "Failed to clean up GCS test directory: %s", schemaPath());
        }
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        // GCS tests use the Hive Metastore catalog which does not support renaming schemas
        return switch (connectorBehavior) {
            case SUPPORTS_RENAME_SCHEMA -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected String createSchemaSql(String schema)
    {
        return format("CREATE SCHEMA %1$s WITH (location = '%2$s%1$s')", schema, schemaPath());
    }

    @Override
    protected String schemaPath()
    {
        return format("gs://%s/%s/", bucketName, schema);
    }

    @Override
    protected boolean locationExists(String location)
    {
        try {
            return fileSystem.newInputFile(Location.of(location)).exists();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        assertQueryFails(
                format("ALTER SCHEMA %s RENAME TO %s", schemaName, schemaName + randomNameSuffix()),
                "Hive metastore does not support renaming schemas");
    }

    @Override
    protected void dropTableFromCatalog(String tableName)
    {
        HiveMetastore metastore = new BridgingHiveMetastore(
                testingThriftHiveMetastoreBuilder()
                        .metastoreClient(hiveHadoop.getHiveMetastoreEndpoint())
                        .build(this::closeAfterClass));
        metastore.dropTable(schema, tableName, false);
        assertThat(metastore.getTable(schema, tableName)).isEmpty();
    }

    @Override
    protected String getMetadataLocation(String tableName)
    {
        HiveMetastore metastore = new BridgingHiveMetastore(
                testingThriftHiveMetastoreBuilder()
                        .metastoreClient(hiveHadoop.getHiveMetastoreEndpoint())
                        .build(this::closeAfterClass));
        return metastore
                .getTable(schema, tableName).orElseThrow()
                .getParameters().get("metadata_location");
    }

    @Override
    protected void deleteDirectory(String location)
    {
        try {
            fileSystem.deleteDirectory(Location.of(location));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected boolean isFileSorted(Location path, String sortColumnName)
    {
        return checkOrcFileSorting(fileSystem, path, sortColumnName);
    }
}
