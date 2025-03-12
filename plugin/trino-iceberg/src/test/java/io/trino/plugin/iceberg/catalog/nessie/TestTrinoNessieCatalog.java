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
package io.trino.plugin.iceberg.catalog.nessie;

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.iceberg.CommitTaskData;
import io.trino.plugin.iceberg.IcebergMetadata;
import io.trino.plugin.iceberg.TableStatisticsWriter;
import io.trino.plugin.iceberg.catalog.BaseTrinoCatalogTest;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.containers.NessieContainer;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.TestingTypeManager;
import org.apache.iceberg.nessie.NessieIcebergClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApiV2;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestTrinoNessieCatalog
        extends BaseTrinoCatalogTest
{
    private NessieContainer nessieContainer;

    @BeforeAll
    public void setupServer()
    {
        nessieContainer = NessieContainer.builder().build();
        nessieContainer.start();
    }

    @AfterAll
    public void teardownServer()
    {
        if (nessieContainer != null) {
            nessieContainer.close();
        }
    }

    @Override
    protected TrinoCatalog createTrinoCatalog(boolean useUniqueTableLocations)
    {
        Path tmpDirectory = null;
        try {
            tmpDirectory = createTempDirectory("test_nessie_catalog_warehouse_dir_");
        }
        catch (IOException e) {
            fail(e.getMessage());
        }
        TrinoFileSystemFactory fileSystemFactory = new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS);
        IcebergNessieCatalogConfig icebergNessieCatalogConfig = new IcebergNessieCatalogConfig()
                .setServerUri(URI.create(nessieContainer.getRestApiUri()));
        NessieApiV2 nessieApi = NessieClientBuilder.createClientBuilderFromSystemSettings()
                .withUri(nessieContainer.getRestApiUri())
                .build(NessieApiV2.class);
        NessieIcebergClient nessieClient = new NessieIcebergClient(nessieApi, icebergNessieCatalogConfig.getDefaultReferenceName(), null, ImmutableMap.of());
        return new TrinoNessieCatalog(
                new CatalogName("catalog_name"),
                new TestingTypeManager(),
                fileSystemFactory,
                new IcebergNessieTableOperationsProvider(fileSystemFactory, nessieClient),
                nessieClient,
                tmpDirectory.toAbsolutePath().toString(),
                useUniqueTableLocations);
    }

    @Test
    public void testDefaultLocation()
            throws IOException
    {
        Path tmpDirectory = createTempDirectory("test_nessie_catalog_default_location_");
        tmpDirectory.toFile().deleteOnExit();
        TrinoFileSystemFactory fileSystemFactory = new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS);
        IcebergNessieCatalogConfig icebergNessieCatalogConfig = new IcebergNessieCatalogConfig()
                .setDefaultWarehouseDir(tmpDirectory.toAbsolutePath().toString())
                .setServerUri(URI.create(nessieContainer.getRestApiUri()));
        NessieApiV2 nessieApi = NessieClientBuilder.createClientBuilderFromSystemSettings()
                .withUri(nessieContainer.getRestApiUri())
                .build(NessieApiV2.class);
        NessieIcebergClient nessieClient = new NessieIcebergClient(nessieApi, icebergNessieCatalogConfig.getDefaultReferenceName(), null, ImmutableMap.of());
        TrinoCatalog catalogWithDefaultLocation = new TrinoNessieCatalog(
                new CatalogName("catalog_name"),
                new TestingTypeManager(),
                fileSystemFactory,
                new IcebergNessieTableOperationsProvider(fileSystemFactory, nessieClient),
                nessieClient,
                icebergNessieCatalogConfig.getDefaultWarehouseDir(),
                false);

        String namespace = "test_default_location_" + randomNameSuffix();
        String table = "tableName";
        SchemaTableName schemaTableName = new SchemaTableName(namespace, table);
        catalogWithDefaultLocation.createNamespace(SESSION, namespace, ImmutableMap.of(),
                new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
        try {
            File expectedSchemaDirectory = new File(tmpDirectory.toFile(), namespace);
            File expectedTableDirectory = new File(expectedSchemaDirectory, schemaTableName.getTableName());
            assertThat(catalogWithDefaultLocation.defaultTableLocation(SESSION, schemaTableName))
                    .isEqualTo(expectedTableDirectory.toPath().toAbsolutePath().toString());
        }
        finally {
            catalogWithDefaultLocation.dropNamespace(SESSION, namespace);
        }
    }

    @Test
    @Override
    public void testView()
    {
        assertThatThrownBy(super::testView)
                .hasMessageContaining("createView is not supported for Iceberg Nessie catalogs");
    }

    @Test
    @Override
    public void testNonLowercaseNamespace()
    {
        TrinoCatalog catalog = createTrinoCatalog(false);

        String namespace = "testNonLowercaseNamespace" + randomNameSuffix();
        String schema = namespace.toLowerCase(ENGLISH);

        // Currently this is actually stored in lowercase by all Catalogs
        catalog.createNamespace(SESSION, namespace, Map.of(), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
        try {
            assertThat(catalog.namespaceExists(SESSION, namespace)).as("catalog.namespaceExists(namespace)")
                    .isTrue();
            assertThat(catalog.namespaceExists(SESSION, schema)).as("catalog.namespaceExists(schema)")
                    .isFalse();
            assertThat(catalog.listNamespaces(SESSION)).as("catalog.listNamespaces")
                    // Catalog listNamespaces may be used as a default implementation for ConnectorMetadata.schemaExists
                    .doesNotContain(schema)
                    .contains(namespace);

            // Test with IcebergMetadata, should the ConnectorMetadata implementation behavior depend on that class
            ConnectorMetadata icebergMetadata = new IcebergMetadata(
                    PLANNER_CONTEXT.getTypeManager(),
                    CatalogHandle.fromId("iceberg:NORMAL:v12345"),
                    jsonCodec(CommitTaskData.class),
                    catalog,
                    (connectorIdentity, fileIoProperties) -> {
                        throw new UnsupportedOperationException();
                    },
                    new TableStatisticsWriter(new NodeVersion("test-version")),
                    Optional.empty(),
                    false,
                    _ -> false,
                    newDirectExecutorService(),
                    directExecutor());
            assertThat(icebergMetadata.schemaExists(SESSION, namespace)).as("icebergMetadata.schemaExists(namespace)")
                    .isTrue();
            assertThat(icebergMetadata.schemaExists(SESSION, schema)).as("icebergMetadata.schemaExists(schema)")
                    .isFalse();
            assertThat(icebergMetadata.listSchemaNames(SESSION)).as("icebergMetadata.listSchemaNames")
                    .doesNotContain(schema)
                    .contains(namespace);
        }
        finally {
            catalog.dropNamespace(SESSION, namespace);
        }
    }
}
