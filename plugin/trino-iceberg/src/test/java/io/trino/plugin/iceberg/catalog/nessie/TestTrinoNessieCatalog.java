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
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.HdfsConfig;
import io.trino.plugin.hive.HdfsConfigurationInitializer;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveHdfsConfiguration;
import io.trino.plugin.hive.authentication.NoHdfsAuthentication;
import io.trino.plugin.iceberg.BaseTrinoCatalogTest;
import io.trino.plugin.iceberg.HdfsFileIoProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.TestingTypeManager;
import io.trino.testing.containers.NessieContainer;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.http.HttpClientBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

public class TestTrinoNessieCatalog
        extends BaseTrinoCatalogTest
{
    private NessieContainer nessieContainer;

    @BeforeClass
    public void setupServer()
    {
        nessieContainer = NessieContainer.builder().build();
        nessieContainer.start();
    }

    @AfterClass
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
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(new HiveHdfsConfiguration(
                new HdfsConfigurationInitializer(
                        new HdfsConfig(),
                        ImmutableSet.of()),
                ImmutableSet.of()),
                new HdfsConfig(),
                new NoHdfsAuthentication());
        NessieConfig nessieConfig = new NessieConfig()
                .setServerUri(nessieContainer.getRestApiUri());
        NessieApiV1 nessieApi = HttpClientBuilder.builder()
                .withUri(nessieContainer.getRestApiUri())
                .build(NessieApiV1.class);
        NessieIcebergClient nessieClient = new NessieIcebergClient(nessieApi, nessieConfig.getDefaultReferenceName(), null);
        return new TrinoNessieCatalog(
                new CatalogName("catalog_name"),
                new TestingTypeManager(),
                new NessieIcebergTableOperationsProvider(new HdfsFileIoProvider(hdfsEnvironment), nessieClient),
                nessieClient,
                tmpDirectory.toAbsolutePath().toString(),
                "testVersion",
                useUniqueTableLocations);
    }

    @Test
    public void testDefaultLocation()
            throws IOException
    {
        Path tmpDirectory = createTempDirectory("test_nessie_catalog_default_location_");
        tmpDirectory.toFile().deleteOnExit();
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(new HiveHdfsConfiguration(
                new HdfsConfigurationInitializer(
                        new HdfsConfig(),
                        ImmutableSet.of()),
                ImmutableSet.of()),
                new HdfsConfig(),
                new NoHdfsAuthentication());
        NessieConfig nessieConfig = new NessieConfig()
                .setDefaultWarehouseDir(tmpDirectory.toAbsolutePath().toString())
                .setServerUri(nessieContainer.getRestApiUri());
        NessieApiV1 nessieApi = HttpClientBuilder.builder()
                .withUri(nessieContainer.getRestApiUri())
                .build(NessieApiV1.class);
        NessieIcebergClient nessieClient = new NessieIcebergClient(nessieApi, nessieConfig.getDefaultReferenceName(), null);
        TrinoCatalog catalogWithDefaultLocation = new TrinoNessieCatalog(
                new CatalogName("catalog_name"),
                new TestingTypeManager(),
                new NessieIcebergTableOperationsProvider(new HdfsFileIoProvider(hdfsEnvironment), nessieClient),
                nessieClient,
                nessieConfig.getDefaultWarehouseDir(),
                "testVersion",
                false);

        String namespace = "test_default_location_" + randomTableSuffix();
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
}
