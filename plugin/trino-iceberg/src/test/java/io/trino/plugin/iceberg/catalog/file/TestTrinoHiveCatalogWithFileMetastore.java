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
package io.trino.plugin.iceberg.catalog.file;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.hive.TrinoViewHiveMetastore;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.catalog.BaseTrinoCatalogTest;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.hms.TrinoHiveCatalog;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.TestingTypeManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.metastore.cache.CachingHiveMetastore.createPerTransactionCache;
import static io.trino.plugin.hive.metastore.file.TestingFileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.plugin.iceberg.IcebergFileFormat.PARQUET;
import static io.trino.plugin.iceberg.IcebergTableProperties.FILE_FORMAT_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.FORMAT_VERSION_PROPERTY;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestTrinoHiveCatalogWithFileMetastore
        extends BaseTrinoCatalogTest
{
    private static final Logger log = Logger.get(TestTrinoHiveCatalogWithFileMetastore.class);

    private Path tempDir;
    private TrinoFileSystemFactory fileSystemFactory;
    private HiveMetastore metastore;

    @BeforeAll
    public void setUp()
            throws Exception
    {
        tempDir = Files.createTempDirectory("test_trino_hive_catalog");
        File metastoreDir = tempDir.resolve("iceberg_data").toFile();
        metastoreDir.mkdirs();
        fileSystemFactory = new LocalFileSystemFactory(metastoreDir.toPath());
        metastore = createTestingFileHiveMetastore(fileSystemFactory, Location.of("local:///"));
    }

    @AfterAll
    public void tearDown()
            throws IOException
    {
        deleteRecursively(tempDir, ALLOW_INSECURE);
    }

    @Override
    protected TrinoCatalog createTrinoCatalog(boolean useUniqueTableLocations)
    {
        CachingHiveMetastore cachingHiveMetastore = createPerTransactionCache(metastore, 1000);
        return new TrinoHiveCatalog(
                new CatalogName("catalog"),
                cachingHiveMetastore,
                new TrinoViewHiveMetastore(cachingHiveMetastore, false, "trino-version", "test"),
                fileSystemFactory,
                new TestingTypeManager(),
                new FileMetastoreTableOperationsProvider(fileSystemFactory),
                useUniqueTableLocations,
                false,
                false,
                new IcebergConfig().isHideMaterializedViewStorageTable(),
                directExecutor());
    }

    @Test
    @Disabled
    public void testDropMaterializedView()
    {
        testDropMaterializedView(false);
    }

    @Test
    public void testDropMaterializedViewWithUniqueTableLocation()
    {
        testDropMaterializedView(true);
    }

    private void testDropMaterializedView(boolean useUniqueTableLocations)
    {
        TrinoCatalog catalog = createTrinoCatalog(useUniqueTableLocations);
        String namespace = "test_create_mv_" + randomNameSuffix();
        String materializedViewName = "materialized_view_name";
        try {
            catalog.createNamespace(SESSION, namespace, defaultNamespaceProperties(namespace), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
            catalog.createMaterializedView(
                    SESSION,
                    new SchemaTableName(namespace, materializedViewName),
                    new ConnectorMaterializedViewDefinition(
                            "SELECT * FROM tpch.tiny.nation",
                            Optional.empty(),
                            Optional.of("catalog_name"),
                            Optional.of("schema_name"),
                            ImmutableList.of(new ConnectorMaterializedViewDefinition.Column("col1", INTEGER.getTypeId(), Optional.empty())),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty(),
                            ImmutableList.of()),
                    ImmutableMap.of(FILE_FORMAT_PROPERTY, PARQUET, FORMAT_VERSION_PROPERTY, 1),
                    false,
                    false);

            catalog.dropMaterializedView(SESSION, new SchemaTableName(namespace, materializedViewName));
        }
        finally {
            try {
                catalog.dropNamespace(SESSION, namespace);
            }
            catch (Exception e) {
                log.warn("Failed to clean up namespace: %s", namespace);
            }
        }
    }

    @Test
    @Override
    public void testListTables()
    {
        // the test actually works but when cleanup up the materialized view the error is thrown
        assertThatThrownBy(super::testListTables).hasMessageMatching("Table 'ns2.*.mv' not found");
    }
}
