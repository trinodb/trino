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

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.HdfsConfig;
import io.trino.plugin.hive.HdfsConfiguration;
import io.trino.plugin.hive.HdfsConfigurationInitializer;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveHdfsConfiguration;
import io.trino.plugin.hive.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.file.FileMetastoreTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.hms.TrinoHiveCatalog;
import io.trino.spi.type.TestingTypeManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.metastore.cache.CachingHiveMetastore.memoizeMetastore;
import static io.trino.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTrinoHiveCatalogTest
        extends BaseTrinoCatalogTest
{
    private final HiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final java.nio.file.Path tempDir;
    private final File metastoreDir;

    public TestTrinoHiveCatalogTest()
            throws IOException
    {
        HdfsConfig config = new HdfsConfig();
        HdfsConfiguration configuration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(config), ImmutableSet.of());
        hdfsEnvironment = new HdfsEnvironment(configuration, config, new NoHdfsAuthentication());

        tempDir = Files.createTempDirectory("test_trino_hive_catalog");
        metastoreDir = tempDir.resolve("iceberg_data").toFile();
        metastore = createTestingFileHiveMetastore(metastoreDir);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        deleteRecursively(tempDir, ALLOW_INSECURE);
    }

    @Override
    protected TrinoCatalog createTrinoCatalog(boolean useUniqueTableLocations)
    {
        return new TrinoHiveCatalog(
                new CatalogName("catalog"),
                memoizeMetastore(metastore, 1000),
                hdfsEnvironment,
                new TestingTypeManager(),
                new FileMetastoreTableOperationsProvider(new HdfsFileIoProvider(hdfsEnvironment)),
                "trino-version",
                useUniqueTableLocations,
                false,
                false);
    }

    @Override
    @Test
    public void testCreateNamespaceWithLocation()
    {
        assertThatThrownBy(super::testCreateNamespaceWithLocation)
                .hasMessageContaining("Database cannot be created with a location set");
    }

    @Override
    @Test
    public void testUseUniqueTableLocations()
    {
        assertThatThrownBy(super::testCreateNamespaceWithLocation)
                .hasMessageContaining("Database cannot be created with a location set");
    }
}
