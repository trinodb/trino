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
package io.trino.plugin.iceberg.catalog.glue;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.glue.AWSGlueAsyncClientBuilder;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastoreConfig;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreStats;
import io.trino.plugin.iceberg.BaseTrinoCatalogTest;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.TestingTypeManager;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static org.testng.Assert.assertEquals;

public class TestTrinoGlueCatalogTest
        extends BaseTrinoCatalogTest
{
    private static final Logger LOG = Logger.get(TestTrinoGlueCatalogTest.class);

    @Override
    protected TrinoCatalog createTrinoCatalog(boolean useUniqueTableLocations)
    {
        TrinoFileSystemFactory fileSystemFactory = new HdfsFileSystemFactory(HDFS_ENVIRONMENT);
        return new TrinoGlueCatalog(
                new CatalogName("catalog_name"),
                fileSystemFactory,
                new TestingTypeManager(),
                new GlueIcebergTableOperationsProvider(
                        fileSystemFactory,
                        new GlueMetastoreStats(),
                        new GlueHiveMetastoreConfig(),
                        DefaultAWSCredentialsProviderChain.getInstance()),
                "test",
                AWSGlueAsyncClientBuilder.defaultClient(),
                new GlueMetastoreStats(),
                Optional.empty(),
                useUniqueTableLocations);
    }

    @Test
    public void testDefaultLocation()
            throws IOException
    {
        Path tmpDirectory = Files.createTempDirectory("test_glue_catalog_default_location_");
        tmpDirectory.toFile().deleteOnExit();

        TrinoFileSystemFactory fileSystemFactory = new HdfsFileSystemFactory(HDFS_ENVIRONMENT);
        TrinoCatalog catalogWithDefaultLocation = new TrinoGlueCatalog(
                new CatalogName("catalog_name"),
                fileSystemFactory,
                new TestingTypeManager(),
                new GlueIcebergTableOperationsProvider(
                        fileSystemFactory,
                        new GlueMetastoreStats(),
                        new GlueHiveMetastoreConfig(),
                        DefaultAWSCredentialsProviderChain.getInstance()),
                "test",
                AWSGlueAsyncClientBuilder.defaultClient(),
                new GlueMetastoreStats(),
                Optional.of(tmpDirectory.toAbsolutePath().toString()),
                false);

        String namespace = "test_default_location_" + randomTableSuffix();
        String table = "tableName";
        SchemaTableName schemaTableName = new SchemaTableName(namespace, table);
        catalogWithDefaultLocation.createNamespace(SESSION, namespace, ImmutableMap.of(), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
        try {
            File expectedSchemaDirectory = new File(tmpDirectory.toFile(), namespace + ".db");
            File expectedTableDirectory = new File(expectedSchemaDirectory, schemaTableName.getTableName());
            assertEquals(catalogWithDefaultLocation.defaultTableLocation(SESSION, schemaTableName), expectedTableDirectory.toPath().toAbsolutePath().toString());
        }
        finally {
            try {
                catalogWithDefaultLocation.dropNamespace(SESSION, namespace);
            }
            catch (Exception e) {
                LOG.warn("Failed to clean up namespace: %s", namespace);
            }
        }
    }
}
