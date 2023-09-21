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

import io.trino.Session;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreConfig;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.plugin.iceberg.TestingIcebergPlugin;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Test(singleThreaded = true) // testException is a shared mutable state
public class TestIcebergFileMetastoreCreateTableFailure
        extends AbstractTestQueryFramework
{
    private static final String ICEBERG_CATALOG = "iceberg";
    private static final String SCHEMA_NAME = "test_schema";

    private Path dataDirectory;
    private HiveMetastore metastore;
    private final AtomicReference<RuntimeException> testException = new AtomicReference<>();

    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        this.dataDirectory = Files.createTempDirectory("test_iceberg_create_table_failure");
        // Using FileHiveMetastore as approximation of HMS
        this.metastore = new FileHiveMetastore(
                new NodeVersion("testversion"),
                HDFS_FILE_SYSTEM_FACTORY,
                new HiveMetastoreConfig().isHideDeltaLakeTables(),
                new FileHiveMetastoreConfig()
                        .setCatalogDirectory(dataDirectory.toString()))
        {
            @Override
            public synchronized void createTable(Table table, PrincipalPrivileges principalPrivileges)
            {
                throw testException.get();
            }
        };

        Session session = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema(SCHEMA_NAME)
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();
        queryRunner.installPlugin(new TestingIcebergPlugin(Optional.of(new TestingIcebergFileMetastoreCatalogModule(metastore)), Optional.empty(), EMPTY_MODULE));
        queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg");
        queryRunner.execute("CREATE SCHEMA " + SCHEMA_NAME);

        return queryRunner;
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
            throws Exception
    {
        if (metastore != null) {
            metastore.dropDatabase(SCHEMA_NAME, true);
        }
        if (dataDirectory != null) {
            deleteRecursively(dataDirectory, ALLOW_INSECURE);
        }
    }

    @Test
    public void testCreateTableFailureMetadataCleanedUp()
    {
        String exceptionMessage = "Test-simulated metastore schema not found exception";
        testException.set(new SchemaNotFoundException("simulated_test_schema", exceptionMessage));
        testCreateTableFailure(exceptionMessage, false);
    }

    @Test
    public void testCreateTableFailureMetadataNotCleanedUp()
    {
        String exceptionMessage = "Test-simulated metastore runtime exception";
        testException.set(new RuntimeException(exceptionMessage));
        testCreateTableFailure(exceptionMessage, true);
    }

    protected void testCreateTableFailure(String expectedExceptionMessage, boolean shouldMetadataFileExist)
    {
        String tableName = "test_create_failure_" + randomNameSuffix();
        String tableLocation = Path.of(dataDirectory.toString(), tableName).toString();
        assertThatThrownBy(() -> getQueryRunner().execute("CREATE TABLE " + tableName + " (a varchar) WITH (location = '" + tableLocation + "')"))
                .hasMessageContaining(expectedExceptionMessage);

        Path metadataDirectory = Path.of(tableLocation, "metadata");
        if (shouldMetadataFileExist) {
            assertThat(metadataDirectory).as("Metadata file should exist").isDirectoryContaining("glob:**.metadata.json");
        }
        else {
            assertThat(metadataDirectory).as("Metadata file should not exist").isEmptyDirectory();
        }
    }
}
