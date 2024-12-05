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
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.Table;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.metastore.HiveMetastoreConfig;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.plugin.iceberg.TestingIcebergPlugin;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
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
                new LocalFileSystemFactory(Path.of(dataDirectory.toString())),
                new HiveMetastoreConfig().isHideDeltaLakeTables(),
                new FileHiveMetastoreConfig()
                        .setCatalogDirectory("local://"))
        {
            @Override
            public synchronized void createTable(Table table, PrincipalPrivileges principalPrivileges)
            {
                if (testException.get() != null) {
                    throw testException.get();
                }
            }
        };

        Session session = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema(SCHEMA_NAME)
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();
        queryRunner.installPlugin(new TestingIcebergPlugin(Path.of(dataDirectory.toString()), Optional.of(new TestingIcebergFileMetastoreCatalogModule(metastore))));
        queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg");
        queryRunner.execute("CREATE SCHEMA " + SCHEMA_NAME);

        return queryRunner;
    }

    @AfterAll
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
        testException.set(new SchemaNotFoundException("simulated_test_schema"));
        String tableName = "test_create_failure_" + randomNameSuffix();
        String tableLocation = "local:///" + tableName;
        String createTableSql = "CREATE TABLE " + tableName + " (a varchar) WITH (location = '" + tableLocation + "')";
        assertThatThrownBy(() -> getQueryRunner().execute(createTableSql))
                .hasMessageContaining("Schema simulated_test_schema not found");

        Path metadataDirectory = dataDirectory.resolve(tableName, "metadata");
        assertThat(metadataDirectory).as("Metadata file should not exist").isEmptyDirectory();

        // it should be possible to create a table with the same name after the failure
        testException.set(null);
        getQueryRunner().execute(createTableSql);
        assertThat(metadataDirectory).as("Metadata file should not exist").isNotEmptyDirectory();
    }
}
