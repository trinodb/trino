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
import io.trino.plugin.hive.metastore.HiveMetastoreConfig;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.plugin.iceberg.TestingIcebergPlugin;
import io.trino.spi.NodeVersion;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private final AtomicReference<RuntimeException> createTableFailure = new AtomicReference<>();
    // When set, the metastore persists the table before raising createTableFailure, simulating a commit that
    // actually landed but whose response was lost (e.g. a timeout, or a retried request observing AlreadyExists).
    private final AtomicBoolean createTableCommitsBeforeFailure = new AtomicBoolean();
    // When set, the metastore becomes unreachable once a createTable has been attempted, simulating a metastore that
    // is unavailable during the post-failure commit-status check (but reachable for the initial existence check).
    private final AtomicBoolean metastoreUnavailableAfterCreate = new AtomicBoolean();
    private final AtomicBoolean metastoreUnavailable = new AtomicBoolean();

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
                RuntimeException failure = createTableFailure.get();
                // Persist the table on a normal create, and also when simulating a commit that landed before the
                // injected failure (createTableCommitsBeforeFailure), so the metastore actually holds the table.
                if (failure == null || createTableCommitsBeforeFailure.get()) {
                    super.createTable(table, principalPrivileges);
                }
                if (metastoreUnavailableAfterCreate.get()) {
                    metastoreUnavailable.set(true);
                }
                if (failure != null) {
                    throw failure;
                }
            }

            @Override
            public synchronized Optional<Table> getTable(String databaseName, String tableName)
            {
                if (metastoreUnavailable.get()) {
                    throw new RuntimeException("simulated metastore unavailable");
                }
                return super.getTable(databaseName, tableName);
            }
        };

        Session session = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema(SCHEMA_NAME)
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();
        queryRunner.installPlugin(new TestingIcebergPlugin(Path.of(dataDirectory.toString()), () -> Optional.of(new TestingIcebergFileMetastoreCatalogModule(metastore))));
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

    @BeforeEach
    public void resetMetastoreBehavior()
    {
        createTableFailure.set(null);
        createTableCommitsBeforeFailure.set(false);
        metastoreUnavailableAfterCreate.set(false);
        metastoreUnavailable.set(false);
    }

    @Test
    public void testCreateTableFailureMetadataCleanedUp()
    {
        // The metastore is reachable and confirms the table was not created, so the orphaned metadata file is removed.
        String tableName = "test_create_failure_" + randomNameSuffix();
        createTableFailure.set(new SchemaNotFoundException("simulated_test_schema"));
        try {
            String tableLocation = "local:///" + tableName;
            String createTableSql = "CREATE TABLE " + tableName + " (a varchar) WITH (location = '" + tableLocation + "')";
            assertThatThrownBy(() -> getQueryRunner().execute(createTableSql))
                    .hasMessageContaining("Schema simulated_test_schema not found");

            Path metadataDirectory = dataDirectory.resolve(tableName, "metadata");
            assertThat(metadataDirectory).as("Metadata file should not exist").isEmptyDirectory();

            // it should be possible to create a table with the same name after the failure
            createTableFailure.set(null);
            getQueryRunner().execute(createTableSql);
            assertThat(metadataDirectory).as("Metadata file should not exist").isNotEmptyDirectory();
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testCreateTableMetadataPreservedWhenCommitActuallySucceeded()
    {
        // The metastore applied the create but the client observed a failure (lost response / retried AlreadyExists).
        // The commit-status check finds the table pointing at our metadata, so the create is treated as successful.
        String tableName = "test_create_succeeded_" + randomNameSuffix();
        createTableCommitsBeforeFailure.set(true);
        createTableFailure.set(new RuntimeException("simulated metastore response loss"));
        try {
            String tableLocation = "local:///" + tableName;
            String createTableSql = "CREATE TABLE " + tableName + " (a integer) WITH (location = '" + tableLocation + "')";

            // The statement must not fail and must not delete the metadata of the table that was actually created.
            getQueryRunner().execute(createTableSql);

            Path metadataDirectory = dataDirectory.resolve(tableName, "metadata");
            assertThat(metadataDirectory).as("Metadata file should be preserved").isNotEmptyDirectory();
            // The table is fully usable.
            assertThat(getQueryRunner().execute("SELECT * FROM " + tableName).getRowCount()).isEqualTo(0);
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testCreateTableMetadataPreservedWhenCommitStateUnknown()
    {
        // The metastore applied the create but the response was lost, and the follow-up commit-status check cannot
        // reach the metastore either. The outcome is unknown, so all new files must be preserved.
        String tableName = "test_create_unknown_" + randomNameSuffix();
        createTableCommitsBeforeFailure.set(true);
        createTableFailure.set(new RuntimeException("simulated metastore response loss"));
        metastoreUnavailableAfterCreate.set(true);
        try {
            String tableLocation = "local:///" + tableName;
            String createTableSql = "CREATE TABLE " + tableName + " (a integer) WITH (location = '" + tableLocation + "')";

            assertThatThrownBy(() -> getQueryRunner().execute(createTableSql))
                    .hasMessageContaining("Cannot determine whether the commit was successful");

            Path metadataDirectory = dataDirectory.resolve(tableName, "metadata");
            assertThat(metadataDirectory).as("Metadata file should be preserved when commit state is unknown").isNotEmptyDirectory();

            // Once the metastore is reachable again the table created by the (actually successful) commit is usable.
            metastoreUnavailable.set(false);
            assertThat(getQueryRunner().execute("SELECT * FROM " + tableName).getRowCount()).isEqualTo(0);
        }
        finally {
            // Restore metastore reachability (an earlier assertion may have failed before it was reset) so the table can be dropped.
            metastoreUnavailable.set(false);
            getQueryRunner().execute("DROP TABLE IF EXISTS " + tableName);
        }
    }
}
