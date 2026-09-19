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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.Table;
import io.trino.plugin.hive.metastore.HiveMetastoreConfig;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.plugin.iceberg.TestingIcebergPlugin;
import io.trino.plugin.iceberg.fileio.ForwardingFileIo;
import io.trino.spi.NodeVersion;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.UUID.randomUUID;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP;
import static org.apache.iceberg.TableProperties.METADATA_PREVIOUS_VERSIONS_MAX;
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
    // When set together with createTableCommitsBeforeFailure, that many later commits move the metadata pointer past the
    // file written by the create before the failure is raised, simulating a concurrent writer that built on the new
    // table right away.
    private final AtomicInteger laterCommitsBeforeFailure = new AtomicInteger();
    // When set, a table that does not descend from the metadata written by the create takes the name before the failure
    // is raised, simulating a concurrent create that won the race for the name.
    private final AtomicBoolean otherTableTakesNameBeforeFailure = new AtomicBoolean();
    private FileIO fileIo;

    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        this.dataDirectory = Files.createTempDirectory("test_iceberg_create_table_failure");
        this.fileIo = new ForwardingFileIo(new LocalFileSystemFactory(dataDirectory).create(ConnectorIdentity.ofUser("test")), false);
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
                    Table current = table;
                    for (int commitNumber = 1; commitNumber <= laterCommitsBeforeFailure.get(); commitNumber++) {
                        current = withLaterCommit(current, commitNumber);
                        replaceTable(table.getDatabaseName(), table.getTableName(), current, principalPrivileges, ImmutableMap.of());
                    }
                }
                else if (otherTableTakesNameBeforeFailure.get()) {
                    super.createTable(otherTable(table), principalPrivileges);
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
        laterCommitsBeforeFailure.set(0);
        otherTableTakesNameBeforeFailure.set(false);
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

    @Test
    public void testCreateTableMetadataPreservedWhenLaterCommitBuiltOnIt()
            throws Exception
    {
        // The metastore applied the create, and a later commit by another writer moved the metadata pointer past our
        // file before the client observed the failure. The current metadata carries the table UUID this operation
        // assigned, so the create is treated as successful and nothing is deleted.
        String tableName = "test_create_built_on_" + randomNameSuffix();
        createTableCommitsBeforeFailure.set(true);
        laterCommitsBeforeFailure.set(1);
        createTableFailure.set(new RuntimeException("simulated metastore response loss"));
        try {
            String tableLocation = "local:///" + tableName;
            String createTableSql = "CREATE TABLE " + tableName + " (a integer) WITH (location = '" + tableLocation + "')";

            getQueryRunner().execute(createTableSql);

            // Both the metadata written by the create and the one written by the later commit are preserved.
            assertThat(metadataFiles(tableName)).hasSize(2);
            // The table is fully usable and reflects the later commit.
            assertThat(getQueryRunner().execute("SELECT * FROM " + tableName).getRowCount()).isEqualTo(0);
            assertThat(computeScalar("SELECT value FROM \"" + tableName + "$properties\" WHERE key = 'later_commits'")).isEqualTo("1");
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testCreateTableMetadataPreservedWhenLaterCommitsEvictedItFromMetadataLog()
            throws Exception
    {
        // Two later commits on a table with write.metadata.previous-versions-max=1: the metadata written by the create is
        // no longer in the current metadata log, but the table UUID still identifies the table as the one this operation
        // created, so nothing is deleted.
        String tableName = "test_create_evicted_" + randomNameSuffix();
        createTableCommitsBeforeFailure.set(true);
        laterCommitsBeforeFailure.set(2);
        createTableFailure.set(new RuntimeException("simulated metastore response loss"));
        try {
            String tableLocation = "local:///" + tableName;
            String createTableSql = "CREATE TABLE " + tableName + " (a integer) WITH (location = '" + tableLocation + "')";

            getQueryRunner().execute(createTableSql);

            assertThat(metadataFiles(tableName)).hasSize(3);
            String currentMetadataLocation = metastore.getTable(SCHEMA_NAME, tableName).orElseThrow().getParameters().get(METADATA_LOCATION_PROP);
            TableMetadata currentMetadata = TableMetadataParser.read(fileIo, currentMetadataLocation);
            assertThat(currentMetadata.previousFiles())
                    .extracting(entry -> entry.file())
                    .hasSize(1)
                    .noneMatch(file -> file.contains("/metadata/00000-"));
            assertThat(getQueryRunner().execute("SELECT * FROM " + tableName).getRowCount()).isEqualTo(0);
            assertThat(computeScalar("SELECT value FROM \"" + tableName + "$properties\" WHERE key = 'later_commits'")).isEqualTo("2");
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testCreateTableFailureMetadataCleanedUpWhenAnotherTableTookTheName()
            throws Exception
    {
        // Another create won the race for the name, and the metastore points at metadata that does not descend from
        // ours: the create failed for good, so the orphaned metadata file is removed and the other table is left alone.
        String tableName = "test_create_lost_name_" + randomNameSuffix();
        otherTableTakesNameBeforeFailure.set(true);
        createTableFailure.set(new RuntimeException("simulated concurrent create"));
        try {
            String tableLocation = "local:///" + tableName;
            String createTableSql = "CREATE TABLE " + tableName + " (a integer) WITH (location = '" + tableLocation + "')";
            assertThatThrownBy(() -> getQueryRunner().execute(createTableSql))
                    .hasMessageContaining("simulated concurrent create");

            assertThat(metadataFiles(tableName)).as("Metadata file should not exist").isEmpty();
            // The table that took the name is intact.
            assertThat(computeActual("SELECT column_name FROM information_schema.columns WHERE table_schema = '" + SCHEMA_NAME + "' AND table_name = '" + tableName + "'").getOnlyColumnAsSet()).containsExactly("b");
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS " + tableName);
        }
    }

    // Writes a metadata file that builds on the one referenced by the table, the way any later Iceberg commit does.
    // write.metadata.previous-versions-max is set to 1 so that the second later commit already evicts the file written
    // by the create from the metadata log.
    private Table withLaterCommit(Table table, int commitNumber)
    {
        String currentLocation = table.getParameters().get(METADATA_LOCATION_PROP);
        TableMetadata current = TableMetadataParser.read(fileIo, currentLocation);
        TableMetadata later = TableMetadata.buildFrom(current)
                .setProperties(ImmutableMap.of(METADATA_PREVIOUS_VERSIONS_MAX, "1", "later_commits", String.valueOf(commitNumber)))
                .build();
        String laterLocation = current.location() + "/metadata/%05d-".formatted(commitNumber) + randomUUID() + ".metadata.json";
        TableMetadataParser.write(later, fileIo.newOutputFile(laterLocation));
        return Table.builder(table)
                .setParameter(METADATA_LOCATION_PROP, laterLocation)
                .setParameter(PREVIOUS_METADATA_LOCATION_PROP, currentLocation)
                .build();
    }

    // A table under the same name whose metadata does not descend from the one written by the create
    private Table otherTable(Table table)
    {
        String otherLocation = "local:///" + table.getTableName() + "_other";
        TableMetadata other = TableMetadata.newTableMetadata(
                new Schema(Types.NestedField.optional(1, "b", Types.IntegerType.get())),
                PartitionSpec.unpartitioned(),
                otherLocation,
                ImmutableMap.of());
        String otherMetadataLocation = otherLocation + "/metadata/00000-" + randomUUID() + ".metadata.json";
        TableMetadataParser.write(other, fileIo.newOutputFile(otherMetadataLocation));
        return Table.builder(table)
                .setParameter(METADATA_LOCATION_PROP, otherMetadataLocation)
                .build();
    }

    private List<Path> metadataFiles(String tableName)
            throws IOException
    {
        try (Stream<Path> files = Files.list(dataDirectory.resolve(tableName, "metadata"))) {
            return files.filter(file -> file.toString().endsWith(".metadata.json")).collect(toImmutableList());
        }
    }
}
