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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.multibindings.ProvidesIntoSet;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.metastore.Database;
import io.trino.plugin.hive.FlociS3AndGlue;
import io.trino.plugin.hive.metastore.glue.ForGlueHiveMetastore;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastore;
import io.trino.plugin.iceberg.TestingIcebergPlugin;
import io.trino.plugin.iceberg.catalog.IcebergCatalogModule;
import io.trino.plugin.iceberg.fileio.ForwardingFileIo;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.PrincipalType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
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
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.hive.metastore.glue.GlueConverter.getTableType;
import static io.trino.plugin.iceberg.IcebergTestUtils.getConnectorService;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Locale.ENGLISH;
import static java.util.UUID.randomUUID;
import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestIcebergGlueTableOperationsCreateTableFailure
        extends AbstractTestQueryFramework
{
    private static final Logger LOG = Logger.get(TestIcebergGlueTableOperationsCreateTableFailure.class);

    private static final String ICEBERG_CATALOG = "iceberg";

    private final String schemaName = "test_iceberg_glue_" + randomNameSuffix();
    // When set, the create request is applied and then fails with this exception, simulating a commit that actually
    // landed but whose response was lost (e.g. a retried request observing AlreadyExists).
    private final AtomicReference<GlueException> createTableFailure = new AtomicReference<>();
    // When set together with createTableFailure, a later commit moves the metadata pointer past the file written by
    // the create before the failure is raised, simulating a concurrent writer that built on the new table right away.
    private final AtomicBoolean laterCommitBeforeFailure = new AtomicBoolean();
    // When set, a table that does not descend from the metadata written by the create takes the name before the create
    // request is sent, simulating a concurrent create that won the race for the name.
    private final AtomicBoolean otherTableTakesName = new AtomicBoolean();
    // When set, a table whose metadata file does not exist takes the name before the create request is sent, so the
    // commit-status check cannot read the current metadata to tell whether it descends from ours.
    private final AtomicBoolean otherTableWithMissingMetadataTakesName = new AtomicBoolean();
    // When set, the metadata directory (which must be on the local file system) is made read-only before the create
    // request is sent, so the orphaned metadata file cannot be deleted during cleanup.
    private final AtomicBoolean metadataDirectoryReadOnlyBeforeFailure = new AtomicBoolean();

    private Path dataDirectory;
    private String bucketName;
    private GlueHiveMetastore glueHiveMetastore;
    // A plain client, without the test interceptor, for the writer that races with the create
    private GlueClient glueClient;
    private TrinoFileSystem fileSystem;
    private FileIO fileIo;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema(schemaName)
                .build();
        QueryRunner queryRunner = new StandaloneQueryRunner(session);

        dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data");
        FlociS3AndGlue floci = closeAfterClass(new FlociS3AndGlue());
        bucketName = "test-iceberg-glue-create-failure-" + randomNameSuffix();
        floci.createBucket(bucketName);
        glueClient = closeAfterClass(floci.createGlueClient());

        queryRunner.installPlugin(new TestingIcebergPlugin(dataDirectory, () -> Optional.of(new TestingGlueCatalogModule())));
        queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", ImmutableMap.<String, String>builder()
                .put("iceberg.catalog.type", "glue")
                .put("hive.metastore.glue.default-warehouse-dir", "s3://%s/".formatted(bucketName))
                .put("fs.s3.enabled", "true")
                .putAll(floci.s3AndGlueProperties())
                .buildOrThrow());

        glueHiveMetastore = getConnectorService(queryRunner, GlueHiveMetastore.class);
        // The connector's file system serves both the s3:// table locations and the local:// one used to make cleanup fail
        fileSystem = getFileSystemFactory(queryRunner).create(ConnectorIdentity.ofUser("test"));
        fileIo = new ForwardingFileIo(fileSystem, false);

        Database database = Database.builder()
                .setDatabaseName(schemaName)
                .setOwnerName(Optional.of("public"))
                .setOwnerType(Optional.of(PrincipalType.ROLE))
                .setLocation(Optional.of("s3://%s/%s".formatted(bucketName, schemaName)))
                .build();
        glueHiveMetastore.createDatabase(database);

        return queryRunner;
    }

    @AfterAll
    public void cleanup()
    {
        try {
            if (glueHiveMetastore != null) {
                glueHiveMetastore.dropDatabase(schemaName, false);
            }
        }
        catch (Exception e) {
            LOG.error(e, "Failed to clean up Glue database: %s", schemaName);
        }
    }

    @BeforeEach
    public void resetGlueBehavior()
    {
        createTableFailure.set(null);
        laterCommitBeforeFailure.set(false);
        otherTableTakesName.set(false);
        otherTableWithMissingMetadataTakesName.set(false);
        metadataDirectoryReadOnlyBeforeFailure.set(false);
    }

    @Test
    public void testCreateTableMetadataPreservedWhenLaterCommitBuiltOnIt()
            throws Exception
    {
        // Glue applied the create, and a later commit by another writer moved the metadata pointer past our file before
        // the client observed the failure. The current metadata carries the table UUID this operation assigned, so the
        // create is treated as successful and nothing is deleted.
        String tableName = "test_create_built_on_" + randomNameSuffix();
        laterCommitBeforeFailure.set(true);
        createTableFailure.set(AlreadyExistsException.builder().message("simulated AlreadyExists from a retried create").build());
        try {
            String tableLocation = "s3://" + bucketName + "/" + schemaName + "/" + tableName;
            String createTableSql = "CREATE TABLE " + tableName + " (a integer) WITH (location = '" + tableLocation + "')";

            getQueryRunner().execute(createTableSql);

            // Both the metadata written by the create and the one written by the later commit are preserved.
            assertThat(metadataFiles(tableLocation)).hasSize(2);
            // The table is fully usable and reflects the later commit.
            assertThat(getQueryRunner().execute("SELECT * FROM " + tableName).getRowCount()).isEqualTo(0);
            assertThat(computeScalar("SELECT value FROM \"" + tableName + "$properties\" WHERE key = 'later_commits'")).isEqualTo("1");
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testCreateTableFailureMetadataCleanedUpWhenAnotherTableTookTheName()
            throws Exception
    {
        // Another create won the race for the name, and Glue points at metadata that does not descend from ours: the
        // create failed for good, so the orphaned metadata file is removed and the other table is left alone.
        String tableName = "test_create_lost_name_" + randomNameSuffix();
        otherTableTakesName.set(true);
        try {
            String tableLocation = "s3://" + bucketName + "/" + schemaName + "/" + tableName;
            String createTableSql = "CREATE TABLE " + tableName + " (a integer) WITH (location = '" + tableLocation + "')";
            assertThatThrownBy(() -> getQueryRunner().execute(createTableSql))
                    .hasMessageContaining("Failed to create table");

            assertThat(metadataFiles(tableLocation)).as("Metadata file should not exist").isEmpty();
            // The table that took the name is intact.
            assertThat(computeActual("SELECT column_name FROM information_schema.columns WHERE table_schema = '" + schemaName + "' AND table_name = '" + tableName + "'").getOnlyColumnAsSet())
                    .containsExactly("b");
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testCreateTableMetadataPreservedWhenCurrentMetadataIsUnreadable()
            throws Exception
    {
        // Another table took the name and its metadata cannot be read, so the outcome is unknown and nothing is deleted.
        String tableName = "test_create_unreadable_" + randomNameSuffix();
        otherTableWithMissingMetadataTakesName.set(true);
        try {
            String tableLocation = "s3://" + bucketName + "/" + schemaName + "/" + tableName;
            String createTableSql = "CREATE TABLE " + tableName + " (a integer) WITH (location = '" + tableLocation + "')";
            assertThatThrownBy(() -> getQueryRunner().execute(createTableSql))
                    .hasMessageContaining("Cannot determine whether the commit was successful");

            assertThat(metadataFiles(tableLocation)).as("Metadata file should be preserved when commit state is unknown").hasSize(1);
        }
        finally {
            glueClient.deleteTable(request -> request.databaseName(schemaName).name(tableName));
        }
    }

    @Test
    public void testCreateTableFailureReportedWhenCleanupFails()
            throws Exception
    {
        // The orphaned metadata file cannot be deleted, and the create failure is still the reported cause.
        String tableName = "test_create_cleanup_failure_" + randomNameSuffix();
        otherTableTakesName.set(true);
        metadataDirectoryReadOnlyBeforeFailure.set(true);
        try {
            String tableLocation = "local:///" + tableName;
            String createTableSql = "CREATE TABLE " + tableName + " (a integer) WITH (location = '" + tableLocation + "')";
            assertThatThrownBy(() -> getQueryRunner().execute(createTableSql))
                    .hasMessageContaining("Table already exists");

            assertThat(metadataFiles(tableLocation)).as("Metadata file could not be deleted").hasSize(1);
        }
        finally {
            Files.setPosixFilePermissions(dataDirectory.resolve(tableName, "metadata"), PosixFilePermissions.fromString("rwxr-xr-x"));
            getQueryRunner().execute("DROP TABLE IF EXISTS " + tableName);
        }
    }

    private List<String> metadataFiles(String tableLocation)
            throws IOException
    {
        ImmutableList.Builder<String> files = ImmutableList.builder();
        FileIterator iterator = fileSystem.listFiles(Location.of(tableLocation + "/metadata"));
        while (iterator.hasNext()) {
            String location = iterator.next().location().toString();
            if (location.endsWith(".metadata.json")) {
                files.add(location);
            }
        }
        return files.build();
    }

    // Writes a metadata file that builds on the one referenced by the table, the way any later Iceberg commit does.
    private TableInput withLaterCommit(Table table)
    {
        String currentLocation = table.parameters().get(METADATA_LOCATION_PROP);
        TableMetadata current = TableMetadataParser.read(fileIo, currentLocation);
        TableMetadata later = TableMetadata.buildFrom(current)
                .setProperties(ImmutableMap.of("later_commits", "1"))
                .build();
        String laterLocation = current.location() + "/metadata/00001-" + randomUUID() + ".metadata.json";
        TableMetadataParser.write(later, fileIo.newOutputFile(laterLocation));
        Map<String, String> parameters = new HashMap<>(table.parameters());
        parameters.put(METADATA_LOCATION_PROP, laterLocation);
        parameters.put(PREVIOUS_METADATA_LOCATION_PROP, currentLocation);
        return TableInput.builder()
                .name(table.name())
                .tableType(getTableType(table))
                .owner(table.owner())
                .storageDescriptor(table.storageDescriptor())
                .parameters(parameters)
                .build();
    }

    // A table under the same name whose metadata does not descend from the one written by the create
    private TableInput otherTable(String tableName, boolean writeMetadata)
    {
        String otherLocation = "s3://" + bucketName + "/" + schemaName + "/" + tableName + "_other";
        String otherMetadataLocation = otherLocation + "/metadata/00000-" + randomUUID() + ".metadata.json";
        if (writeMetadata) {
            TableMetadata other = TableMetadata.newTableMetadata(
                    new Schema(Types.NestedField.optional(1, "b", Types.IntegerType.get())),
                    PartitionSpec.unpartitioned(),
                    otherLocation,
                    ImmutableMap.of());
            TableMetadataParser.write(other, fileIo.newOutputFile(otherMetadataLocation));
        }
        return TableInput.builder()
                .name(tableName)
                .tableType(EXTERNAL_TABLE.name())
                .storageDescriptor(StorageDescriptor.builder().location(otherLocation).build())
                .parameters(ImmutableMap.of(
                        TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE.toUpperCase(ENGLISH),
                        METADATA_LOCATION_PROP, otherMetadataLocation))
                .build();
    }

    private class TestingGlueCatalogModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            install(new IcebergCatalogModule());
        }

        @ProvidesIntoSet
        @ForGlueHiveMetastore
        public ExecutionInterceptor createExecutionInterceptor()
        {
            return new ExecutionInterceptor()
            {
                @Override
                public void beforeExecution(Context.BeforeExecution context, ExecutionAttributes executionAttributes)
                {
                    if (!(context.request() instanceof CreateTableRequest request)) {
                        return;
                    }
                    if (otherTableTakesName.get()) {
                        glueClient.createTable(create -> create.databaseName(schemaName).tableInput(otherTable(request.tableInput().name(), true)));
                    }
                    else if (otherTableWithMissingMetadataTakesName.get()) {
                        glueClient.createTable(create -> create.databaseName(schemaName).tableInput(otherTable(request.tableInput().name(), false)));
                    }
                    if (metadataDirectoryReadOnlyBeforeFailure.get()) {
                        try {
                            Files.setPosixFilePermissions(dataDirectory.resolve(request.tableInput().name(), "metadata"), PosixFilePermissions.fromString("r-xr-xr-x"));
                        }
                        catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }

                @Override
                public void afterExecution(Context.AfterExecution context, ExecutionAttributes executionAttributes)
                {
                    if (!(context.request() instanceof CreateTableRequest request)) {
                        return;
                    }
                    // The create has been applied at this point, so the later commit builds on the table it created.
                    if (laterCommitBeforeFailure.get()) {
                        Table table = glueClient.getTable(get -> get.databaseName(request.databaseName()).name(request.tableInput().name())).table();
                        glueClient.updateTable(update -> update.databaseName(request.databaseName()).tableInput(withLaterCommit(table)));
                    }
                    GlueException failure = createTableFailure.get();
                    if (failure != null) {
                        throw failure;
                    }
                }
            };
        }
    }
}
