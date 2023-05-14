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
package io.trino.plugin.deltalake.metastore.glue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.io.Resources;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.json.JsonModule;
import io.opentelemetry.api.trace.Tracer;
import io.trino.filesystem.manager.FileSystemModule;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.deltalake.DeltaLakeMetadata;
import io.trino.plugin.deltalake.DeltaLakeMetadataFactory;
import io.trino.plugin.deltalake.DeltaLakeModule;
import io.trino.plugin.deltalake.metastore.DeltaLakeMetastoreModule;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastore;
import io.trino.spi.NodeManager;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.type.TypeManager;
import io.trino.testing.TestingConnectorContext;
import io.trino.testing.TestingConnectorSession;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.testing.Closeables.closeAll;
import static io.trino.plugin.deltalake.DeltaLakeMetadata.DELTA_STORAGE_FORMAT;
import static io.trino.plugin.deltalake.DeltaLakeTableProperties.LOCATION_PROPERTY;
import static io.trino.plugin.deltalake.metastore.HiveMetastoreBackedDeltaLakeMetastore.TABLE_PROVIDER_PROPERTY;
import static io.trino.plugin.deltalake.metastore.HiveMetastoreBackedDeltaLakeMetastore.TABLE_PROVIDER_VALUE;
import static io.trino.plugin.hive.HiveStorageFormat.PARQUET;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static io.trino.spi.security.PrincipalType.ROLE;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;
import static org.apache.hadoop.hive.metastore.TableType.VIRTUAL_VIEW;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDeltaLakeGlueMetastore
{
    private File tempDir;
    private LifeCycleManager lifeCycleManager;
    private HiveMetastore metastoreClient;
    private DeltaLakeMetadataFactory metadataFactory;
    private String databaseName;
    private TestingConnectorSession session;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        tempDir = Files.createTempDirectory(null).toFile();
        String temporaryLocation = tempDir.toURI().toString();

        Map<String, String> config = ImmutableMap.<String, String>builder()
                .put("hive.metastore", "glue")
                .put("delta.hide-non-delta-lake-tables", "true")
                .buildOrThrow();

        Bootstrap app = new Bootstrap(
                // connector dependencies
                new JsonModule(),
                binder -> {
                    ConnectorContext context = new TestingConnectorContext();
                    binder.bind(CatalogName.class).toInstance(new CatalogName("test"));
                    binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                    binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                    binder.bind(PageIndexerFactory.class).toInstance(context.getPageIndexerFactory());
                    binder.bind(NodeVersion.class).toInstance(new NodeVersion("test_version"));
                    binder.bind(Tracer.class).toInstance(context.getTracer());
                },
                // connector modules
                new DeltaLakeMetastoreModule(),
                new DeltaLakeModule(),
                // test setup
                binder -> binder.bind(HdfsEnvironment.class).toInstance(HDFS_ENVIRONMENT),
                new FileSystemModule());

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();

        lifeCycleManager = injector.getInstance(LifeCycleManager.class);
        metastoreClient = injector.getInstance(GlueHiveMetastore.class);
        metadataFactory = injector.getInstance(DeltaLakeMetadataFactory.class);

        session = TestingConnectorSession.builder()
                .setPropertyMetadata(injector.getInstance(Key.get(new TypeLiteral<Set<SessionPropertiesProvider>>() {})).stream()
                        .map(SessionPropertiesProvider::getSessionProperties)
                        .flatMap(List::stream)
                        .collect(toImmutableList()))
                .build();

        databaseName = "test_delta_glue" + randomNameSuffix();
        metastoreClient.createDatabase(Database.builder()
                .setDatabaseName(databaseName)
                .setOwnerName(Optional.of("public"))
                .setOwnerType(Optional.of(ROLE))
                .setLocation(Optional.of(temporaryLocation))
                .build());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        closeAll(
                () -> metastoreClient.dropDatabase(databaseName, true),
                () -> lifeCycleManager.stop(),
                () -> {
                    if (tempDir.exists()) {
                        deleteRecursively(tempDir.toPath(), ALLOW_INSECURE);
                    }
                });

        databaseName = null;
        lifeCycleManager = null;
        tempDir = null;
    }

    @Test
    public void testHideNonDeltaLakeTable()
            throws Exception
    {
        SchemaTableName deltaLakeTable = new SchemaTableName(databaseName, "delta_lake_table_" + randomNameSuffix());
        SchemaTableName nonDeltaLakeTable1 = new SchemaTableName(databaseName, "hive_table_" + randomNameSuffix());
        SchemaTableName nonDeltaLakeTable2 = new SchemaTableName(databaseName, "hive_table_" + randomNameSuffix());
        SchemaTableName nonDeltaLakeView1 = new SchemaTableName(databaseName, "hive_view_" + randomNameSuffix());

        String deltaLakeTableLocation = tableLocation(deltaLakeTable);
        createTable(deltaLakeTable, deltaLakeTableLocation, tableBuilder -> {
            tableBuilder.setParameter(TABLE_PROVIDER_PROPERTY, TABLE_PROVIDER_VALUE);
            tableBuilder.setParameter(LOCATION_PROPERTY, deltaLakeTableLocation);
            tableBuilder.getStorageBuilder()
                    // this mimics what Databricks is doing when creating a Delta table in the Hive metastore
                    .setStorageFormat(DELTA_STORAGE_FORMAT)
                    .setSerdeParameters(ImmutableMap.of(DeltaLakeMetadata.PATH_PROPERTY, deltaLakeTableLocation))
                    .setLocation(deltaLakeTableLocation);
        });
        createTransactionLog(deltaLakeTableLocation);

        createTable(nonDeltaLakeTable1, tableLocation(nonDeltaLakeTable1), tableBuilder -> {});
        createTable(nonDeltaLakeTable2, tableLocation(nonDeltaLakeTable2), tableBuilder -> tableBuilder.setParameter(TABLE_PROVIDER_PROPERTY, "foo"));
        createView(nonDeltaLakeView1, tableLocation(nonDeltaLakeTable1), tableBuilder -> {});

        DeltaLakeMetadata metadata = metadataFactory.create(SESSION.getIdentity());

        // Verify the tables were created as non Delta Lake tables
        assertThatThrownBy(() -> metadata.getTableHandle(session, nonDeltaLakeTable1))
                .isInstanceOf(TrinoException.class)
                .hasMessage(format("%s is not a Delta Lake table", nonDeltaLakeTable1));
        assertThatThrownBy(() -> metadata.getTableHandle(session, nonDeltaLakeTable2))
                .isInstanceOf(TrinoException.class)
                .hasMessage(format("%s is not a Delta Lake table", nonDeltaLakeTable2));

        // TODO (https://github.com/trinodb/trino/issues/5426)
        //  these assertions should use information_schema instead of metadata directly,
        //  as information_schema or MetadataManager may apply additional logic

        // list all tables
        assertThat(metadata.listTables(session, Optional.empty()))
                .contains(deltaLakeTable)
                .doesNotContain(nonDeltaLakeTable1)
                .doesNotContain(nonDeltaLakeTable2);

        // list all tables in a schema
        assertThat(metadata.listTables(session, Optional.of(databaseName)))
                .contains(deltaLakeTable)
                .doesNotContain(nonDeltaLakeTable1)
                .doesNotContain(nonDeltaLakeTable2);

        // list all columns in a schema
        assertThat(listTableColumns(metadata, new SchemaTablePrefix(databaseName)))
                .contains(deltaLakeTable)
                .doesNotContain(nonDeltaLakeTable1)
                .doesNotContain(nonDeltaLakeTable2);

        // list all columns in a table
        assertThat(listTableColumns(metadata, new SchemaTablePrefix(databaseName, deltaLakeTable.getTableName())))
                .contains(deltaLakeTable)
                .doesNotContain(nonDeltaLakeTable1)
                .doesNotContain(nonDeltaLakeTable2);
        assertThat(listTableColumns(metadata, new SchemaTablePrefix(databaseName, nonDeltaLakeTable1.getTableName())))
                .isEmpty();
        assertThat(listTableColumns(metadata, new SchemaTablePrefix(databaseName, nonDeltaLakeTable2.getTableName())))
                .isEmpty();
        assertThat(listTableColumns(metadata, new SchemaTablePrefix(databaseName, nonDeltaLakeView1.getTableName())))
                .isEmpty();
    }

    private Set<SchemaTableName> listTableColumns(DeltaLakeMetadata metadata, SchemaTablePrefix tablePrefix)
    {
        List<TableColumnsMetadata> allTableColumns = ImmutableList.copyOf(metadata.streamTableColumns(session, tablePrefix));

        Set<SchemaTableName> redirectedTables = allTableColumns.stream()
                .filter(tableColumns -> tableColumns.getColumns().isEmpty())
                .map(TableColumnsMetadata::getTable)
                .collect(toImmutableSet());

        if (!redirectedTables.isEmpty()) {
            throw new IllegalStateException("Unexpected redirects reported for tables: " + redirectedTables);
        }

        return allTableColumns.stream()
                .map(TableColumnsMetadata::getTable)
                .collect(toImmutableSet());
    }

    /**
     * Creates a valid transaction log
     */
    private void createTransactionLog(String deltaLakeTableLocation)
            throws URISyntaxException, IOException
    {
        File deltaTableLogLocation = new File(new File(new URI(deltaLakeTableLocation)), "_delta_log");
        verify(deltaTableLogLocation.mkdirs(), "mkdirs() on '%s' failed", deltaTableLogLocation);
        String entry = Resources.toString(Resources.getResource("deltalake/person/_delta_log/00000000000000000000.json"), UTF_8);
        Files.writeString(new File(deltaTableLogLocation, "00000000000000000000.json").toPath(), entry);
    }

    private String tableLocation(SchemaTableName tableName)
    {
        return new File(tempDir, tableName.getTableName()).toURI().toString();
    }

    private void createTable(SchemaTableName tableName, String tableLocation, Consumer<Table.Builder> tableConfiguration)
    {
        Table.Builder table = Table.builder()
                .setDatabaseName(tableName.getSchemaName())
                .setTableName(tableName.getTableName())
                .setOwner(Optional.of(session.getUser()))
                .setTableType(EXTERNAL_TABLE.name())
                .setDataColumns(List.of(new Column("a_column", HIVE_STRING, Optional.empty())));

        table.getStorageBuilder()
                .setStorageFormat(fromHiveStorageFormat(PARQUET))
                .setLocation(tableLocation);

        tableConfiguration.accept(table);

        PrincipalPrivileges principalPrivileges = new PrincipalPrivileges(ImmutableMultimap.of(), ImmutableMultimap.of());
        metastoreClient.createTable(table.build(), principalPrivileges);
    }

    private void createView(SchemaTableName viewName, String tableLocation, Consumer<Table.Builder> tableConfiguration)
    {
        Table.Builder table = Table.builder()
                .setDatabaseName(viewName.getSchemaName())
                .setTableName(viewName.getTableName())
                .setOwner(Optional.of(session.getUser()))
                .setTableType(VIRTUAL_VIEW.name())
                .setDataColumns(List.of(new Column("a_column", HIVE_STRING, Optional.empty())));

        table.getStorageBuilder()
                .setStorageFormat(fromHiveStorageFormat(PARQUET))
                .setLocation(tableLocation);

        tableConfiguration.accept(table);

        PrincipalPrivileges principalPrivileges = new PrincipalPrivileges(ImmutableMultimap.of(), ImmutableMultimap.of());
        metastoreClient.createTable(table.build(), principalPrivileges);
    }
}
