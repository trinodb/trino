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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Key;
import com.google.inject.Module;
import io.trino.Session;
import io.trino.execution.QueryInfo;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.local.LocalFileSystem;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.spi.QueryId;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryMetadata;
import io.trino.spi.security.Identity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.iceberg.IcebergConnectorFactory.createConnector;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the tables of a {@code SECURITY DEFINER} view are accessed as the view owner for
 * the whole query, not only during analysis. The identity is captured in the file system, which is
 * where connectors act on it, for example when they vend credentials for a table.
 */
@Execution(ExecutionMode.SAME_THREAD)
final class TestIcebergDefinerViewIdentity
        extends AbstractTestQueryFramework
{
    private static final String VIEW_OWNER = "view_owner";
    private static final String VIEW_READER = "view_reader";
    private static final String VIEW_OWNER_GROUP = "view_owners";
    private static final String INNER_VIEW_OWNER = "inner_view_owner";

    private final List<FileAccess> fileReads = new CopyOnWriteArrayList<>();
    private final List<QueryCompletedEvent> completedQueries = new CopyOnWriteArrayList<>();
    private final List<FileAccess> fileWrites = new CopyOnWriteArrayList<>();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema("test_schema")
                .setIdentity(Identity.ofUser(VIEW_OWNER))
                .build();

        QueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setEventListener(new EventListener()
                {
                    @Override
                    public void queryCompleted(QueryCompletedEvent event)
                    {
                        completedQueries.add(event);
                    }
                })
                .build();
        // the owner's groups are part of the identity a definer view's tables are resolved as
        queryRunner.getCoordinator().getGroupProvider().setConfiguredGroupProvider(user -> user.equals(VIEW_OWNER) ? ImmutableSet.of(VIEW_OWNER_GROUP) : ImmutableSet.of());

        Path dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data");
        dataDirectory.toFile().mkdirs();
        queryRunner.installPlugin(new IdentityRecordingIcebergPlugin(dataDirectory));
        queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", ImmutableMap.of(
                // files are expected to be read from the file system, not from a cache
                "iceberg.metadata-cache.enabled", "false"));
        queryRunner.execute("CREATE SCHEMA test_schema");

        return queryRunner;
    }

    @Test
    void testDefinerViewTableIsAccessedAsViewOwner()
    {
        String tableName = "test_definer_table_" + randomNameSuffix();
        String viewName = "test_definer_view_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 id", 1);
        assertUpdate("CREATE VIEW " + viewName + " SECURITY DEFINER AS SELECT * FROM " + tableName);
        String tableLocation = getTableLocation(tableName);

        fileReads.clear();
        assertQuery(viewReaderSession(), "SELECT * FROM " + viewName, "VALUES 1");

        assertThat(tableReadUsers(tableLocation)).containsExactly(VIEW_OWNER);
        assertThat(dataFileReadUsers(tableLocation)).containsExactly(VIEW_OWNER);
    }

    @Test
    void testQueryDetailsDoNotExposeTheIdentityOfTheViewOwner()
            throws Exception
    {
        String tableName = "test_reported_table_" + randomNameSuffix();
        String viewName = "test_reported_view_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 id", 1);
        assertUpdate("CREATE VIEW " + viewName + " SECURITY DEFINER AS SELECT * FROM " + tableName);

        QueryId queryId = getDistributedQueryRunner().executeWithPlan(viewReaderSession(), "SELECT * FROM " + viewName).queryId();

        QueryInfo queryInfo = getDistributedQueryRunner().getCoordinator().getFullQueryInfo(queryId);
        ObjectMapper objectMapper = getDistributedQueryRunner().getCoordinator().getInstance(Key.get(ObjectMapper.class));
        String stages = objectMapper.writeValueAsString(queryInfo.getStages().orElseThrow());

        // the plan of the scan is reported, but not the identity it was resolved as
        assertThat(stages).contains(tableName);
        assertThat(stages).doesNotContain(VIEW_OWNER, VIEW_OWNER_GROUP);

        for (String explain : List.of("EXPLAIN ", "EXPLAIN ANALYZE VERBOSE ")) {
            String plan = (String) computeActual(viewReaderSession(), explain + "SELECT * FROM " + viewName).getOnlyValue();
            assertThat(plan).contains(tableName);
            assertThat(plan).doesNotContain(VIEW_OWNER, VIEW_OWNER_GROUP);
        }

        // a listener receives the stage plans of the query as well
        assertEventually(() -> assertThat(completedQueries).anyMatch(event -> event.getMetadata().getQueryId().equals(queryId.toString())));
        QueryMetadata metadata = completedQueries.stream()
                .filter(event -> event.getMetadata().getQueryId().equals(queryId.toString()))
                .map(QueryCompletedEvent::getMetadata)
                .findFirst()
                .orElseThrow();
        assertThat(metadata.getPayload().orElseThrow()).contains(tableName);
        assertThat(metadata.getPayload().orElseThrow()).doesNotContain(VIEW_OWNER, VIEW_OWNER_GROUP);
        assertThat(metadata.getPlan().orElseThrow()).doesNotContain(VIEW_OWNER, VIEW_OWNER_GROUP);
        assertThat(metadata.getJsonPlan().orElseThrow()).doesNotContain(VIEW_OWNER, VIEW_OWNER_GROUP);
    }

    @Test
    void testInvokerViewTableIsAccessedAsQueryingUser()
    {
        String tableName = "test_invoker_table_" + randomNameSuffix();
        String viewName = "test_invoker_view_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 id", 1);
        assertUpdate("CREATE VIEW " + viewName + " SECURITY INVOKER AS SELECT * FROM " + tableName);
        String tableLocation = getTableLocation(tableName);

        fileReads.clear();
        assertQuery(viewReaderSession(), "SELECT * FROM " + viewName, "VALUES 1");

        assertThat(tableReadUsers(tableLocation)).containsExactly(VIEW_READER);
        assertThat(dataFileReadUsers(tableLocation)).containsExactly(VIEW_READER);
    }

    @Test
    void testTableIsAccessedAsQueryingUser()
    {
        String tableName = "test_table_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 id", 1);
        String tableLocation = getTableLocation(tableName);

        fileReads.clear();
        assertQuery(viewReaderSession(), "SELECT * FROM " + tableName, "VALUES 1");

        assertThat(tableReadUsers(tableLocation)).containsExactly(VIEW_READER);
        assertThat(dataFileReadUsers(tableLocation)).containsExactly(VIEW_READER);
    }

    @ParameterizedTest
    @MethodSource("nestedViewSecurityModes")
    void testNestedViewTableIsAccessedAsTheIdentityTheInnerViewRunsAs(String outerMode, String innerMode, String expectedUser)
    {
        String tableName = "test_nested_table_" + randomNameSuffix();
        String innerViewName = "test_inner_view_" + randomNameSuffix();
        String outerViewName = "test_outer_view_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 id", 1);
        assertUpdate(sessionOf(INNER_VIEW_OWNER), "CREATE VIEW " + innerViewName + " SECURITY " + innerMode + " AS SELECT * FROM " + tableName);
        assertUpdate("CREATE VIEW " + outerViewName + " SECURITY " + outerMode + " AS SELECT * FROM " + innerViewName);
        String tableLocation = getTableLocation(tableName);

        fileReads.clear();
        assertQuery(viewReaderSession(), "SELECT * FROM " + outerViewName, "VALUES 1");

        assertThat(tableReadUsers(tableLocation)).containsExactly(expectedUser);
        assertThat(dataFileReadUsers(tableLocation)).containsExactly(expectedUser);
    }

    static Stream<Arguments> nestedViewSecurityModes()
    {
        // A DEFINER view runs as its owner. An INVOKER view runs as whoever reads it, which for a nested view is the
        // identity the outer view runs as.
        return Stream.of(
                Arguments.of("DEFINER", "DEFINER", INNER_VIEW_OWNER),
                Arguments.of("DEFINER", "INVOKER", VIEW_OWNER),
                Arguments.of("INVOKER", "DEFINER", INNER_VIEW_OWNER),
                Arguments.of("INVOKER", "INVOKER", VIEW_READER));
    }

    @Test
    void testWriteTargetIsAccessedAsQueryingUser()
    {
        String sourceTableName = "test_write_source_" + randomNameSuffix();
        String viewName = "test_write_view_" + randomNameSuffix();
        String targetTableName = "test_write_target_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + sourceTableName + " AS SELECT 1 id", 1);
        assertUpdate("CREATE VIEW " + viewName + " SECURITY DEFINER AS SELECT * FROM " + sourceTableName);
        assertUpdate(viewReaderSession(), "CREATE TABLE " + targetTableName + " (id integer)");
        String sourceTableLocation = getTableLocation(sourceTableName);
        String targetTableLocation = getTableLocation(targetTableName);

        fileReads.clear();
        fileWrites.clear();
        assertUpdate(viewReaderSession(), "INSERT INTO " + targetTableName + " SELECT * FROM " + viewName, 1);

        // the table behind the view is read as the view owner, the target of the write stays with the querying user
        assertThat(dataFileReadUsers(sourceTableLocation)).containsExactly(VIEW_OWNER);
        assertThat(fileWriteUsers(targetTableLocation)).containsExactly(VIEW_READER);
    }

    @Test
    void testDefinerViewTableFunction()
    {
        String tableName = "test_function_table_" + randomNameSuffix();
        String viewName = "test_function_view_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 id", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES 2", 1);
        List<Object> snapshots = computeActual("SELECT snapshot_id FROM \"" + tableName + "$snapshots\" ORDER BY committed_at").getOnlyColumn().toList();
        assertUpdate("CREATE VIEW " + viewName + " SECURITY DEFINER AS " +
                "SELECT id FROM TABLE(system.table_changes('test_schema', '" + tableName + "', " + snapshots.getFirst() + ", " + snapshots.getLast() + "))");
        String tableLocation = getTableLocation(tableName);

        fileReads.clear();
        assertQuery(viewReaderSession(), "SELECT * FROM " + viewName, "VALUES 2");

        // TODO (https://github.com/trinodb/trino/issues/15736) The function is analyzed as the view owner, but the handle of a
        //  table function does not record the identity it was resolved as, so the table it reads is accessed as the querying user.
        assertThat(tableReadUsers(tableLocation)).containsOnly(VIEW_OWNER, VIEW_READER);
        assertThat(dataFileReadUsers(tableLocation)).containsExactly(VIEW_READER);
    }

    private Session viewReaderSession()
    {
        return sessionOf(VIEW_READER);
    }

    private Session sessionOf(String user)
    {
        return Session.builder(getSession())
                .setIdentity(Identity.ofUser(user))
                .build();
    }

    private String getTableLocation(String tableName)
    {
        Pattern locationPattern = Pattern.compile(".*location = '(.*?)'.*", Pattern.DOTALL);
        Matcher matcher = locationPattern.matcher((String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue());
        verify(matcher.find(), "Location not found in SHOW CREATE TABLE result");
        String location = matcher.group(1);
        verify(!matcher.find(), "Unexpected second match");
        return location;
    }

    /**
     * Users which read a file of the table, for example a metadata file, a manifest or a data file.
     */
    private Set<String> tableReadUsers(String tableLocation)
    {
        return usersOf(fileReads, tableLocation);
    }

    /**
     * Users which read a data file of the table. Data files are read on workers, so this covers the
     * identity sent with the plan fragment.
     */
    private Set<String> dataFileReadUsers(String tableLocation)
    {
        return usersOf(fileReads, tableLocation + "/data/");
    }

    private Set<String> fileWriteUsers(String tableLocation)
    {
        return usersOf(fileWrites, tableLocation);
    }

    private static Set<String> usersOf(List<FileAccess> accesses, String locationPrefix)
    {
        return accesses.stream()
                .filter(access -> access.location().startsWith(locationPrefix))
                .map(FileAccess::user)
                .collect(toImmutableSet());
    }

    private class IdentityRecordingIcebergPlugin
            extends IcebergPlugin
    {
        private final Path localFileSystemRootPath;

        public IdentityRecordingIcebergPlugin(Path localFileSystemRootPath)
        {
            this.localFileSystemRootPath = requireNonNull(localFileSystemRootPath, "localFileSystemRootPath is null");
        }

        @Override
        public Iterable<ConnectorFactory> getConnectorFactories()
        {
            return ImmutableList.of(new IdentityRecordingIcebergConnectorFactory(localFileSystemRootPath));
        }
    }

    private class IdentityRecordingIcebergConnectorFactory
            implements ConnectorFactory
    {
        private final Path localFileSystemRootPath;

        public IdentityRecordingIcebergConnectorFactory(Path localFileSystemRootPath)
        {
            this.localFileSystemRootPath = requireNonNull(localFileSystemRootPath, "localFileSystemRootPath is null");
        }

        @Override
        public String getName()
        {
            return "iceberg";
        }

        @Override
        public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
        {
            Module module = binder -> {
                newMapBinder(binder, String.class, TrinoFileSystemFactory.class)
                        .addBinding("local")
                        .toInstance(identity -> new IdentityRecordingFileSystem(localFileSystemRootPath, identity.getUser()));
                configBinder(binder).bindConfigDefaults(
                        FileHiveMetastoreConfig.class,
                        metastoreConfig -> metastoreConfig.setCatalogDirectory("local:///" + catalogName));
            };
            return createConnector(
                    catalogName,
                    ImmutableMap.<String, String>builder()
                            .putAll(config)
                            .put("iceberg.catalog.type", "TESTING_FILE_METASTORE")
                            .buildOrThrow(),
                    context,
                    module,
                    Optional.empty());
        }
    }

    private class IdentityRecordingFileSystem
            extends LocalFileSystem
    {
        private final String user;

        public IdentityRecordingFileSystem(Path rootPath, String user)
        {
            super(rootPath);
            this.user = user;
        }

        @Override
        public TrinoInputFile newInputFile(Location location)
        {
            fileReads.add(new FileAccess(user, location.toString()));
            return super.newInputFile(location);
        }

        @Override
        public TrinoOutputFile newOutputFile(Location location)
        {
            fileWrites.add(new FileAccess(user, location.toString()));
            return super.newOutputFile(location);
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length)
        {
            fileReads.add(new FileAccess(user, location.toString()));
            return super.newInputFile(location, length);
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
        {
            fileReads.add(new FileAccess(user, location.toString()));
            return super.newInputFile(location, length, lastModified);
        }
    }

    private record FileAccess(String user, String location) {}
}
