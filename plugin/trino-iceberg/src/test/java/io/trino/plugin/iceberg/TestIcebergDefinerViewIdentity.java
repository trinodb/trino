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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;
import io.trino.Session;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.local.LocalFileSystem;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.security.Identity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.iceberg.IcebergConnectorFactory.createConnector;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
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

    private final List<FileRead> fileReads = new CopyOnWriteArrayList<>();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema("test_schema")
                .setIdentity(Identity.ofUser(VIEW_OWNER))
                .build();

        QueryRunner queryRunner = DistributedQueryRunner.builder(session).build();

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

        fileReads.clear();
        assertQuery(viewReaderSession(), "SELECT * FROM " + viewName, "VALUES 1");

        assertThat(tableReadUsers(tableName)).containsExactly(VIEW_OWNER);
        assertThat(dataFileReadUsers(tableName)).containsExactly(VIEW_OWNER);
    }

    @Test
    void testInvokerViewTableIsAccessedAsQueryingUser()
    {
        String tableName = "test_invoker_table_" + randomNameSuffix();
        String viewName = "test_invoker_view_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 id", 1);
        assertUpdate("CREATE VIEW " + viewName + " SECURITY INVOKER AS SELECT * FROM " + tableName);

        fileReads.clear();
        assertQuery(viewReaderSession(), "SELECT * FROM " + viewName, "VALUES 1");

        assertThat(tableReadUsers(tableName)).containsExactly(VIEW_READER);
        assertThat(dataFileReadUsers(tableName)).containsExactly(VIEW_READER);
    }

    @Test
    void testTableIsAccessedAsQueryingUser()
    {
        String tableName = "test_table_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 id", 1);

        fileReads.clear();
        assertQuery(viewReaderSession(), "SELECT * FROM " + tableName, "VALUES 1");

        assertThat(tableReadUsers(tableName)).containsExactly(VIEW_READER);
        assertThat(dataFileReadUsers(tableName)).containsExactly(VIEW_READER);
    }

    private Session viewReaderSession()
    {
        return Session.builder(getSession())
                .setIdentity(Identity.ofUser(VIEW_READER))
                .build();
    }

    /**
     * Users which read a file of the table, for example a metadata file, a manifest or a data file.
     * The table directory is {@code <table name>-<random suffix>}, which keeps the metadata of the
     * Hive file metastore, read as the metastore user, out of the result.
     */
    private Set<String> tableReadUsers(String tableName)
    {
        return fileReads.stream()
                .filter(fileRead -> fileRead.location().contains("/" + tableName + "-"))
                .map(FileRead::user)
                .collect(toImmutableSet());
    }

    /**
     * Users which read a data file of the table. Data files are read on workers, so this covers the
     * identity sent with the plan fragment.
     */
    private Set<String> dataFileReadUsers(String tableName)
    {
        return fileReads.stream()
                .filter(fileRead -> fileRead.location().contains("/" + tableName + "-") && fileRead.location().contains("/data/"))
                .map(FileRead::user)
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
            fileReads.add(new FileRead(user, location.toString()));
            return super.newInputFile(location);
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length)
        {
            fileReads.add(new FileRead(user, location.toString()));
            return super.newInputFile(location, length);
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
        {
            fileReads.add(new FileRead(user, location.toString()));
            return super.newInputFile(location, length, lastModified);
        }
    }

    private record FileRead(String user, String location) {}
}
