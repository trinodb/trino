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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.metastore.TableInfo;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.plugin.iceberg.CommitTaskData;
import io.trino.plugin.iceberg.DefaultIcebergFileSystemFactory;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergMetadata;
import io.trino.plugin.iceberg.IcebergSessionProperties;
import io.trino.plugin.iceberg.TableStatisticsWriter;
import io.trino.plugin.iceberg.catalog.BaseTrinoCatalogTest;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogConfig.Security;
import io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogConfig.SessionType;
import io.trino.plugin.iceberg.encryption.IcebergEncryptionConfig;
import io.trino.spi.NodeVersion;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ConnectorExpressionEvaluator;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.ConnectorViewDefinition.ViewColumn;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.testing.TestingConnectorSession;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.rest.DelegatingRestSessionCatalog;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.View;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.units.Duration.ZERO;
import static io.trino.hdfs.HdfsTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.metastore.TableInfo.ExtendedRelationType.OTHER_VIEW;
import static io.trino.plugin.iceberg.IcebergTestUtils.TABLE_STATISTICS_READER;
import static io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogConfig.SessionType.NONE;
import static io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogConfig.SessionType.USER;
import static io.trino.plugin.iceberg.catalog.rest.RestCatalogTestUtils.backendCatalog;
import static io.trino.plugin.iceberg.delete.DeletionVectorWriter.UNSUPPORTED_DELETION_VECTOR_WRITER;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.INTEGER;

public class TestTrinoRestCatalog
        extends BaseTrinoCatalogTest
{
    @Override
    protected TrinoCatalog createTrinoCatalog(boolean useUniqueTableLocations)
            throws IOException
    {
        return createTrinoRestCatalog(useUniqueTableLocations, ImmutableMap.of());
    }

    @Override
    protected void createNamespaceWithProperties(TrinoCatalog catalog, String namespace, Map<String, String> properties)
    {
        catalog.createNamespace(
                SESSION,
                namespace,
                properties.entrySet().stream()
                        .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)),
                new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
    }

    private static TrinoRestCatalog createTrinoRestCatalog(boolean useUniqueTableLocations, Map<String, String> properties)
            throws IOException
    {
        Path warehouseLocation = Files.createTempDirectory(null);

        String catalogName = "iceberg_rest";
        RESTSessionCatalog restSessionCatalog = DelegatingRestSessionCatalog
                .builder()
                .delegate(backendCatalog(warehouseLocation))
                .build();

        restSessionCatalog.initialize(catalogName, properties);

        return createTrinoRestCatalog(useUniqueTableLocations, restSessionCatalog, false, false);
    }

    private static TrinoRestCatalog createTrinoRestCatalog(
            boolean useUniqueTableLocations,
            RESTSessionCatalog restSessionCatalog,
            boolean nestedNamespaceEnabled,
            boolean caseInsensitiveNameMatching)
    {
        return createTrinoRestCatalog(
                useUniqueTableLocations,
                restSessionCatalog,
                nestedNamespaceEnabled,
                caseInsensitiveNameMatching,
                NONE,
                Optional.empty(),
                Optional.empty());
    }

    private static TrinoRestCatalog createTrinoRestCatalog(
            boolean useUniqueTableLocations,
            RESTSessionCatalog restSessionCatalog,
            boolean nestedNamespaceEnabled,
            boolean caseInsensitiveNameMatching,
            SessionType sessionType,
            Optional<Cache<NamespaceListingKey, List<TableIdentifier>>> namespaceTableListingCache,
            Optional<Cache<NamespaceListingKey, List<TableIdentifier>>> namespaceViewListingCache)
    {
        String catalogName = "iceberg_rest";
        return new TrinoRestCatalog(
                new DefaultIcebergFileSystemFactory(HDFS_FILE_SYSTEM_FACTORY),
                restSessionCatalog,
                new CatalogName(catalogName),
                Security.NONE,
                sessionType,
                ImmutableMap.of(),
                nestedNamespaceEnabled,
                "test",
                TESTING_TYPE_MANAGER,
                useUniqueTableLocations,
                caseInsensitiveNameMatching,
                EvictableCacheBuilder.newBuilder().expireAfterWrite(1000, MILLISECONDS).shareNothingWhenDisabled().build(),
                EvictableCacheBuilder.newBuilder().expireAfterWrite(1000, MILLISECONDS).shareNothingWhenDisabled().build(),
                namespaceTableListingCache,
                namespaceViewListingCache,
                true,
                false);
    }

    private static Cache<NamespaceListingKey, List<TableIdentifier>> createNamespaceListingCache()
    {
        return EvictableCacheBuilder.newBuilder()
                .expireAfterWrite(1, MINUTES)
                .maximumWeight(10_000)
                .<NamespaceListingKey, List<TableIdentifier>>weigher((_, identifiers) -> identifiers.size() + 1)
                .shareNothingWhenDisabled()
                .build();
    }

    /**
     * Delegating catalog that counts invocations of {@code methodName} with a single argument,
     * so tests can assert on the REST-layer traffic actually reaching the backend.
     */
    private Catalog countingBackend(Catalog backend, String methodName, AtomicInteger counter)
    {
        return (Catalog) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {Catalog.class, SupportsNamespaces.class, ViewCatalog.class, Closeable.class},
                (_, method, args) -> {
                    if (method.getName().equals(methodName) && args != null && args.length == 1) {
                        counter.incrementAndGet();
                    }
                    try {
                        return method.invoke(backend, args);
                    }
                    catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    @Test
    @Override
    public void testNonLowercaseNamespace()
            throws Exception
    {
        TrinoCatalog catalog = createTrinoCatalog(false);

        String namespace = "testNonLowercaseNamespace" + randomNameSuffix();
        String schema = namespace.toLowerCase(ENGLISH);

        catalog.createNamespace(SESSION, namespace, defaultNamespaceProperties(namespace), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
        try {
            assertThat(catalog.namespaceExists(SESSION, namespace)).as("catalog.namespaceExists(namespace)")
                    .isTrue();
            assertThat(catalog.namespaceExists(SESSION, schema)).as("catalog.namespaceExists(schema)")
                    .isFalse();
            assertThat(catalog.listNamespaces(SESSION)).as("catalog.listNamespaces")
                    // Catalog listNamespaces may be used as a default implementation for ConnectorMetadata.schemaExists
                    .doesNotContain(schema)
                    .contains(namespace);

            // Test with IcebergMetadata, should the ConnectorMetadata implementation behavior depend on that class
            ConnectorMetadata icebergMetadata = new IcebergMetadata(
                    new CatalogName("iceberg"),
                    PLANNER_CONTEXT.getTypeManager(),
                    jsonCodec(CommitTaskData.class),
                    catalog,
                    (_, _) -> {
                        throw new UnsupportedOperationException();
                    },
                    TABLE_STATISTICS_READER,
                    new TableStatisticsWriter(new NodeVersion("test-version")),
                    UNSUPPORTED_DELETION_VECTOR_WRITER,
                    Optional.empty(),
                    false,
                    _ -> false,
                    newDirectExecutorService(),
                    directExecutor(),
                    newDirectExecutorService(),
                    newDirectExecutorService(),
                    0,
                    ZERO,
                    ConnectorExpressionEvaluator.NO_OP);
            assertThat(icebergMetadata.schemaExists(SESSION, namespace)).as("icebergMetadata.schemaExists(namespace)")
                    .isTrue();
            assertThat(icebergMetadata.schemaExists(SESSION, schema)).as("icebergMetadata.schemaExists(schema)")
                    .isFalse();
            assertThat(icebergMetadata.listSchemaNames(SESSION)).as("icebergMetadata.listSchemaNames")
                    .doesNotContain(schema)
                    .contains(namespace);
        }
        finally {
            catalog.dropNamespace(SESSION, namespace);
        }
    }

    @Test
    public void testPrefix()
            throws Exception
    {
        TrinoCatalog catalog = createTrinoRestCatalog(false, ImmutableMap.of("prefix", "dev"));

        String namespace = "testPrefixNamespace" + randomNameSuffix();

        assertThatThrownBy(() ->
                catalog.createNamespace(
                        SESSION,
                        namespace,
                        defaultNamespaceProperties(namespace),
                        new TrinoPrincipal(PrincipalType.USER, SESSION.getUser())))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Failed to create namespace")
                .cause()
                .as("should fail as the prefix dev is not implemented for the current endpoint")
                .hasMessageContaining("Malformed request: No route for request: POST v1/dev/namespaces");
    }

    @Test
    public void testNestedListNamespacesIgnoresNamespaceDeletedDuringRecursiveListing()
    {
        TrinoCatalog catalog = createTrinoRestCatalog(false, new NamespaceDeletedDuringRecursiveListingCatalog(), true, false);

        assertThat(catalog.listNamespaces(SESSION))
                .containsExactly("ExIsTiNg", "ExIsTiNg.child");
    }

    @Test
    public void testCaseInsensitiveNamespaceLookupIgnoresNamespaceDeletedDuringRecursiveListing()
    {
        TrinoCatalog catalog = createTrinoRestCatalog(false, new NamespaceDeletedDuringRecursiveListingCatalog(), false, true);

        assertThat(catalog.namespaceExists(SESSION, "existing")).isTrue();
    }

    @Test
    public void testNestedListNamespacesReusesSessionContext()
    {
        Map<String, Integer> sessionIdCounts = new ConcurrentHashMap<>();
        RESTSessionCatalog restSessionCatalog = new NamespaceDeletedDuringRecursiveListingCatalog()
        {
            @Override
            public List<Namespace> listNamespaces(SessionContext context, Namespace namespace)
            {
                sessionIdCounts.merge(context.sessionId(), 1, Integer::sum);
                return super.listNamespaces(context, namespace);
            }
        };

        TrinoRestCatalog catalog = createTrinoRestCatalog(false, restSessionCatalog, true, false);

        catalog.listNamespaces(SESSION);

        assertThat(sessionIdCounts.values()).singleElement(INTEGER).isGreaterThan(1);
    }

    @Test
    public void testNestedListNamespacesPropagatesRecursiveRestFailures()
    {
        RESTSessionCatalog restSessionCatalog = new RESTSessionCatalog()
        {
            @Override
            public List<Namespace> listNamespaces(SessionContext context, Namespace namespace)
            {
                if (namespace.isEmpty()) {
                    return List.of(Namespace.of("failing"));
                }
                throw new RESTException("catalog failure");
            }
        };
        TrinoCatalog catalog = createTrinoRestCatalog(false, restSessionCatalog, true, false);

        assertThatThrownBy(() -> catalog.listNamespaces(SESSION))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Failed to list namespaces")
                .cause()
                .isInstanceOf(RESTException.class)
                .hasMessage("catalog failure");
    }

    private static class NamespaceDeletedDuringRecursiveListingCatalog
            extends RESTSessionCatalog
    {
        private static final Namespace EXISTING_NAMESPACE = Namespace.of("ExIsTiNg");
        private static final Namespace EXISTING_CHILD_NAMESPACE = Namespace.of("ExIsTiNg", "child");
        private static final Namespace DELETED_NAMESPACE = Namespace.of("deleted");

        @Override
        public List<Namespace> listNamespaces(SessionContext context, Namespace namespace)
        {
            if (namespace.isEmpty()) {
                return List.of(EXISTING_NAMESPACE, DELETED_NAMESPACE);
            }
            if (namespace.equals(EXISTING_NAMESPACE)) {
                return List.of(EXISTING_CHILD_NAMESPACE);
            }
            if (namespace.equals(EXISTING_CHILD_NAMESPACE)) {
                return List.of();
            }
            if (namespace.equals(DELETED_NAMESPACE)) {
                throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
            }
            throw new AssertionError("Unexpected namespace: " + namespace);
        }

        @Override
        public boolean namespaceExists(SessionContext context, Namespace namespace)
        {
            return namespace.equals(EXISTING_NAMESPACE);
        }
    }

    @Test
    public void testCaseInsensitiveNamespaceListingCacheReusesListing()
            throws IOException
    {
        // Case-insensitive resolutions in one namespace share a single listTables call.

        Path warehouseLocation = Files.createTempDirectory(null);
        warehouseLocation.toFile().deleteOnExit();

        String catalogName = "iceberg_rest";
        Catalog backend = backendCatalog(warehouseLocation);
        AtomicInteger listTablesCount = new AtomicInteger();

        RESTSessionCatalog restSessionCatalog = DelegatingRestSessionCatalog.builder()
                .delegate(countingBackend(backend, "listTables", listTablesCount))
                .build();
        restSessionCatalog.initialize(catalogName, ImmutableMap.of());

        Cache<NamespaceListingKey, List<TableIdentifier>> listingCache = createNamespaceListingCache();

        TrinoRestCatalog catalog = createTrinoRestCatalog(false, restSessionCatalog, false, true, NONE, Optional.of(listingCache), Optional.empty());

        String schema = "ns_cache_test_" + randomNameSuffix();
        List<String> tableNames = List.of("table_a", "table_b", "table_c");
        catalog.createNamespace(SESSION, schema, ImmutableMap.of(), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
        try {
            // Create tables directly against the backend so setup doesn't warm Trino-side caches.
            Schema tableSchema = new Schema(Types.NestedField.required(1, "c", Types.IntegerType.get()));
            for (String name : tableNames) {
                backend.buildTable(TableIdentifier.of(schema, name), tableSchema).create();
            }
            listTablesCount.set(0);

            for (String name : tableNames) {
                catalog.loadTable(SESSION, new SchemaTableName(schema, name));
            }

            assertThat(listTablesCount.get())
                    .as("listTables invocations during case-insensitive resolution of %d tables", tableNames.size())
                    .isEqualTo(1);
        }
        finally {
            for (String name : tableNames) {
                try {
                    catalog.dropTable(SESSION, new SchemaTableName(schema, name));
                }
                catch (RuntimeException ignored) {
                }
            }
            catalog.dropNamespace(SESSION, schema);
        }
    }

    @Test
    public void testCaseInsensitiveNamespaceListingCacheRefreshesOnMiss()
            throws IOException
    {
        // A table created out-of-band, for example by Spark, must not stay hidden for the TTL.

        Path warehouseLocation = Files.createTempDirectory(null);
        warehouseLocation.toFile().deleteOnExit();

        String catalogName = "iceberg_rest";
        Catalog backend = backendCatalog(warehouseLocation);
        AtomicInteger listTablesCount = new AtomicInteger();

        RESTSessionCatalog restSessionCatalog = DelegatingRestSessionCatalog.builder()
                .delegate(countingBackend(backend, "listTables", listTablesCount))
                .build();
        restSessionCatalog.initialize(catalogName, ImmutableMap.of());

        Cache<NamespaceListingKey, List<TableIdentifier>> listingCache = createNamespaceListingCache();

        TrinoRestCatalog catalog = createTrinoRestCatalog(false, restSessionCatalog, false, true, NONE, Optional.of(listingCache), Optional.empty());

        String schema = "ns_cache_refresh_test_" + randomNameSuffix();
        catalog.createNamespace(SESSION, schema, ImmutableMap.of(), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
        try {
            Schema tableSchema = new Schema(Types.NestedField.required(1, "c", Types.IntegerType.get()));

            backend.buildTable(TableIdentifier.of(schema, "table_a"), tableSchema).create();
            listTablesCount.set(0);
            catalog.loadTable(SESSION, new SchemaTableName(schema, "table_a"));
            assertThat(listTablesCount.get())
                    .as("listTables calls on initial resolve (cold cache)")
                    .isEqualTo(1);

            // Created while the cache still holds the stale [table_a] listing.
            backend.buildTable(TableIdentifier.of(schema, "table_b"), tableSchema).create();

            catalog.loadTable(SESSION, new SchemaTableName(schema, "table_b"));
            assertThat(listTablesCount.get())
                    .as("listTables calls after miss-triggered refresh picks up the out-of-band table")
                    .isEqualTo(2);
        }
        finally {
            for (String name : List.of("table_a", "table_b")) {
                try {
                    catalog.dropTable(SESSION, new SchemaTableName(schema, name));
                }
                catch (RuntimeException ignored) {
                }
            }
            catalog.dropNamespace(SESSION, schema);
        }
    }

    @Test
    public void testNamespaceListingCacheInvalidationForMixedCaseNamespace()
            throws IOException
    {
        // Entries are keyed by the remote namespace, which keeps the REST server's casing, while DDL
        // arrives with the lowercased Trino name.

        Path warehouseLocation = Files.createTempDirectory(null);
        warehouseLocation.toFile().deleteOnExit();

        String catalogName = "iceberg_rest";
        Catalog backend = backendCatalog(warehouseLocation);
        RESTSessionCatalog restSessionCatalog = DelegatingRestSessionCatalog.builder()
                .delegate(backend)
                .build();
        restSessionCatalog.initialize(catalogName, ImmutableMap.of());

        Cache<NamespaceListingKey, List<TableIdentifier>> listingCache = createNamespaceListingCache();

        TrinoRestCatalog catalog = createTrinoRestCatalog(false, restSessionCatalog, false, true, NONE, Optional.of(listingCache), Optional.empty());

        String remoteSchema = "MixedCaseNs" + randomNameSuffix();
        String trinoSchema = remoteSchema.toLowerCase(ENGLISH);
        catalog.createNamespace(SESSION, remoteSchema, ImmutableMap.of(), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
        try {
            Schema tableSchema = new Schema(Types.NestedField.required(1, "c", Types.IntegerType.get()));
            backend.buildTable(TableIdentifier.of(remoteSchema, "tbl"), tableSchema).create();

            catalog.loadTable(SESSION, new SchemaTableName(trinoSchema, "tbl"));
            assertThat(listingCache.size())
                    .as("listing cache size after resolving a table in a mixed-case remote namespace")
                    .isEqualTo(1);

            catalog.dropTable(SESSION, new SchemaTableName(trinoSchema, "tbl"));
            assertThat(listingCache.size())
                    .as("listing cache size after dropTable in a mixed-case remote namespace")
                    .isEqualTo(0);
        }
        finally {
            catalog.dropNamespace(SESSION, trinoSchema);
        }
    }

    @Test
    public void testCaseInsensitiveNamespaceViewListingCacheReusesListing()
            throws IOException
    {
        // Case-insensitive resolutions in one namespace share a single listViews call.

        Path warehouseLocation = Files.createTempDirectory(null);
        warehouseLocation.toFile().deleteOnExit();

        String catalogName = "iceberg_rest";
        Catalog backend = backendCatalog(warehouseLocation);
        AtomicInteger listViewsCount = new AtomicInteger();

        RESTSessionCatalog restSessionCatalog = DelegatingRestSessionCatalog.builder()
                .delegate(countingBackend(backend, "listViews", listViewsCount))
                .build();
        restSessionCatalog.initialize(catalogName, ImmutableMap.of());

        Cache<NamespaceListingKey, List<TableIdentifier>> viewListingCache = createNamespaceListingCache();

        TrinoRestCatalog catalog = createTrinoRestCatalog(false, restSessionCatalog, false, true, NONE, Optional.empty(), Optional.of(viewListingCache));

        String schema = "ns_view_cache_test_" + randomNameSuffix();
        List<String> viewNames = List.of("view_a", "view_b", "view_c");
        catalog.createNamespace(SESSION, schema, ImmutableMap.of(), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
        try {
            for (String name : viewNames) {
                catalog.createView(
                        SESSION,
                        new SchemaTableName(schema, name),
                        viewDefinition("SELECT 1 AS c", new ViewColumn("c", BIGINT.getTypeId(), Optional.empty())),
                        ImmutableMap.of(),
                        false);
            }
            // createView resolves the target name, which leaves a listing cached.
            viewListingCache.invalidateAll();
            listViewsCount.set(0);

            for (String name : viewNames) {
                assertThat(catalog.getView(SESSION, new SchemaTableName(schema, name))).isPresent();
            }

            assertThat(listViewsCount.get())
                    .as("listViews invocations during case-insensitive resolution of %d views", viewNames.size())
                    .isEqualTo(1);
        }
        finally {
            for (String name : viewNames) {
                try {
                    catalog.dropView(SESSION, new SchemaTableName(schema, name));
                }
                catch (RuntimeException ignored) {
                }
            }
            catalog.dropNamespace(SESSION, schema);
        }
    }

    @Test
    public void testNamespaceListingCacheIsPartitionedPerUser()
            throws IOException
    {
        // Uses the view path because loadTable caches BaseTable per SchemaTableName with no user in the key,
        // which would mask the difference.

        Path warehouseLocation = Files.createTempDirectory(null);
        warehouseLocation.toFile().deleteOnExit();

        String catalogName = "iceberg_rest";
        Catalog backend = backendCatalog(warehouseLocation);
        AtomicInteger listViewsCount = new AtomicInteger();

        RESTSessionCatalog restSessionCatalog = DelegatingRestSessionCatalog.builder()
                .delegate(countingBackend(backend, "listViews", listViewsCount))
                .build();
        restSessionCatalog.initialize(catalogName, ImmutableMap.of());

        Cache<NamespaceListingKey, List<TableIdentifier>> viewListingCache = createNamespaceListingCache();

        TrinoRestCatalog catalog = createTrinoRestCatalog(false, restSessionCatalog, false, true, USER, Optional.empty(), Optional.of(viewListingCache));

        ConnectorSession alice = sessionForUser("alice", ImmutableMap.of());
        ConnectorSession bob = sessionForUser("bob", ImmutableMap.of());
        ConnectorSession aliceOtherToken = sessionForUser("alice", ImmutableMap.of("token", "another"));
        // These two differ only in where the field boundaries fall, and NUL is valid US-ASCII.
        ConnectorSession aliceAmbiguousA = sessionForUser("alice", ImmutableMap.of("a", "b\0c\0d"));
        ConnectorSession aliceAmbiguousB = sessionForUser("alice", ImmutableMap.of("a", "b", "c", "d"));

        String schema = "ns_user_cache_test_" + randomNameSuffix();
        List<String> viewNames = List.of("view_a", "view_b");
        catalog.createNamespace(alice, schema, ImmutableMap.of(), new TrinoPrincipal(PrincipalType.USER, alice.getUser()));
        try {
            for (String name : viewNames) {
                catalog.createView(
                        alice,
                        new SchemaTableName(schema, name),
                        viewDefinition("SELECT 1 AS c", new ViewColumn("c", BIGINT.getTypeId(), Optional.empty())),
                        ImmutableMap.of(),
                        false);
            }
            // createView resolves the target name, which leaves alice's listing cached.
            viewListingCache.invalidateAll();
            listViewsCount.set(0);

            assertThat(catalog.getView(alice, new SchemaTableName(schema, "view_a"))).isPresent();
            assertThat(catalog.getView(bob, new SchemaTableName(schema, "view_a"))).isPresent();
            assertThat(listViewsCount.get())
                    .as("listViews invocations for one view resolved by two users")
                    .isEqualTo(2);

            assertThat(catalog.getView(alice, new SchemaTableName(schema, "view_b"))).isPresent();
            assertThat(listViewsCount.get())
                    .as("listViews invocations after the same user resolves a second view")
                    .isEqualTo(2);

            assertThat(catalog.getView(aliceOtherToken, new SchemaTableName(schema, "view_a"))).isPresent();
            assertThat(listViewsCount.get())
                    .as("listViews invocations for the same user with different extra credentials")
                    .isEqualTo(3);

            assertThat(viewListingCache.size())
                    .as("one listing cache entry per (namespace, user, credentials) tuple")
                    .isEqualTo(3);

            assertThat(catalog.getView(aliceAmbiguousA, new SchemaTableName(schema, "view_a"))).isPresent();
            assertThat(catalog.getView(aliceAmbiguousB, new SchemaTableName(schema, "view_a"))).isPresent();
            assertThat(listViewsCount.get())
                    .as("listViews invocations for credential sets differing only in field boundaries")
                    .isEqualTo(5);
        }
        finally {
            for (String name : viewNames) {
                try {
                    catalog.dropView(alice, new SchemaTableName(schema, name));
                }
                catch (RuntimeException ignored) {
                }
            }
            catalog.dropNamespace(alice, schema);
        }
    }

    private static ConnectorSession sessionForUser(String user, Map<String, String> extraCredentials)
    {
        return TestingConnectorSession.builder()
                .setIdentity(ConnectorIdentity.forUser(user)
                        .withExtraCredentials(extraCredentials)
                        .build())
                .setPropertyMetadata(new IcebergSessionProperties(
                        new IcebergConfig(),
                        new IcebergEncryptionConfig(),
                        new OrcReaderConfig(),
                        new OrcWriterConfig(),
                        new ParquetReaderConfig(),
                        new ParquetWriterConfig())
                        .getSessionProperties())
                .build();
    }

    @Override
    protected TableInfo.ExtendedRelationType getViewType()
    {
        return OTHER_VIEW;
    }

    @Test
    public void testReplaceViewReuseExistingLocation()
            throws IOException
    {
        TrinoRestCatalog catalog = createTrinoRestCatalog(true, ImmutableMap.of());

        String namespace = "test_create_replace_view_" + randomNameSuffix();
        SchemaTableName viewName = new SchemaTableName(namespace, "test_view");
        ConnectorViewDefinition viewDefinition = viewDefinition(
                "SELECT name FROM local.tiny.nation",
                new ViewColumn("name", VARCHAR.getTypeId(), Optional.empty()));

        catalog.createNamespace(SESSION, namespace, defaultNamespaceProperties(namespace), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));

        catalog.createView(SESSION, viewName, viewDefinition, ImmutableMap.of(), false);
        assertViewDefinition(catalog.getView(SESSION, viewName).orElseThrow(), viewDefinition);

        View initialView = catalog.getIcebergView(SESSION, viewName, false).orElse(null);
        assertThat(initialView).isNotNull();
        assertThat(initialView.location()).isNotNull();

        ConnectorViewDefinition updatedViewDefinition = viewDefinition(
                "SELECT regionkey, name, comment FROM local.tiny.region",
                new ViewColumn("regionkey", BIGINT.getTypeId(), Optional.empty()),
                new ViewColumn("name", VARCHAR.getTypeId(), Optional.empty()),
                new ViewColumn("comment", VARCHAR.getTypeId(), Optional.empty()));

        catalog.createView(SESSION, viewName, updatedViewDefinition, ImmutableMap.of(), true);
        assertViewDefinition(catalog.getView(SESSION, viewName).orElseThrow(), updatedViewDefinition);

        View updatedView = catalog.getIcebergView(SESSION, viewName, false).orElse(null);
        assertThat(updatedView).isNotNull();
        assertThat(updatedView.location()).isEqualTo(initialView.location());
        assertThat(updatedView.currentVersion().versionId()).isEqualTo(initialView.currentVersion().versionId() + 1);

        catalog.dropView(SESSION, viewName);
        catalog.dropNamespace(SESSION, namespace);
    }

    private static ConnectorViewDefinition viewDefinition(@Language("SQL") String sql, ViewColumn... columns)
    {
        return new ConnectorViewDefinition(
                sql,
                Optional.empty(),
                Optional.empty(),
                ImmutableList.copyOf(columns),
                Optional.empty(),
                Optional.of(SESSION.getUser()),
                false,
                ImmutableList.of());
    }
}
