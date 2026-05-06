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
import com.google.common.collect.ImmutableMap;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.metastore.TableInfo;
import io.trino.plugin.iceberg.CommitTaskData;
import io.trino.plugin.iceberg.DefaultIcebergFileSystemFactory;
import io.trino.plugin.iceberg.IcebergMetadata;
import io.trino.plugin.iceberg.TableStatisticsWriter;
import io.trino.plugin.iceberg.catalog.BaseTrinoCatalogTest;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogConfig.Security;
import io.trino.spi.NodeVersion;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.rest.DelegatingRestSessionCatalog;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import static io.trino.plugin.iceberg.catalog.rest.RestCatalogTestUtils.backendCatalog;
import static io.trino.plugin.iceberg.delete.DeletionVectorWriter.UNSUPPORTED_DELETION_VECTOR_WRITER;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        warehouseLocation.toFile().deleteOnExit();

        String catalogName = "iceberg_rest";
        RESTSessionCatalog restSessionCatalog = DelegatingRestSessionCatalog
                .builder()
                .delegate(backendCatalog(warehouseLocation))
                .build();

        restSessionCatalog.initialize(catalogName, properties);

        return new TrinoRestCatalog(
                new DefaultIcebergFileSystemFactory(HDFS_FILE_SYSTEM_FACTORY),
                restSessionCatalog,
                new CatalogName(catalogName),
                Security.NONE,
                NONE,
                ImmutableMap.of(),
                false,
                "test",
                TESTING_TYPE_MANAGER,
                useUniqueTableLocations,
                false,
                EvictableCacheBuilder.newBuilder().expireAfterWrite(1000, MILLISECONDS).shareNothingWhenDisabled().build(),
                EvictableCacheBuilder.newBuilder().expireAfterWrite(1000, MILLISECONDS).shareNothingWhenDisabled().build(),
                Optional.empty(),
                Optional.empty(),
                true);
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
                    ZERO);
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
    public void testCaseInsensitiveNamespaceListingCacheReusesListing()
            throws IOException
    {
        // Resolving a case-insensitive name normally goes: findRemoteTable -> listTables(namespace) -> scan for equalsIgnoreCase match.
        // With the namespace listing cache enabled, multiple resolutions against the same namespace should share a single listTables call.

        Path warehouseLocation = Files.createTempDirectory(null);
        warehouseLocation.toFile().deleteOnExit();

        String catalogName = "iceberg_rest";
        // Delegating proxy counts listTables(Namespace) calls hitting the backend so we can assert on actual REST-layer traffic.
        Catalog backend = backendCatalog(warehouseLocation);
        AtomicInteger listTablesCount = new AtomicInteger();
        Catalog countingBackend = (Catalog) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {Catalog.class, SupportsNamespaces.class, ViewCatalog.class, Closeable.class},
                (_, method, args) -> {
                    if (method.getName().equals("listTables") && args != null && args.length == 1) {
                        listTablesCount.incrementAndGet();
                    }
                    return method.invoke(backend, args);
                });

        RESTSessionCatalog restSessionCatalog = DelegatingRestSessionCatalog.builder()
                .delegate(countingBackend)
                .build();
        restSessionCatalog.initialize(catalogName, ImmutableMap.of());

        // Real, weight-based listing cache matching the production configuration shape.
        Cache<NamespaceListingKey, List<TableIdentifier>> listingCache = EvictableCacheBuilder.newBuilder()
                .expireAfterWrite(1, MINUTES)
                .maximumWeight(10_000)
                .<NamespaceListingKey, List<TableIdentifier>>weigher((_, identifiers) -> identifiers.size())
                .shareNothingWhenDisabled()
                .build();

        TrinoRestCatalog catalog = new TrinoRestCatalog(
                new DefaultIcebergFileSystemFactory(HDFS_FILE_SYSTEM_FACTORY),
                restSessionCatalog,
                new CatalogName(catalogName),
                Security.NONE,
                NONE,
                ImmutableMap.of(),
                false,
                "test",
                TESTING_TYPE_MANAGER,
                false,
                true, // caseInsensitiveNameMatching enabled so findRemoteTable is exercised
                EvictableCacheBuilder.newBuilder().expireAfterWrite(1, MINUTES).shareNothingWhenDisabled().build(),
                EvictableCacheBuilder.newBuilder().expireAfterWrite(1, MINUTES).shareNothingWhenDisabled().build(),
                Optional.of(listingCache),
                Optional.empty(), // no view listing cache - this test only asserts on the table path
                true);

        String schema = "ns_cache_test_" + randomNameSuffix();
        List<String> tableNames = List.of("table_a", "table_b", "table_c");
        catalog.createNamespace(SESSION, schema, ImmutableMap.of(), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
        try {
            // Create tables directly against the backend so setup doesn't warm Trino-side caches.
            Schema tableSchema = new Schema(Types.NestedField.required(1, "c", Types.IntegerType.get()));
            for (String name : tableNames) {
                backend.buildTable(TableIdentifier.of(schema, name), tableSchema).create();
            }
            // Reset the counter so only the assertions below are measured.
            listTablesCount.set(0);

            // Resolve three distinct tables. Without the listing cache each would trigger its own listTables.
            for (String name : tableNames) {
                catalog.loadTable(SESSION, new SchemaTableName(schema, name));
            }

            // One listTables call serves all three resolutions within the TTL window.
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
        // A cached listing can become stale if a table is created out-of-band (e.g. by Spark) after the cache was populated.
        // On a cache miss (no case-insensitive match in the cached listing), the implementation must invalidate and refetch
        // once before declaring the table missing, so the out-of-band table becomes visible immediately on next lookup.

        Path warehouseLocation = Files.createTempDirectory(null);
        warehouseLocation.toFile().deleteOnExit();

        String catalogName = "iceberg_rest";
        Catalog backend = backendCatalog(warehouseLocation);
        AtomicInteger listTablesCount = new AtomicInteger();
        Catalog countingBackend = (Catalog) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {Catalog.class, SupportsNamespaces.class, ViewCatalog.class, Closeable.class},
                (_, method, args) -> {
                    if (method.getName().equals("listTables") && args != null && args.length == 1) {
                        listTablesCount.incrementAndGet();
                    }
                    return method.invoke(backend, args);
                });

        RESTSessionCatalog restSessionCatalog = DelegatingRestSessionCatalog.builder()
                .delegate(countingBackend)
                .build();
        restSessionCatalog.initialize(catalogName, ImmutableMap.of());

        Cache<NamespaceListingKey, List<TableIdentifier>> listingCache = EvictableCacheBuilder.newBuilder()
                .expireAfterWrite(1, MINUTES)
                .maximumWeight(10_000)
                .<NamespaceListingKey, List<TableIdentifier>>weigher((_, identifiers) -> identifiers.size())
                .shareNothingWhenDisabled()
                .build();

        TrinoRestCatalog catalog = new TrinoRestCatalog(
                new DefaultIcebergFileSystemFactory(HDFS_FILE_SYSTEM_FACTORY),
                restSessionCatalog,
                new CatalogName(catalogName),
                Security.NONE,
                NONE,
                ImmutableMap.of(),
                false,
                "test",
                TESTING_TYPE_MANAGER,
                false,
                true,
                EvictableCacheBuilder.newBuilder().expireAfterWrite(1, MINUTES).shareNothingWhenDisabled().build(),
                EvictableCacheBuilder.newBuilder().expireAfterWrite(1, MINUTES).shareNothingWhenDisabled().build(),
                Optional.of(listingCache),
                Optional.empty(),
                true);

        String schema = "ns_cache_refresh_test_" + randomNameSuffix();
        catalog.createNamespace(SESSION, schema, ImmutableMap.of(), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
        try {
            Schema tableSchema = new Schema(Types.NestedField.required(1, "c", Types.IntegerType.get()));

            // Create first table and load it via Trino - populates the cache with [table_a].
            backend.buildTable(TableIdentifier.of(schema, "table_a"), tableSchema).create();
            listTablesCount.set(0);
            catalog.loadTable(SESSION, new SchemaTableName(schema, "table_a"));
            assertThat(listTablesCount.get())
                    .as("listTables calls on initial resolve (cold cache)")
                    .isEqualTo(1);

            // Create a second table out-of-band while the cache still holds the stale [table_a] listing.
            backend.buildTable(TableIdentifier.of(schema, "table_b"), tableSchema).create();

            // Resolving table_b must find it despite the stale cache - the miss should trigger a refresh.
            catalog.loadTable(SESSION, new SchemaTableName(schema, "table_b"));
            assertThat(listTablesCount.get())
                    .as("listTables calls after miss-triggered refresh picks up the out-of-band table")
                    .isEqualTo(2);

            // The refresh also repopulated the cache, so table_b lookups are fast on subsequent calls.
            catalog.loadTable(SESSION, new SchemaTableName(schema, "table_b"));
            catalog.loadTable(SESSION, new SchemaTableName(schema, "table_a"));
            assertThat(listTablesCount.get())
                    .as("subsequent lookups for tables now in the refreshed cache do not trigger more listTables")
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

    @Override
    protected TableInfo.ExtendedRelationType getViewType()
    {
        return OTHER_VIEW;
    }
}
