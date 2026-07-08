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

import com.google.common.collect.ImmutableList;
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
import io.trino.spi.connector.ConnectorExpressionEvaluator;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.ConnectorViewDefinition.ViewColumn;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.rest.DelegatingRestSessionCatalog;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.view.View;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.io.IOException;
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
import static io.trino.plugin.iceberg.catalog.rest.RestCatalogTestUtils.backendCatalog;
import static io.trino.plugin.iceberg.delete.DeletionVectorWriter.UNSUPPORTED_DELETION_VECTOR_WRITER;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
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
        String catalogName = "iceberg_rest";
        return new TrinoRestCatalog(
                new DefaultIcebergFileSystemFactory(HDFS_FILE_SYSTEM_FACTORY),
                restSessionCatalog,
                new CatalogName(catalogName),
                Security.NONE,
                NONE,
                ImmutableMap.of(),
                nestedNamespaceEnabled,
                "test",
                TESTING_TYPE_MANAGER,
                useUniqueTableLocations,
                caseInsensitiveNameMatching,
                EvictableCacheBuilder.newBuilder().expireAfterWrite(1000, MILLISECONDS).shareNothingWhenDisabled().build(),
                EvictableCacheBuilder.newBuilder().expireAfterWrite(1000, MILLISECONDS).shareNothingWhenDisabled().build(),
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

    @Test
    public void testSchemaExistsExactMatchAvoidsListing()
    {
        AtomicInteger listNamespacesCalls = new AtomicInteger(0);

        RESTSessionCatalog restSessionCatalog = new RESTSessionCatalog()
        {
            @Override
            public boolean namespaceExists(SessionContext context, Namespace namespace)
            {
                // Simulate that the exact path exists on the remote catalog
                return namespace.equals(Namespace.of("My_Db", "My_Schema"));
            }

            @Override
            public List<Namespace> listNamespaces(SessionContext context, Namespace namespace)
            {
                listNamespacesCalls.incrementAndGet();
                return List.of();
            }
        };

        // caseInsensitiveNameMatching = true
        TrinoCatalog catalog = createTrinoRestCatalog(false, restSessionCatalog, true, false);

        boolean exists = catalog.namespaceExists(SESSION, "My_Db.My_Schema");

        assertThat(exists).isTrue();
        // PROOF: listNamespaces was never called because optimistic check succeeded
        assertThat(listNamespacesCalls.get()).isEqualTo(0);
    }

    @Test
    public void testNamespaceExistsCaseInsensitiveParentLookup()
    {
        // Use Maps to record the exact requested path and its call frequency
        Map<String, Integer> namespaceExistsCalls = new ConcurrentHashMap<>();
        Map<String, Integer> listNamespacesCalls = new ConcurrentHashMap<>();

        // The absolute truth of the case-sensitive remote catalog
        Namespace trueRoot = Namespace.of("My_Db");
        Namespace trueParent = Namespace.of("My_Db", "My_Schema");
        Namespace trueChild = Namespace.of("My_Db", "My_Schema", "My_Subschema");

        RESTSessionCatalog restSessionCatalog = new RESTSessionCatalog()
        {
            @Override
            public boolean namespaceExists(SessionContext context, Namespace namespace)
            {
                // Record the exact string payload that Trino requested
                String path = String.join(".", namespace.levels());
                namespaceExistsCalls.merge(path.toLowerCase(ENGLISH), 1, Integer::sum);

                // Strict Case-Sensitive Server logic
                return namespace.equals(trueRoot) ||
                        namespace.equals(trueParent) ||
                        namespace.equals(trueChild);
            }

            @Override
            public List<Namespace> listNamespaces(SessionContext context, Namespace namespace)
            {
                // Record the exact string payload requested for listing
                String path = String.join(".", namespace.levels());
                listNamespacesCalls.merge(path.toLowerCase(ENGLISH), 1, Integer::sum);

                if (namespace.isEmpty()) {
                    return List.of(trueRoot);
                }
                if (namespace.equals(trueRoot)) {
                    return List.of(trueParent);
                }
                if (namespace.equals(trueParent)) {
                    return List.of(trueChild);
                }
                return List.of();
            }
        };

        TrinoCatalog catalog = createTrinoRestCatalog(false, restSessionCatalog, true, true);

        // Input completely mangled casing.
        boolean exists = catalog.namespaceExists(SESSION, "mY_Db.my_SCHema.my_SUBschEma");

        // 1. Resolution must succeed
        assertThat(exists).isTrue();

        // 2. ASSERT EXACT LISTING TRAVERSAL
        // Because optimistic checks failed, it had to walk the tree using the true casing at each step.
        assertThat(listNamespacesCalls)
                .as("Must list the root, then the verified DB, then the verified schema")
                .hasSize(3)
                .containsEntry("", 1)                  // Listed Root
                .containsEntry("my_db", 1)             // Listed Database
                .containsEntry("my_db.my_schema", 1);  // Listed Schema

        // 3. ASSERT EXACT EXISTENCE CHECKS (Translation + Execution phases)
        assertThat(namespaceExistsCalls)
                .as("Must attempt optimistic translation lookups, then one final execution check")
                .hasSize(3)
                // PHASE 1: TRANSLATION (The Optimistic failures)
                .containsEntry("my_db.my_schema.my_subschema", 2)
                .containsEntry("my_db.my_schema", 1)
                .containsEntry("my_db", 1);
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
