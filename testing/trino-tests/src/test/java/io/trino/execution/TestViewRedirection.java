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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.metadata.MetadataManager.MAX_VIEW_REDIRECTIONS;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestViewRedirection
        extends AbstractTestQueryFramework
{
    private static final String CATALOG_NAME = "test_catalog";
    private static final String SCHEMA_ONE = "test_schema_1";
    private static final String SCHEMA_TWO = "test_schema_2";
    private static final String SCHEMA_THREE = "test_schema_3";
    private static final List<String> SCHEMAS = ImmutableList.of(SCHEMA_ONE, SCHEMA_TWO, SCHEMA_THREE);

    // Views
    private static final String VIEW_FOO = "view_foo";
    private static final String VALID_REDIRECTION_SRC = "valid_redirection_src";
    private static final String VALID_REDIRECTION_TARGET = "valid_redirection_target";
    private static final String BAD_REDIRECTION_SRC = "bad_redirection_src";
    private static final String NON_EXISTENT_VIEW = "non_existent_view";
    private static final String REDIRECTION_TWICE_SRC = "redirection_twice_src";
    private static final String INTERMEDIATE_VIEW = "intermediate_view";
    private static final String REDIRECTION_LOOP_PING = "redirection_loop_ping";
    private static final String REDIRECTION_LOOP_PONG = "redirection_loop_pong";
    private static final List<String> REDIRECTION_CHAIN = IntStream.range(0, MAX_VIEW_REDIRECTIONS + 1).boxed()
            .map(i -> "redirection_chain_view_" + i)
            .collect(toImmutableList());

    private static final Map<String, Set<String>> SCHEMA_VIEW_MAPPING = ImmutableMap.of(
            SCHEMA_ONE, ImmutableSet.of(VIEW_FOO, VALID_REDIRECTION_SRC, BAD_REDIRECTION_SRC, REDIRECTION_TWICE_SRC, REDIRECTION_LOOP_PING),
            SCHEMA_TWO, ImmutableSet.of(VALID_REDIRECTION_TARGET, INTERMEDIATE_VIEW, REDIRECTION_LOOP_PONG),
            SCHEMA_THREE, ImmutableSet.copyOf(REDIRECTION_CHAIN));

    private static final Map<SchemaTableName, SchemaTableName> VIEW_REDIRECTIONS = ImmutableMap.<SchemaTableName, SchemaTableName>builder()
            // Redirection to a valid view
            .put(schemaTableName(SCHEMA_ONE, VALID_REDIRECTION_SRC), schemaTableName(SCHEMA_TWO, VALID_REDIRECTION_TARGET))
            // Redirection to a non-existent view
            .put(schemaTableName(SCHEMA_ONE, BAD_REDIRECTION_SRC), schemaTableName(SCHEMA_TWO, NON_EXISTENT_VIEW))
            // Multi-step redirection
            .put(schemaTableName(SCHEMA_ONE, REDIRECTION_TWICE_SRC), schemaTableName(SCHEMA_TWO, INTERMEDIATE_VIEW))
            .put(schemaTableName(SCHEMA_TWO, INTERMEDIATE_VIEW), schemaTableName(SCHEMA_ONE, VIEW_FOO))
            // Redirection loop
            .put(schemaTableName(SCHEMA_ONE, REDIRECTION_LOOP_PING), schemaTableName(SCHEMA_TWO, REDIRECTION_LOOP_PONG))
            .put(schemaTableName(SCHEMA_TWO, REDIRECTION_LOOP_PONG), schemaTableName(SCHEMA_ONE, REDIRECTION_LOOP_PING))
            // Redirection chain exceeding max hops
            .putAll(IntStream.range(0, REDIRECTION_CHAIN.size() - 1)
                    .boxed()
                    .collect(toImmutableMap(
                            i -> schemaTableName(SCHEMA_THREE, REDIRECTION_CHAIN.get(i)),
                            i -> schemaTableName(SCHEMA_THREE, REDIRECTION_CHAIN.get(i + 1)))))
            .buildOrThrow();

    private static final String VIEW_SQL = "SELECT 1 AS col";

    private static final Session TEST_SESSION = testSessionBuilder()
            .setCatalog(CATALOG_NAME)
            .build();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = DistributedQueryRunner.builder(TEST_SESSION).build();
        queryRunner.installPlugin(new MockConnectorPlugin(createMockConnectorFactory()));
        queryRunner.createCatalog(CATALOG_NAME, "mock", ImmutableMap.of());
        return queryRunner;
    }

    private MockConnectorFactory createMockConnectorFactory()
    {
        return MockConnectorFactory.builder()
                .withListSchemaNames(_ -> SCHEMAS)
                .withGetViews((_, prefix) -> {
                    Map<SchemaTableName, ConnectorViewDefinition> allViews = SCHEMA_VIEW_MAPPING.entrySet().stream()
                            .flatMap(entry -> entry.getValue().stream()
                                    .filter(viewName -> !VIEW_REDIRECTIONS.containsKey(schemaTableName(entry.getKey(), viewName)))
                                    .map(viewName -> schemaTableName(entry.getKey(), viewName)))
                            .collect(toImmutableMap(
                                    name -> name,
                                    name -> createViewDefinition(name)));

                    return filterByPrefix(allViews, prefix);
                })
                .withRedirectView((_, viewName) ->
                        Optional.ofNullable(VIEW_REDIRECTIONS.get(viewName))
                                .map(target -> new CatalogSchemaTableName(CATALOG_NAME, target)))
                .build();
    }

    private static ConnectorViewDefinition createViewDefinition(SchemaTableName viewName)
    {
        return new ConnectorViewDefinition(
                VIEW_SQL,
                Optional.of(CATALOG_NAME),
                Optional.of(viewName.getSchemaName()),
                ImmutableList.of(new ConnectorViewDefinition.ViewColumn("col", BIGINT.getTypeId(), Optional.empty())),
                Optional.empty(),
                Optional.empty(),
                true,
                ImmutableList.of());
    }

    private static Map<SchemaTableName, ConnectorViewDefinition> filterByPrefix(
            Map<SchemaTableName, ConnectorViewDefinition> views,
            SchemaTablePrefix prefix)
    {
        return views.entrySet().stream()
                .filter(entry -> prefix.getSchema().map(s -> s.equals(entry.getKey().getSchemaName())).orElse(true))
                .filter(entry -> prefix.getTable().map(t -> t.equals(entry.getKey().getTableName())).orElse(true))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Test
    public void testValidViewRedirection()
    {
        // SELECT through a redirected view should resolve to the target view
        assertThat(query(format("SELECT * FROM %s.%s.%s", CATALOG_NAME, SCHEMA_ONE, VALID_REDIRECTION_SRC)))
                .matches("VALUES BIGINT '1'");
    }

    @Test
    public void testShowCreateViewWithRedirection()
    {
        // SHOW CREATE VIEW on redirected source should show the target view's definition
        String showCreateTarget = (String) computeScalar(format("SHOW CREATE VIEW %s.%s.%s", CATALOG_NAME, SCHEMA_TWO, VALID_REDIRECTION_TARGET));
        String showCreateSource = (String) computeScalar(format("SHOW CREATE VIEW %s.%s.%s", CATALOG_NAME, SCHEMA_ONE, VALID_REDIRECTION_SRC));
        assertThat(showCreateTarget).isEqualTo(showCreateSource.replace(
                SCHEMA_ONE + "." + VALID_REDIRECTION_SRC,
                SCHEMA_TWO + "." + VALID_REDIRECTION_TARGET));
    }

    @Test
    public void testMultiHopViewRedirection()
    {
        // Two hops: REDIRECTION_TWICE_SRC -> INTERMEDIATE_VIEW -> VIEW_FOO
        assertThat(query(format("SELECT * FROM %s.%s.%s", CATALOG_NAME, SCHEMA_ONE, REDIRECTION_TWICE_SRC)))
                .matches("VALUES BIGINT '1'");
    }

    @Test
    public void testViewRedirectionToNonExistentView()
    {
        // Redirect to a view that doesn't exist in the target — falls through to table resolution.
        // Mock connector provides a default table handle, so the query succeeds with empty results.
        assertThat(query(format("SELECT * FROM %s.%s.%s", CATALOG_NAME, SCHEMA_ONE, BAD_REDIRECTION_SRC)))
                .returnsEmptyResult();
    }

    @Test
    public void testViewRedirectionLoop()
    {
        // Loop: REDIRECTION_LOOP_PING -> REDIRECTION_LOOP_PONG -> REDIRECTION_LOOP_PING
        assertThat(query(format("SELECT * FROM %s.%s.%s", CATALOG_NAME, SCHEMA_ONE, REDIRECTION_LOOP_PING)))
                .failure()
                .hasMessageContaining("View redirections form a loop");
    }

    @Test
    public void testViewRedirectionMaxHopsExceeded()
    {
        // Chain of MAX_VIEW_REDIRECTIONS + 1 views exceeds the limit
        assertThat(query(format("SELECT * FROM %s.%s.%s", CATALOG_NAME, SCHEMA_THREE, REDIRECTION_CHAIN.get(0))))
                .failure()
                .hasMessageContaining(format("View redirected too many times (%d)", MAX_VIEW_REDIRECTIONS));
    }

    @Test
    public void testNoRedirectionForDirectView()
    {
        // VIEW_FOO is not in the redirection map — should resolve directly
        assertThat(query(format("SELECT * FROM %s.%s.%s", CATALOG_NAME, SCHEMA_ONE, VIEW_FOO)))
                .matches("VALUES BIGINT '1'");
    }

    @Test
    public void testShowCreateViewWithLoop()
    {
        // SHOW CREATE VIEW should also detect loops
        assertThat(query(format("SHOW CREATE VIEW %s.%s.%s", CATALOG_NAME, SCHEMA_ONE, REDIRECTION_LOOP_PING)))
                .failure()
                .hasMessageContaining("View redirections form a loop");
    }

    @Test
    public void testShowCreateViewWithMaxHopsExceeded()
    {
        // SHOW CREATE VIEW should also detect max hops exceeded
        assertThat(query(format("SHOW CREATE VIEW %s.%s.%s", CATALOG_NAME, SCHEMA_THREE, REDIRECTION_CHAIN.get(0))))
                .failure()
                .hasMessageContaining(format("View redirected too many times (%d)", MAX_VIEW_REDIRECTIONS));
    }

    @Test
    public void testDropViewWithRedirection()
    {
        // DROP VIEW on a redirected view — should resolve and attempt to drop the target
        // This will fail because mock connector's dropView is a no-op and we can't verify state,
        // but we verify it doesn't throw a redirection error
        assertQuerySucceeds(format("DROP VIEW IF EXISTS %s.%s.%s", CATALOG_NAME, SCHEMA_ONE, VALID_REDIRECTION_SRC));
    }

    @Test
    public void testDropViewWithLoop()
    {
        assertThat(query(format("DROP VIEW %s.%s.%s", CATALOG_NAME, SCHEMA_ONE, REDIRECTION_LOOP_PING)))
                .failure()
                .hasMessageContaining("View redirections form a loop");
    }

    @Test
    public void testCreateOrReplaceViewWithRedirection()
    {
        // CREATE OR REPLACE VIEW on a redirected name should resolve to the target
        assertQuerySucceeds(format(
                "CREATE OR REPLACE VIEW %s.%s.%s AS SELECT 2 AS col",
                CATALOG_NAME,
                SCHEMA_ONE,
                VALID_REDIRECTION_SRC));
    }

    @Test
    public void testCreateOrReplaceViewWithLoop()
    {
        assertThat(query(format(
                "CREATE OR REPLACE VIEW %s.%s.%s AS SELECT 2 AS col",
                CATALOG_NAME,
                SCHEMA_ONE,
                REDIRECTION_LOOP_PING)))
                .failure()
                .hasMessageContaining("View redirections form a loop");
    }

    @Test
    public void testRenameViewWithLoop()
    {
        assertThat(query(format(
                "ALTER VIEW %s.%s.%s RENAME TO %s.%s.renamed_view",
                CATALOG_NAME,
                SCHEMA_ONE,
                REDIRECTION_LOOP_PING,
                CATALOG_NAME,
                SCHEMA_ONE)))
                .failure()
                .hasMessageContaining("View redirections form a loop");
    }

    @Test
    public void testCommentOnViewWithRedirection()
    {
        // COMMENT ON VIEW with a redirected view name should resolve to the target
        assertQuerySucceeds(format(
                "COMMENT ON VIEW %s.%s.%s IS 'test comment'",
                CATALOG_NAME,
                SCHEMA_ONE,
                VALID_REDIRECTION_SRC));
    }

    @Test
    public void testCommentOnViewWithLoop()
    {
        assertThat(query(format(
                "COMMENT ON VIEW %s.%s.%s IS 'test comment'",
                CATALOG_NAME,
                SCHEMA_ONE,
                REDIRECTION_LOOP_PING)))
                .failure()
                .hasMessageContaining("View redirections form a loop");
    }

    @Test
    public void testSelectFromNonExistentView()
    {
        // A view that doesn't exist and has no redirection — falls through to table resolution.
        // Mock connector provides a default table handle, so the query succeeds with empty results.
        assertThat(query(format("SELECT * FROM %s.%s.totally_fake_view", CATALOG_NAME, SCHEMA_ONE)))
                .returnsEmptyResult();
    }
}
