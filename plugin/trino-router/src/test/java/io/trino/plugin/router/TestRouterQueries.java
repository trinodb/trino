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
package io.trino.plugin.router;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.io.File;

import static com.google.common.base.Verify.verify;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * End-to-end coverage for the {@code router} connector: exercises real SQL statements through
 * {@code router.<prefix><schema>.<table>} and confirms they land on the physical target catalog
 * resolved by {@link RouterMetadata#redirectTable}. {@link TestRouterMetadata} covers the redirect
 * logic itself against a stubbed Glue client; this class proves the engine actually honors (or, for
 * object-creation statements, does not yet honor) that redirect.
 *
 * <p>Trino only consults {@code redirectTable()} when resolving a table/view/materialized view that
 * already exists (see {@code MetadataManager.getRedirectedTableName}/{@code getViewInternal}/
 * {@code getMaterializedViewInternal}). Statements that create a brand-new object bind directly to
 * the catalog named in the SQL, so {@code CREATE TABLE}/{@code CREATE VIEW}/
 * {@code CREATE MATERIALIZED VIEW} through {@code router.*} fail today with {@code NOT_SUPPORTED},
 * since {@link RouterMetadata} implements no creation methods. Both behaviors are asserted below so
 * a future change to either is a deliberate, reviewed diff rather than a silent regression.
 */
@Execution(SAME_THREAD) // shares one target catalog/schema across tests
public class TestRouterQueries
        extends AbstractTestQueryFramework
{
    private static final String ROUTER_CATALOG = "router";
    private static final String TARGET_CATALOG = "target";
    private static final String PREFIX = "redirect_";
    private static final String TARGET_SCHEMA = "sales";

    private static String routerTable(String table)
    {
        return "%s.%s%s.%s".formatted(ROUTER_CATALOG, PREFIX, TARGET_SCHEMA, table);
    }

    private static String targetTable(String table)
    {
        return "%s.%s.%s".formatted(TARGET_CATALOG, TARGET_SCHEMA, table);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder().setCatalog(TARGET_CATALOG).setSchema(TARGET_SCHEMA).build())
                .build();

        File dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve(getClass().getSimpleName()).toFile();
        verify(dataDirectory.mkdirs());

        queryRunner.installPlugin(new IcebergPlugin());
        queryRunner.createCatalog(
                TARGET_CATALOG,
                "iceberg",
                ImmutableMap.of(
                        "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                        "hive.metastore.catalog.dir", dataDirectory.getPath(),
                        "fs.hadoop.enabled", "true"));

        // No Glue call is made by any statement below: they all resolve through redirectTable(),
        // which is pure schema-prefix string matching. router.glue.region is set only so building
        // the (otherwise unused) GlueClient doesn't fail trying to resolve a default region.
        queryRunner.installPlugin(new RouterPlugin());
        queryRunner.createCatalog(
                ROUTER_CATALOG,
                "router",
                ImmutableMap.of(
                        "router.schema-prefix-rules", PREFIX + "=" + TARGET_CATALOG,
                        "router.glue.region", "us-east-1"));

        queryRunner.execute("CREATE SCHEMA " + TARGET_CATALOG + "." + TARGET_SCHEMA);

        return queryRunner;
    }

    @Test
    public void testInsertSelectThroughRedirect()
    {
        String table = "orders_" + randomNameSuffix();
        getQueryRunner().execute("CREATE TABLE " + targetTable(table) + " (id INT, total DOUBLE)");

        getQueryRunner().execute("INSERT INTO " + routerTable(table) + " VALUES (1, 10.5), (2, 20.0)");

        assertThat(getQueryRunner().execute("TABLE " + routerTable(table)))
                .containsAll(getQueryRunner().execute("TABLE " + targetTable(table)));
    }

    @Test
    public void testUpdateThroughRedirect()
    {
        String table = "orders_" + randomNameSuffix();
        getQueryRunner().execute("CREATE TABLE " + targetTable(table) + " (id INT, total DOUBLE)");
        getQueryRunner().execute("INSERT INTO " + targetTable(table) + " VALUES (1, 10.5), (2, 20.0)");

        getQueryRunner().execute("UPDATE " + routerTable(table) + " SET total = 99.9 WHERE id = 1");

        assertThat(getQueryRunner().execute("SELECT total FROM " + targetTable(table) + " WHERE id = 1").getOnlyValue())
                .isEqualTo(99.9);
    }

    @Test
    public void testDeleteThroughRedirect()
    {
        String table = "orders_" + randomNameSuffix();
        getQueryRunner().execute("CREATE TABLE " + targetTable(table) + " (id INT)");
        getQueryRunner().execute("INSERT INTO " + targetTable(table) + " VALUES (1), (2), (3)");

        getQueryRunner().execute("DELETE FROM " + routerTable(table) + " WHERE id = 2");

        assertThat(getQueryRunner().execute("SELECT id FROM " + targetTable(table)).getOnlyColumnAsSet())
                .containsExactlyInAnyOrder(1, 3);
    }

    @Test
    public void testAlterTableAddColumnThroughRedirect()
    {
        String table = "orders_" + randomNameSuffix();
        getQueryRunner().execute("CREATE TABLE " + targetTable(table) + " (id INT)");

        getQueryRunner().execute("ALTER TABLE " + routerTable(table) + " ADD COLUMN total DOUBLE");

        assertThat(getQueryRunner().execute("SELECT column_name FROM " + TARGET_CATALOG + ".information_schema.columns WHERE table_schema = '" + TARGET_SCHEMA + "' AND table_name = '" + table + "'")
                .getOnlyColumnAsSet())
                .contains("total");
    }

    @Test
    public void testAlterTableExecuteOptimizeThroughRedirect()
    {
        String table = "orders_" + randomNameSuffix();
        getQueryRunner().execute("CREATE TABLE " + targetTable(table) + " (id INT)");
        getQueryRunner().execute("INSERT INTO " + targetTable(table) + " VALUES (1)");
        getQueryRunner().execute("INSERT INTO " + targetTable(table) + " VALUES (2)");

        // Table procedure invocation through the redirected name must not fail; the physical
        // effect (file compaction) is already covered by Iceberg's own optimize tests, this only
        // proves the procedure call resolves against the redirect target.
        getQueryRunner().execute("ALTER TABLE " + routerTable(table) + " EXECUTE optimize");

        assertThat(getQueryRunner().execute("SELECT id FROM " + targetTable(table)).getOnlyColumnAsSet())
                .containsExactlyInAnyOrder(1, 2);
    }

    @Test
    public void testDropTableThroughRedirect()
    {
        String table = "orders_" + randomNameSuffix();
        getQueryRunner().execute("CREATE TABLE " + targetTable(table) + " (id INT)");

        getQueryRunner().execute("DROP TABLE " + routerTable(table));

        assertThat(getQueryRunner().tableExists(getSession(), targetTable(table))).isFalse();
    }

    @Test
    public void testCreateTableThroughRedirectIsNotSupported()
    {
        // CREATE TABLE has no existing object to redirect from, so it binds directly to the
        // router catalog's own ConnectorMetadata, which implements no creation methods.
        assertThatThrownByRouter(
                "CREATE TABLE " + routerTable("orders_" + randomNameSuffix()) + " (id INT)",
                "This connector does not support creating tables");
    }

    @Test
    public void testSelectViewThroughRedirect()
    {
        String table = "orders_" + randomNameSuffix();
        String view = "orders_view_" + randomNameSuffix();
        getQueryRunner().execute("CREATE TABLE " + targetTable(table) + " (id INT)");
        getQueryRunner().execute("INSERT INTO " + targetTable(table) + " VALUES (1), (2)");
        // Created directly on the target: CREATE VIEW binds by catalog name, same as CREATE TABLE.
        getQueryRunner().execute("CREATE VIEW " + targetTable(view) + " AS SELECT * FROM " + targetTable(table));

        // But reading it through the router-qualified name must follow the redirect (this is what
        // the "follow catalog redirects for views and materialized views" fix added).
        assertThat(getQueryRunner().execute("SELECT id FROM " + routerTable(view)).getOnlyColumnAsSet())
                .containsExactlyInAnyOrder(1, 2);
    }

    @Test
    public void testCreateViewThroughRedirectIsNotSupported()
    {
        assertThatThrownByRouter(
                "CREATE VIEW " + routerTable("orders_view_" + randomNameSuffix()) + " AS SELECT 1 x",
                "This connector does not support creating views");
    }

    @Test
    public void testDropViewThroughRedirectIsNotSupported()
    {
        String view = "orders_view_" + randomNameSuffix();
        getQueryRunner().execute("CREATE VIEW " + targetTable(view) + " AS SELECT 1 x");

        // Unlike SELECT, DropViewTask resolves the view directly against the named catalog
        // (metadata.getView on "router" itself, unredirected) before ever calling dropView(), so
        // this fails one step earlier than CREATE VIEW's NOT_SUPPORTED: the router simply reports
        // no such view, since RouterMetadata never claims ownership of the redirected object.
        assertThatThrownByRouter("DROP VIEW " + routerTable(view), "does not exist");

        getQueryRunner().execute("DROP VIEW " + targetTable(view));
    }

    @Test
    public void testSelectMaterializedViewThroughRedirectFailsOnStorageTableResolution()
    {
        String table = "orders_" + randomNameSuffix();
        String mv = "orders_mv_" + randomNameSuffix();
        getQueryRunner().execute("CREATE TABLE " + targetTable(table) + " (id INT)");
        getQueryRunner().execute("INSERT INTO " + targetTable(table) + " VALUES (1), (2)");
        getQueryRunner().execute("CREATE MATERIALIZED VIEW " + targetTable(mv) + " AS SELECT * FROM " + targetTable(table));
        getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + targetTable(mv));

        // Verified gap, not a spec: getMaterializedView() itself follows the redirect fine (it finds
        // the MV on "target"), but once it's found fresh, StatementAnalyzer re-qualifies the MV's
        // storage table using the ORIGINAL router-qualified name (catalog "router", schema
        // "redirect_sales") instead of the redirected one, so checkStorageTableNotRedirected() looks
        // for a table that only exists under the router's own (non-existent, prefix-rule-only)
        // "redirect_sales" schema and fails. Selecting a materialized view through a redirected
        // schema does not work today -- this pins that down so a future fix is a deliberate, tested
        // diff rather than a silent behavior change.
        assertThatThrownByRouter("SELECT id FROM " + routerTable(mv), "does not exist");
    }

    @Test
    public void testCreateMaterializedViewThroughRedirectIsNotSupported()
    {
        assertThatThrownByRouter(
                "CREATE MATERIALIZED VIEW " + routerTable("orders_mv_" + randomNameSuffix()) + " AS SELECT 1 x",
                "This connector does not support creating materialized views");
    }

    @Test
    public void testMetadataTableThroughRedirect()
    {
        String table = "orders_" + randomNameSuffix();
        getQueryRunner().execute("CREATE TABLE " + targetTable(table) + " (id INT)");
        getQueryRunner().execute("INSERT INTO " + targetTable(table) + " VALUES (1)");
        getQueryRunner().execute("INSERT INTO " + targetTable(table) + " VALUES (2)");

        // Iceberg's "$snapshots" metadata table, reached through the router's redirected schema,
        // must see the same snapshot history as querying the physical target catalog directly.
        assertThat(getQueryRunner().execute("SELECT snapshot_id FROM " + ROUTER_CATALOG + "." + PREFIX + TARGET_SCHEMA + ".\"" + table + "$snapshots\"").getOnlyColumnAsSet())
                .isEqualTo(getQueryRunner().execute("SELECT snapshot_id FROM " + TARGET_CATALOG + "." + TARGET_SCHEMA + ".\"" + table + "$snapshots\"").getOnlyColumnAsSet());
    }

    private void assertThatThrownByRouter(String sql, String expectedMessageContains)
    {
        assertThatThrownBy(() -> getQueryRunner().execute(sql))
                .hasMessageContaining(expectedMessageContains);
    }
}
