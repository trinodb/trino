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
package io.trino.plugin.hudi;

import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.plugin.hudi.testing.TpchHudiTablesInitializer;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestView;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.plugin.hudi.HudiQueryRunner.HUDI_CATALOG_NAME;
import static io.trino.plugin.hudi.testing.HudiTestUtils.COLUMNS_TO_HIDE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ADD_COLUMN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_REFRESH_VIEW;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHudiConnectorTest
        extends BaseConnectorTest
{
    protected HiveMetastore metastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = HudiQueryRunner.builder()
                .addConnectorProperty("hudi.columns-to-hide", COLUMNS_TO_HIDE)
                .setDataLoader(new TpchHudiTablesInitializer(REQUIRED_TPCH_TABLES))
                .build();
        metastore = ((HudiConnector) queryRunner.getCoordinator().getConnector(HUDI_CATALOG_NAME))
                .getInjector().getInstance(HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());
        return queryRunner;
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_ADD_COLUMN,
                 SUPPORTS_COMMENT_ON_COLUMN,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_CREATE_TABLE,
                 SUPPORTS_DELETE,
                 SUPPORTS_DEREFERENCE_PUSHDOWN,
                 SUPPORTS_INSERT,
                 SUPPORTS_LIMIT_PUSHDOWN,
                 SUPPORTS_MERGE,
                 SUPPORTS_RENAME_COLUMN,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_TOPN_PUSHDOWN,
                 SUPPORTS_UPDATE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        String schema = getSession().getSchema().orElseThrow();
        assertThat((String) computeScalar("SHOW CREATE TABLE orders"))
                .matches("\\QCREATE TABLE hudi." + schema + ".orders (\n" +
                        "   orderkey bigint,\n" +
                        "   custkey bigint,\n" +
                        "   orderstatus varchar(1),\n" +
                        "   totalprice double,\n" +
                        "   orderdate date,\n" +
                        "   orderpriority varchar(15),\n" +
                        "   clerk varchar(15),\n" +
                        "   shippriority integer,\n" +
                        "   comment varchar(79)\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   location = \\E'.*/orders'\n\\Q" +
                        ")");
    }

    @Test
    public void testHideHiveSysSchema()
    {
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).doesNotContain("sys");
        assertQueryFails("SHOW TABLES IN hudi.sys", ".*Schema 'sys' does not exist");
    }

    @Test
    @Override
    public void testRefreshView()
    {
        // todo drop this override once CREATE TABLE and SUPPORTS_ADD_COLUMN will be implemented
        skipTestUnless(hasBehavior(SUPPORTS_REFRESH_VIEW) && hasBehavior(SUPPORTS_CREATE_TABLE) && hasBehavior(SUPPORTS_ADD_COLUMN));
        super.testRefreshView();
    }

    @Test
    @Override
    public void testCompatibleTypeChangeForView()
    {
        // todo drop this override once CREATE TABLE will be implemented
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));
        super.testCompatibleTypeChangeForView();
    }

    @Test
    @Override
    public void testCompatibleTypeChangeForView2()
    {
        // todo drop this override once CREATE TABLE will be implemented
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));
        super.testCompatibleTypeChangeForView2();
    }

    @Test
    @Override
    public void testCommentView()
    {
        // todo drop this override once COMMENT ON TABLE will be implemented
        try (TestView view = new TestView(getQueryRunner()::execute, "test_comment_view", "SELECT * FROM region")) {
            // comment set
            assertUpdate("COMMENT ON VIEW " + view.getName() + " IS 'new comment'");
            assertThat((String) computeScalar("SHOW CREATE VIEW " + view.getName())).contains("COMMENT 'new comment'");
            assertThat(getTableComment(view.getName())).isEqualTo("new comment");

            // comment deleted
            assertUpdate("COMMENT ON VIEW " + view.getName() + " IS NULL");
            assertThat(getTableComment(view.getName())).isEqualTo(null);

            // comment set to non-empty value before verifying setting empty comment
            assertUpdate("COMMENT ON VIEW " + view.getName() + " IS 'updated comment'");
            assertThat(getTableComment(view.getName())).isEqualTo("updated comment");

            // comment set to empty
            assertUpdate("COMMENT ON VIEW " + view.getName() + " IS ''");
            assertThat(getTableComment(view.getName())).isEqualTo("");
        }

        String viewName = "test_comment_view" + randomNameSuffix();
        try {
            // comment set when creating a table
            assertUpdate("CREATE VIEW " + viewName + " COMMENT 'new view comment' AS SELECT * FROM region");
            assertThat(getTableComment(viewName)).isEqualTo("new view comment");
        }
        finally {
            assertUpdate("DROP VIEW IF EXISTS " + viewName);
        }
    }

    @Test
    @Override
    public void testCommentViewColumn()
    {
        // todo drop this override once COMMENT ON TABLE will be implemented
        String viewColumnName = "regionkey";
        try (TestView view = new TestView(getQueryRunner()::execute, "test_comment_view_column", "SELECT * FROM region")) {
            // comment set
            assertUpdate("COMMENT ON COLUMN " + view.getName() + "." + viewColumnName + " IS 'new region key comment'");
            assertThat(getColumnComment(view.getName(), viewColumnName)).isEqualTo("new region key comment");

            // comment deleted
            assertUpdate("COMMENT ON COLUMN " + view.getName() + "." + viewColumnName + " IS NULL");
            assertThat(getColumnComment(view.getName(), viewColumnName)).isEqualTo(null);

            // comment set to non-empty value before verifying setting empty comment
            assertUpdate("COMMENT ON COLUMN " + view.getName() + "." + viewColumnName + " IS 'updated region key comment'");
            assertThat(getColumnComment(view.getName(), viewColumnName)).isEqualTo("updated region key comment");

            // comment set to empty
            assertUpdate("COMMENT ON COLUMN " + view.getName() + "." + viewColumnName + " IS ''");
            assertThat(getColumnComment(view.getName(), viewColumnName)).isEqualTo("");
        }
    }

    @Test
    public void testMetastoreAfterCreateView()
    {
        try (TestView table = new TestView(getQueryRunner()::execute, "test_cache_metastore", "SELECT 1 col")) {
            assertThat(metastore.getTable("tests", table.getName()).orElseThrow().getParameters())
                    .doesNotContainKeys("trino_last_transaction_version", "trino_metadata_schema_string")
                    .contains(entry("comment", "Presto View"));
        }
    }
}
