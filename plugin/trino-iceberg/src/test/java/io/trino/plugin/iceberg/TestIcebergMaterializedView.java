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

import io.trino.Session;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.Table;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.sql.tree.ExplainType;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.nio.file.Path;
import java.util.Map;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD) // Uses file metastore sharing location between catalogs
public class TestIcebergMaterializedView
        extends BaseIcebergMaterializedViewTest
{
    private Session secondIceberg;
    private HiveMetastore metastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session icebergSession = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema("tpch")
                .build();
        QueryRunner queryRunner = DistributedQueryRunner.builder(icebergSession).build();
        try {
            Path baseDataDir = queryRunner.getCoordinator().getBaseDataDir();
            queryRunner.installPlugin(new TestingIcebergPlugin(baseDataDir));
            queryRunner.createCatalog("iceberg", "iceberg", Map.of(
                    "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                    // Intentionally sharing the file metastore directory with Hive
                    "hive.metastore.catalog.dir", "local:///iceberg-catalog",
                    "iceberg.hive-catalog-name", "hive"));

            metastore = getHiveMetastore(queryRunner);

            queryRunner.createCatalog("iceberg2", "iceberg", Map.of(
                    "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                    "hive.metastore.catalog.dir", "local:///iceberg2-catalog",
                    "iceberg.hive-catalog-name", "hive"));

            secondIceberg = Session.builder(queryRunner.getDefaultSession())
                    .setCatalog("iceberg2")
                    .build();

            queryRunner.createCatalog("iceberg_legacy_mv", "iceberg", Map.of(
                    "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                    // Intentionally sharing the file metastore directory with Iceberg
                    "hive.metastore.catalog.dir", "local:///iceberg-catalog",
                    "iceberg.hive-catalog-name", "hive",
                    "iceberg.materialized-views.hide-storage-table", "false"));

            queryRunner.execute(secondIceberg, "CREATE SCHEMA " + secondIceberg.getSchema().orElseThrow());

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            queryRunner.installPlugin(createMockConnectorPlugin());
            queryRunner.createCatalog("mock", "mock");

            queryRunner.execute("CREATE SCHEMA tpch");
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
        return queryRunner;
    }

    @Override
    protected String getSchemaDirectory()
    {
        return "local:///iceberg-catalog/tpch";
    }

    @Override
    protected String getStorageMetadataLocation(String materializedViewName)
    {
        Table table = metastore.getTable("tpch", materializedViewName).orElseThrow();
        return table.getParameters().get(METADATA_LOCATION_PROP);
    }

    @Test
    public void testTwoIcebergCatalogs()
    {
        Session defaultIceberg = getSession();

        // Base table for staleness check
        String createTable = "CREATE TABLE common_base_table AS SELECT 10 value";
        assertUpdate(secondIceberg, createTable, 1); // this one will be used by MV
        assertUpdate(defaultIceberg, createTable, 1); // this one exists so that it can be mistakenly treated as the base table

        assertUpdate(defaultIceberg,
                """
                CREATE MATERIALIZED VIEW iceberg.tpch.mv_on_iceberg2
                AS SELECT sum(value) AS s FROM iceberg2.tpch.common_base_table
                """);

        // The MV is initially stale
        assertThat(getExplainPlan("TABLE mv_on_iceberg2", ExplainType.Type.IO))
                .contains("\"table\" : \"common_base_table\"");
        assertThat(query("TABLE mv_on_iceberg2"))
                .matches("VALUES BIGINT '10'");

        // After REFRESH, the MV is fresh
        assertUpdate(defaultIceberg, "REFRESH MATERIALIZED VIEW mv_on_iceberg2", 1);
        assertThat(getExplainPlan("TABLE mv_on_iceberg2", ExplainType.Type.IO))
                .contains("\"table\" : \"mv_on_iceberg2$materialized_view_storage")
                .doesNotContain("common_base_table");
        assertThat(query("TABLE mv_on_iceberg2"))
                .matches("VALUES BIGINT '10'");

        // After INSERT to the base table, the MV is still fresh, because it currently does not detect changes to tables in other catalog.
        assertUpdate(secondIceberg, "INSERT INTO common_base_table VALUES 7", 1);
        assertThat(getExplainPlan("TABLE mv_on_iceberg2", ExplainType.Type.IO))
                .contains("\"table\" : \"mv_on_iceberg2$materialized_view_storage")
                .doesNotContain("common_base_table");
        assertThat(query("TABLE mv_on_iceberg2"))
                .matches("VALUES BIGINT '10'");

        // After REFRESH, the MV is fresh again
        assertUpdate(defaultIceberg, "REFRESH MATERIALIZED VIEW mv_on_iceberg2", 1);
        assertThat(getExplainPlan("TABLE mv_on_iceberg2", ExplainType.Type.IO))
                .contains("\"table\" : \"mv_on_iceberg2$materialized_view_storage")
                .doesNotContain("common_base_table");
        assertThat(query("TABLE mv_on_iceberg2"))
                .matches("VALUES BIGINT '17'");

        assertUpdate(secondIceberg, "DROP TABLE common_base_table");
        assertUpdate(defaultIceberg, "DROP TABLE common_base_table");
        assertUpdate("DROP MATERIALIZED VIEW mv_on_iceberg2");
    }

    @Test
    public void testForeignSourceWithZeroGracePeriodAndWhenStaleFail()
    {
        assertUpdate(secondIceberg, "CREATE TABLE zero_grace_base_table AS SELECT 10 value", 1);

        // Changes to a table in another catalog cannot be tracked, so such a view would never be reported
        // as fresh, not even directly after a successful refresh. A zero grace period accepts no staleness,
        // so WHEN STALE FAIL would reject every read of it. Creating it is rejected up front instead.
        assertQueryFails(
                """
                CREATE MATERIALIZED VIEW iceberg.tpch.mv_zero_grace_on_iceberg2
                GRACE PERIOD INTERVAL '0' SECOND
                WHEN STALE FAIL
                AS SELECT sum(value) AS s FROM iceberg2.tpch.zero_grace_base_table
                """,
                "line 1:1: Materialized view with a zero GRACE PERIOD and WHEN STALE FAIL cannot depend on another catalog \\[iceberg2], " +
                        "because such a source cannot be tracked for freshness and the view is never fresh\\. " +
                        "Use a non-zero GRACE PERIOD, or WHEN STALE INLINE\\.");

        assertUpdate(secondIceberg, "DROP TABLE zero_grace_base_table");
    }

    @Test
    public void testTableFunctionSourceWithZeroGracePeriodAndWhenStaleFail()
    {
        // A table function result cannot be tracked either, so such a view would never be fresh
        // and WHEN STALE FAIL would reject every read of it.
        assertQueryFails(
                """
                CREATE MATERIALIZED VIEW iceberg.tpch.mv_zero_grace_on_ptf
                GRACE PERIOD INTERVAL '0' SECOND
                WHEN STALE FAIL
                AS SELECT * FROM TABLE(mock.system.sequence_function())
                """,
                "line 1:1: Materialized view with a zero GRACE PERIOD and WHEN STALE FAIL cannot depend on a table function \\[mock.system.sequence_function], " +
                        "because such a source cannot be tracked for freshness and the view is never fresh\\. " +
                        "Use a non-zero GRACE PERIOD, or WHEN STALE INLINE\\.");
    }

    @Test
    public void testNonDeterministicSourceWithZeroGracePeriodAndWhenStaleFail()
    {
        assertUpdate("CREATE TABLE zero_grace_nondeterministic_base_table AS SELECT 10 value", 1);

        // A non-deterministic function changes value without any source changing, so such a view
        // would never be fresh and WHEN STALE FAIL would reject every read of it.
        assertQueryFails(
                """
                CREATE MATERIALIZED VIEW iceberg.tpch.mv_zero_grace_on_random
                GRACE PERIOD INTERVAL '0' SECOND
                WHEN STALE FAIL
                AS SELECT value, random() r FROM zero_grace_nondeterministic_base_table
                """,
                "line 1:1: Materialized view with a zero GRACE PERIOD and WHEN STALE FAIL cannot depend on a non-deterministic function \\[random], " +
                        "because such a source cannot be tracked for freshness and the view is never fresh\\. " +
                        "Use a non-zero GRACE PERIOD, or WHEN STALE INLINE\\.");

        // current_timestamp is not a resolved function, so it is detected separately.
        assertQueryFails(
                """
                CREATE MATERIALIZED VIEW iceberg.tpch.mv_zero_grace_on_current_timestamp
                GRACE PERIOD INTERVAL '0' SECOND
                WHEN STALE FAIL
                AS SELECT value, current_timestamp ts FROM zero_grace_nondeterministic_base_table
                """,
                "line 1:1: Materialized view with a zero GRACE PERIOD and WHEN STALE FAIL cannot depend on a current time function, " +
                        "because such a source cannot be tracked for freshness and the view is never fresh\\. " +
                        "Use a non-zero GRACE PERIOD, or WHEN STALE INLINE\\.");

        assertUpdate("DROP TABLE zero_grace_nondeterministic_base_table");
    }
}
