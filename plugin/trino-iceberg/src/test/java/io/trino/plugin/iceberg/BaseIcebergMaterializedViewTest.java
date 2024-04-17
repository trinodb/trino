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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.iceberg.fileio.ForwardingFileIo;
import io.trino.spi.Page;
import io.trino.spi.QueryId;
import io.trino.spi.SplitWeight;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.function.table.TableFunctionProcessorState;
import io.trino.spi.function.table.TableFunctionSplitProcessor;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.sql.tree.ExplainType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.function.table.TableFunctionProcessorState.Finished.FINISHED;
import static io.trino.spi.function.table.TableFunctionProcessorState.Processed.produced;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.testing.MaterializedResult.DEFAULT_PRECISION;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.DROP_MATERIALIZED_VIEW;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.REFRESH_MATERIALIZED_VIEW;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.RENAME_MATERIALIZED_VIEW;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class BaseIcebergMaterializedViewTest
        extends AbstractTestQueryFramework
{
    protected abstract String getSchemaDirectory();

    protected abstract String getStorageMetadataLocation(String materializedViewName);

    protected static MockConnectorPlugin createMockConnectorPlugin()
    {
        return new MockConnectorPlugin(MockConnectorFactory.builder()
                .withTableFunctions(ImmutableSet.of(new SequenceTableFunction()))
                .withFunctionProvider(Optional.of(new FunctionProvider()
                {
                    @Override
                    public TableFunctionProcessorProvider getTableFunctionProcessorProvider(ConnectorTableFunctionHandle functionHandle)
                    {
                        if (functionHandle instanceof SequenceTableFunctionHandle) {
                            return new SequenceTableFunctionProcessorProvider();
                        }
                        throw new IllegalArgumentException("This ConnectorTableFunctionHandle is not supported");
                    }
                }))
                .withTableFunctionSplitSources(functionHandle -> {
                    if (functionHandle instanceof SequenceTableFunctionHandle) {
                        return new FixedSplitSource(ImmutableList.of(new SequenceConnectorSplit()));
                    }
                    throw new IllegalArgumentException("This ConnectorTableFunctionHandle is not supported");
                })
                .build());
    }

    @BeforeAll
    public void setUp()
    {
        assertUpdate("CREATE TABLE base_table1(_bigint BIGINT, _date DATE) WITH (partitioning = ARRAY['_date'])");
        assertUpdate("INSERT INTO base_table1 VALUES (0, DATE '2019-09-08'), (1, DATE '2019-09-09'), (2, DATE '2019-09-09')", 3);
        assertUpdate("INSERT INTO base_table1 VALUES (3, DATE '2019-09-09'), (4, DATE '2019-09-10'), (5, DATE '2019-09-10')", 3);

        assertUpdate("CREATE TABLE base_table2 (_varchar VARCHAR, _bigint BIGINT, _date DATE) WITH (partitioning = ARRAY['_bigint', '_date'])");
        assertUpdate("INSERT INTO base_table2 VALUES ('a', 0, DATE '2019-09-08'), ('a', 1, DATE '2019-09-08'), ('a', 0, DATE '2019-09-09')", 3);
    }

    @Test
    public void testShowTables()
    {
        assertUpdate("CREATE MATERIALIZED VIEW materialized_view_show_tables_test AS SELECT * FROM base_table1");
        Set<String> expectedTables = ImmutableSet.of("base_table1", "base_table2", "materialized_view_show_tables_test");
        Set<String> actualTables = computeActual("SHOW TABLES").getOnlyColumnAsSet().stream()
                .map(String.class::cast)
                .collect(toImmutableSet());
        // containsAll rather than isEqualTo as the test is not singleThreaded
        assertThat(actualTables).containsAll(expectedTables);

        assertUpdate("DROP MATERIALIZED VIEW materialized_view_show_tables_test");
    }

    @Test
    public void testCommentColumnMaterializedView()
    {
        String viewColumnName = "_bigint";
        String materializedViewName = "test_materialized_view_" + randomNameSuffix();
        assertUpdate(format("CREATE MATERIALIZED VIEW %s AS SELECT * FROM base_table1", materializedViewName));
        assertUpdate(format("COMMENT ON COLUMN %s.%s IS 'new comment'", materializedViewName, viewColumnName));
        assertThat(getColumnComment(materializedViewName, viewColumnName)).isEqualTo("new comment");
        assertQuery(format("SELECT count(*) FROM %s", materializedViewName), "VALUES 6");
        assertUpdate(format("DROP MATERIALIZED VIEW %s", materializedViewName));
    }

    @Test
    public void testMaterializedViewsMetadata()
    {
        String materializedViewName = "test_materialized_view_" + randomNameSuffix();

        computeActual("CREATE TABLE small_region AS SELECT * FROM tpch.tiny.region LIMIT 1");
        computeActual(format("CREATE MATERIALIZED VIEW %s AS SELECT * FROM small_region LIMIT 1", materializedViewName));

        // test freshness update
        assertQuery(
                // TODO (https://github.com/trinodb/trino/issues/9039) remove redundant schema_name filter
                format("SELECT freshness FROM system.metadata.materialized_views WHERE catalog_name = CURRENT_CATALOG AND schema_name = CURRENT_SCHEMA AND name = '%s'", materializedViewName),
                "VALUES 'STALE'");

        computeActual(format("REFRESH MATERIALIZED VIEW %s", materializedViewName));

        assertQuery(
                // TODO (https://github.com/trinodb/trino/issues/9039) remove redundant schema_name filter
                format("SELECT freshness FROM system.metadata.materialized_views WHERE catalog_name = CURRENT_CATALOG AND schema_name = CURRENT_SCHEMA AND name = '%s'", materializedViewName),
                "VALUES 'FRESH'");

        assertUpdate("DROP TABLE small_region");
        assertUpdate("DROP MATERIALIZED VIEW " + materializedViewName);
    }

    @Test
    public void testCreateWithInvalidPropertyFails()
    {
        assertThatThrownBy(() -> computeActual("CREATE MATERIALIZED VIEW materialized_view_with_property " +
                "WITH (invalid_property = ARRAY['_date']) AS " +
                "SELECT _bigint, _date FROM base_table1"))
                .hasMessage("Catalog 'iceberg' materialized view property 'invalid_property' does not exist");
    }

    @Test
    public void testCreateWithDuplicateSourceTableSucceeds()
    {
        assertUpdate("" +
                "CREATE MATERIALIZED VIEW materialized_view_with_duplicate_source AS " +
                "SELECT _bigint, _date FROM base_table1 " +
                "UNION ALL " +
                "SELECT _bigint, _date FROM base_table1 ");

        assertUpdate("REFRESH MATERIALIZED VIEW materialized_view_with_duplicate_source", 12);

        assertQuery("SELECT count(*) FROM materialized_view_with_duplicate_source", "VALUES 12");
        assertUpdate("DROP MATERIALIZED VIEW materialized_view_with_duplicate_source");
    }

    @Test
    public void testShowCreate()
    {
        String schema = getSession().getSchema().orElseThrow();

        assertUpdate("CREATE MATERIALIZED VIEW test_mv_show_create " +
                "WITH (\n" +
                "   partitioning = ARRAY['_date'],\n" +
                "   format = 'ORC',\n" +
                "   orc_bloom_filter_columns = ARRAY['_date'],\n" +
                "   orc_bloom_filter_fpp = 0.1) AS " +
                "SELECT _bigint, _date FROM base_table1");
        assertQuery("SELECT COUNT(*) FROM test_mv_show_create", "VALUES 6");

        assertThat((String) computeScalar("SHOW CREATE MATERIALIZED VIEW test_mv_show_create"))
                .matches(
                        "\\QCREATE MATERIALIZED VIEW iceberg." + schema + ".test_mv_show_create\n" +
                                "WITH (\n" +
                                "   format = 'ORC',\n" +
                                "   format_version = 2,\n" +
                                "   location = '" + getSchemaDirectory() + "/test_mv_show_create-\\E[0-9a-f]+\\Q',\n" +
                                "   orc_bloom_filter_columns = ARRAY['_date'],\n" +
                                "   orc_bloom_filter_fpp = 1E-1,\n" +
                                "   partitioning = ARRAY['_date'],\n" +
                                "   storage_schema = '" + schema + "'\n" +
                                ") AS\n" +
                                "SELECT\n" +
                                "  _bigint\n" +
                                ", _date\n" +
                                "FROM\n" +
                                "  base_table1");
        assertUpdate("DROP MATERIALIZED VIEW test_mv_show_create");
    }

    @Test
    public void testSystemMaterializedViewProperties()
    {
        assertThat(computeActual("SELECT * FROM system.metadata.materialized_view_properties WHERE catalog_name = 'iceberg'"))
                .contains(new MaterializedRow(DEFAULT_PRECISION, "iceberg", "partitioning", "[]", "array(varchar)", "Partition transforms"));
    }

    @Test
    public void testSessionCatalogSchema()
    {
        String schema = getSession().getSchema().orElseThrow();
        Session session = Session.builder(getSession())
                .setCatalog("tpch")
                .setSchema("tiny")
                .build();
        String qualifiedMaterializedViewName = "iceberg." + schema + ".materialized_view_session_test";
        assertUpdate(session, "CREATE MATERIALIZED VIEW " + qualifiedMaterializedViewName + " AS SELECT * FROM nation");
        assertQuery(session, "SELECT COUNT(*) FROM " + qualifiedMaterializedViewName, "VALUES 25");
        assertUpdate(session, "DROP MATERIALIZED VIEW " + qualifiedMaterializedViewName);

        session = Session.builder(getSession())
                .setCatalog(Optional.empty())
                .setSchema(Optional.empty())
                .build();
        assertUpdate(session, "CREATE MATERIALIZED VIEW " + qualifiedMaterializedViewName + " AS SELECT * FROM iceberg." + schema + ".base_table1");
        assertQuery(session, "SELECT COUNT(*) FROM " + qualifiedMaterializedViewName, "VALUES 6");
        assertUpdate(session, "DROP MATERIALIZED VIEW " + qualifiedMaterializedViewName);
    }

    @Test
    public void testDropIfExists()
    {
        String schema = getSession().getSchema().orElseThrow();
        assertQueryFails(
                "DROP MATERIALIZED VIEW non_existing_materialized_view",
                "line 1:1: Materialized view 'iceberg." + schema + ".non_existing_materialized_view' does not exist");
        assertUpdate("DROP MATERIALIZED VIEW IF EXISTS non_existing_materialized_view");
    }

    @Test
    public void testDropDenyPermission()
    {
        assertUpdate("CREATE MATERIALIZED VIEW materialized_view_drop_deny AS SELECT * FROM base_table1");
        assertAccessDenied(
                "DROP MATERIALIZED VIEW materialized_view_drop_deny",
                "Cannot drop materialized view .*.materialized_view_drop_deny.*",
                privilege("materialized_view_drop_deny", DROP_MATERIALIZED_VIEW));
        assertUpdate("DROP MATERIALIZED VIEW materialized_view_drop_deny");
    }

    @Test
    public void testRenameDenyPermission()
    {
        assertUpdate("CREATE MATERIALIZED VIEW materialized_view_rename_deny AS SELECT * FROM base_table1");
        assertAccessDenied(
                "ALTER MATERIALIZED VIEW materialized_view_rename_deny RENAME TO materialized_view_rename_deny_new",
                "Cannot rename materialized view .*.materialized_view_rename_deny.*",
                privilege("materialized_view_rename_deny", RENAME_MATERIALIZED_VIEW));
        assertUpdate("DROP MATERIALIZED VIEW materialized_view_rename_deny");
    }

    @Test
    public void testRefreshDenyPermission()
    {
        assertUpdate("CREATE MATERIALIZED VIEW materialized_view_refresh_deny AS SELECT * FROM base_table1");
        assertAccessDenied(
                "REFRESH MATERIALIZED VIEW materialized_view_refresh_deny",
                "Cannot refresh materialized view .*.materialized_view_refresh_deny.*",
                privilege("materialized_view_refresh_deny", REFRESH_MATERIALIZED_VIEW));

        assertUpdate("DROP MATERIALIZED VIEW materialized_view_refresh_deny");
    }

    @Test
    public void testCreateRefreshSelect()
    {
        Session session = getSession();

        // A very simple non-partitioned materialized view
        assertUpdate("CREATE MATERIALIZED VIEW materialized_view_no_part as select * from base_table1");
        // A non-partitioned materialized view with grouping and aggregation
        assertUpdate("CREATE MATERIALIZED VIEW materialized_view_agg as select _date, count(_date) as num_dates from base_table1 group by 1");
        // A partitioned materialized view with grouping and aggregation
        assertUpdate("CREATE MATERIALIZED VIEW materialized_view_part WITH (partitioning = ARRAY['_date']) as select _date, count(_date) as num_dates from base_table1 group by 1");
        // A non-partitioned join materialized view
        assertUpdate("CREATE MATERIALIZED VIEW materialized_view_join as " +
                "select t2._bigint, _varchar, t1._date from base_table1 t1, base_table2 t2 where t1._date = t2._date");
        // A partitioned join materialized view
        assertUpdate("CREATE MATERIALIZED VIEW materialized_view_join_part WITH (partitioning = ARRAY['_date', '_bigint']) as " +
                "select t1._bigint, _varchar, t2._date, sum(1) as my_sum from base_table1 t1, base_table2 t2 where t1._date = t2._date group by 1, 2, 3 order by 1, 2");

        // The tests here follow the pattern:
        // 1. Select the data from unrefreshed materialized view, verify the number of rows in the result
        // 2. Ensure that plan uses base tables and not the storage table
        // 3. Refresh the materialized view
        // 4. Select the data from refreshed materialized view, verify the number of rows in the result
        // 5. Ensure that the plan uses the storage table
        // 6. In some cases validate the result data
        assertThat(computeActual("SELECT * FROM materialized_view_no_part").getRowCount()).isEqualTo(6);
        assertThat(getExplainPlan("SELECT * FROM materialized_view_no_part", ExplainType.Type.IO))
                .contains("base_table1");
        assertUpdate("REFRESH MATERIALIZED VIEW materialized_view_no_part", 6);
        assertThat(computeActual("SELECT * FROM materialized_view_no_part").getRowCount()).isEqualTo(6);
        assertThat(getExplainPlan("SELECT * FROM materialized_view_no_part", ExplainType.Type.IO)).doesNotContain("base_table1");

        assertThat(computeActual("SELECT * FROM materialized_view_agg").getRowCount()).isEqualTo(3);
        assertThat(getExplainPlan("SELECT * FROM materialized_view_agg", ExplainType.Type.IO))
                .contains("base_table1");
        assertUpdate("REFRESH MATERIALIZED VIEW materialized_view_agg", 3);
        assertThat(computeActual("SELECT * FROM materialized_view_agg").getRowCount()).isEqualTo(3);
        assertThat(getExplainPlan("SELECT * FROM materialized_view_agg", ExplainType.Type.IO))
                .doesNotContain("base_table1");
        assertQuery(session, "SELECT * FROM materialized_view_agg", "VALUES (DATE '2019-09-10', 2)," +
                "(DATE '2019-09-08', 1), (DATE '2019-09-09', 3)");

        assertThat(computeActual("SELECT * FROM materialized_view_part").getRowCount()).isEqualTo(3);
        assertThat(getExplainPlan("SELECT * FROM materialized_view_part", ExplainType.Type.IO))
                .contains("base_table1");
        assertUpdate("REFRESH MATERIALIZED VIEW materialized_view_part", 3);
        assertThat(computeActual("SELECT * FROM materialized_view_part").getRowCount()).isEqualTo(3);
        assertThat(getExplainPlan("SELECT * FROM materialized_view_part", ExplainType.Type.IO)).doesNotContain("base_table1");

        assertThat(computeActual("SELECT * FROM materialized_view_join").getRowCount()).isEqualTo(5);
        assertThat(getExplainPlan("SELECT * FROM materialized_view_join", ExplainType.Type.IO)).contains("base_table1", "base_table2");
        assertUpdate("REFRESH MATERIALIZED VIEW materialized_view_join", 5);
        assertThat(computeActual("SELECT * FROM materialized_view_join").getRowCount()).isEqualTo(5);
        assertThat(getExplainPlan("SELECT * FROM materialized_view_join", ExplainType.Type.IO)).doesNotContain("base_table1", "base_table2");

        assertThat(computeActual("SELECT * FROM materialized_view_join_part").getRowCount()).isEqualTo(4);
        assertThat(getExplainPlan("SELECT * FROM materialized_view_join_part", ExplainType.Type.IO)).contains("base_table1", "base_table2");
        assertUpdate("REFRESH MATERIALIZED VIEW materialized_view_join_part", 4);
        assertThat(computeActual("SELECT * FROM materialized_view_join_part").getRowCount()).isEqualTo(4);
        assertThat(getExplainPlan("SELECT * FROM materialized_view_join_part", ExplainType.Type.IO)).doesNotContain("base_table1", "base_table2");
        assertQuery(session, "SELECT * FROM materialized_view_join_part", "VALUES (2, 'a', DATE '2019-09-09', 1), " +
                "(0, 'a', DATE '2019-09-08', 2), (3, 'a', DATE '2019-09-09', 1), (1, 'a', DATE '2019-09-09', 1)");

        assertUpdate("DROP MATERIALIZED VIEW materialized_view_no_part");
        assertUpdate("DROP MATERIALIZED VIEW materialized_view_agg");
        assertUpdate("DROP MATERIALIZED VIEW materialized_view_part");
        assertUpdate("DROP MATERIALIZED VIEW materialized_view_join");
        assertUpdate("DROP MATERIALIZED VIEW materialized_view_join_part");
    }

    @Test
    public void testDetectStaleness()
    {
        // Base tables and materialized views for staleness check
        assertUpdate("CREATE TABLE base_table3(_bigint BIGINT, _date DATE) WITH (partitioning = ARRAY['_date'])");
        assertUpdate("INSERT INTO base_table3 VALUES (0, DATE '2019-09-08'), (1, DATE '2019-09-09'), (2, DATE '2019-09-09')", 3);

        assertUpdate("CREATE TABLE base_table4 (_varchar VARCHAR, _bigint BIGINT, _date DATE) WITH (partitioning = ARRAY['_bigint', '_date'])");
        assertUpdate("INSERT INTO base_table4 VALUES ('a', 0, DATE '2019-09-08'), ('a', 1, DATE '2019-09-08'), ('a', 0, DATE '2019-09-09')", 3);

        // A partitioned materialized view with grouping and aggregation
        assertUpdate("CREATE MATERIALIZED VIEW materialized_view_part_stale WITH (partitioning = ARRAY['_date']) AS SELECT _date, count(_date) AS num_dates FROM base_table3 GROUP BY 1");
        // A non-partitioned join materialized view
        assertUpdate("CREATE MATERIALIZED VIEW materialized_view_join_stale as " +
                "SELECT t2._bigint, _varchar, t1._date FROM base_table3 t1, base_table4 t2 WHERE t1._date = t2._date");
        // A partitioned join materialized view
        assertUpdate("CREATE MATERIALIZED VIEW materialized_view_join_part_stale WITH (partitioning = ARRAY['_date', '_bigint']) as " +
                "SELECT t1._bigint, _varchar, t2._date, sum(1) AS my_sum FROM base_table3 t1, base_table4 t2 WHERE t1._date = t2._date GROUP BY 1, 2, 3 ORDER BY 1, 2");

        // Ensure that when data is inserted into base table, materialized view is rendered stale. Note that, currently updates and deletes to/from iceberg tables is not supported.
        assertUpdate("REFRESH MATERIALIZED VIEW materialized_view_part_stale", 2);
        assertUpdate("REFRESH MATERIALIZED VIEW materialized_view_join_stale", 4);
        assertUpdate("REFRESH MATERIALIZED VIEW materialized_view_join_part_stale", 3);

        assertUpdate("INSERT INTO base_table3 VALUES (3, DATE '2019-09-09'), (4, DATE '2019-09-10'), (5, DATE '2019-09-10')", 3);
        assertThat(getExplainPlan("SELECT * FROM materialized_view_part_stale", ExplainType.Type.IO))
                .doesNotContain("base_table");

        assertThat(getExplainPlan("SELECT * FROM materialized_view_join_stale", ExplainType.Type.IO))
                .doesNotContain("base_table");

        assertThat(getExplainPlan("SELECT * FROM materialized_view_join_part_stale", ExplainType.Type.IO))
                .doesNotContain("base_table");

        assertUpdate("REFRESH MATERIALIZED VIEW materialized_view_part_stale", 3);
        assertUpdate("REFRESH MATERIALIZED VIEW materialized_view_join_stale", 5);
        assertUpdate("REFRESH MATERIALIZED VIEW materialized_view_join_part_stale", 4);

        assertThat(getExplainPlan("SELECT * FROM materialized_view_part_stale", ExplainType.Type.IO))
                .doesNotContain("base_table3");

        assertThat(getExplainPlan("SELECT * FROM materialized_view_join_stale", ExplainType.Type.IO))
                .doesNotContain("base_table3", "base_table4");

        assertThat(getExplainPlan("SELECT * FROM materialized_view_join_part_stale", ExplainType.Type.IO))
                .doesNotContain("base_table3", "base_table4");

        assertUpdate("DROP TABLE base_table3");
        assertUpdate("DROP TABLE base_table4");
        assertUpdate("DROP MATERIALIZED VIEW materialized_view_part_stale");
        assertUpdate("DROP MATERIALIZED VIEW materialized_view_join_stale");
        assertUpdate("DROP MATERIALIZED VIEW materialized_view_join_part_stale");
    }

    @Test
    public void testMaterializedViewOnExpiredTable()
    {
        Session sessionWithShortRetentionUnlocked = Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", "expire_snapshots_min_retention", "0s")
                .build();

        assertUpdate("CREATE TABLE mv_on_expired_base_table AS SELECT 10 a", 1);
        assertUpdate("""
                CREATE MATERIALIZED VIEW mv_on_expired_the_mv
                GRACE PERIOD INTERVAL '0' SECOND
                AS SELECT sum(a) s FROM mv_on_expired_base_table""");

        assertUpdate("REFRESH MATERIALIZED VIEW mv_on_expired_the_mv", 1);
        // View is fresh
        assertThat(query("TABLE mv_on_expired_the_mv"))
                .matches("VALUES BIGINT '10'");

        // Create two new snapshots
        assertUpdate("INSERT INTO mv_on_expired_base_table VALUES 7", 1);
        assertUpdate("INSERT INTO mv_on_expired_base_table VALUES 5", 1);

        // Expire snapshots, so that the original one is not live and not parent of any live
        computeActual(sessionWithShortRetentionUnlocked, "ALTER TABLE mv_on_expired_base_table EXECUTE EXPIRE_SNAPSHOTS (retention_threshold => '0s')");

        // View still can be queried
        assertThat(query("TABLE mv_on_expired_the_mv"))
                .matches("VALUES BIGINT '22'");

        // View can also be refreshed
        assertUpdate("REFRESH MATERIALIZED VIEW mv_on_expired_the_mv", 1);
        assertThat(query("TABLE mv_on_expired_the_mv"))
                .matches("VALUES BIGINT '22'");

        assertUpdate("DROP TABLE mv_on_expired_base_table");
        assertUpdate("DROP MATERIALIZED VIEW mv_on_expired_the_mv");
    }

    @Test
    public void testMaterializedViewOnTableRolledBack()
    {
        assertUpdate("CREATE TABLE mv_on_rolled_back_base_table(a integer)");
        assertUpdate("""
                CREATE MATERIALIZED VIEW mv_on_rolled_back_the_mv
                GRACE PERIOD INTERVAL '0' SECOND
                AS SELECT sum(a) s FROM mv_on_rolled_back_base_table""");

        // Create some snapshots
        assertUpdate("INSERT INTO mv_on_rolled_back_base_table VALUES 4", 1);
        long firstSnapshot = getLatestSnapshotId("mv_on_rolled_back_base_table");
        assertUpdate("INSERT INTO mv_on_rolled_back_base_table VALUES 8", 1);

        // Base MV on a snapshot "in the future"
        assertUpdate("REFRESH MATERIALIZED VIEW mv_on_rolled_back_the_mv", 1);
        assertUpdate(format("CALL system.rollback_to_snapshot(CURRENT_SCHEMA, 'mv_on_rolled_back_base_table', %s)", firstSnapshot));

        // View still can be queried
        assertThat(query("TABLE mv_on_rolled_back_the_mv"))
                .matches("VALUES BIGINT '4'");

        // View can also be refreshed
        assertUpdate("REFRESH MATERIALIZED VIEW mv_on_rolled_back_the_mv", 1);
        assertThat(query("TABLE mv_on_rolled_back_the_mv"))
                .matches("VALUES BIGINT '4'");

        assertUpdate("DROP TABLE mv_on_rolled_back_base_table");
        assertUpdate("DROP MATERIALIZED VIEW mv_on_rolled_back_the_mv");
    }

    @Test
    public void testSqlFeatures()
    {
        String schema = getSession().getSchema().orElseThrow();

        // Materialized views to test SQL features
        assertUpdate("CREATE MATERIALIZED VIEW materialized_view_window WITH (partitioning = ARRAY['_date']) AS SELECT _date, " +
                "sum(_bigint) OVER (PARTITION BY _date ORDER BY _date) as sum_ints from base_table1");
        assertUpdate("REFRESH MATERIALIZED VIEW materialized_view_window", 6);
        assertUpdate("CREATE MATERIALIZED VIEW materialized_view_union WITH (partitioning = ARRAY['_date']) AS " +
                "select _date, count(_date) as num_dates from base_table1 group by 1 UNION " +
                "select _date, count(_date) as num_dates from base_table2 group by 1");
        assertUpdate("REFRESH MATERIALIZED VIEW materialized_view_union", 5);
        assertUpdate("CREATE MATERIALIZED VIEW materialized_view_subquery WITH (partitioning = ARRAY['_date']) AS " +
                "SELECT _date, count(_date) AS num_dates FROM base_table1 WHERE _date = (select max(_date) FROM base_table2) GROUP BY 1");
        assertUpdate("REFRESH MATERIALIZED VIEW materialized_view_subquery", 1);

        // This set of tests intend to test various SQL features in the context of materialized views. It also tests commands pertaining to materialized views.
        assertThat(getExplainPlan("SELECT * FROM materialized_view_window", ExplainType.Type.IO))
                .doesNotContain("base_table1");
        assertThat(getExplainPlan("SELECT * FROM materialized_view_union", ExplainType.Type.IO))
                .doesNotContain("base_table1");
        assertThat(getExplainPlan("SELECT * FROM materialized_view_subquery", ExplainType.Type.IO))
                .doesNotContain("base_table1");

        String qualifiedMaterializedViewName = "iceberg." + schema + ".materialized_view_window";
        assertQueryFails("SHOW CREATE VIEW materialized_view_window",
                "line 1:1: Relation '" + qualifiedMaterializedViewName + "' is a materialized view, not a view");

        assertThat((String) computeScalar("SHOW CREATE MATERIALIZED VIEW materialized_view_window"))
                .matches("\\QCREATE MATERIALIZED VIEW " + qualifiedMaterializedViewName + "\n" +
                        "WITH (\n" +
                        "   format = 'PARQUET',\n" +
                        "   format_version = 2,\n" +
                        "   location = '" + getSchemaDirectory() + "/materialized_view_window-\\E[0-9a-f]+\\Q',\n" +
                        "   partitioning = ARRAY['_date'],\n" +
                        "   storage_schema = '" + schema + "'\n" +
                        ") AS\n" +
                        "SELECT\n" +
                        "  _date\n" +
                        ", sum(_bigint) OVER (PARTITION BY _date ORDER BY _date ASC) sum_ints\n" +
                        "FROM\n" +
                        "  base_table1");

        assertQueryFails("INSERT INTO materialized_view_window VALUES (0, '2019-09-08'), (1, DATE '2019-09-09'), (2, DATE '2019-09-09')",
                "Inserting into materialized views is not supported");

        computeScalar("EXPLAIN (TYPE LOGICAL) REFRESH MATERIALIZED VIEW materialized_view_window");
        computeScalar("EXPLAIN (TYPE DISTRIBUTED) REFRESH MATERIALIZED VIEW materialized_view_window");
        computeScalar("EXPLAIN (TYPE VALIDATE) REFRESH MATERIALIZED VIEW materialized_view_window");
        computeScalar("EXPLAIN (TYPE IO) REFRESH MATERIALIZED VIEW materialized_view_window");
        computeScalar("EXPLAIN ANALYZE REFRESH MATERIALIZED VIEW materialized_view_window");

        assertUpdate("DROP MATERIALIZED VIEW materialized_view_window");
        assertUpdate("DROP MATERIALIZED VIEW materialized_view_union");
        assertUpdate("DROP MATERIALIZED VIEW materialized_view_subquery");
    }

    @Test
    public void testReplace()
    {
        // Materialized view to test 'replace' feature
        assertUpdate("CREATE MATERIALIZED VIEW materialized_view_replace WITH (partitioning = ARRAY['_date']) as select _date, count(_date) as num_dates from base_table1 group by 1");
        assertUpdate("REFRESH MATERIALIZED VIEW materialized_view_replace", 3);

        assertUpdate("CREATE OR REPLACE MATERIALIZED VIEW materialized_view_replace as select sum(1) as num_rows from base_table2");
        assertThat(getExplainPlan("SELECT * FROM materialized_view_replace", ExplainType.Type.IO))
                .contains("base_table2");
        assertUpdate("REFRESH MATERIALIZED VIEW materialized_view_replace", 1);
        computeScalar("SELECT * FROM materialized_view_replace");
        assertThat(query("SELECT * FROM materialized_view_replace"))
                .matches("VALUES BIGINT '3'");

        assertUpdate("DROP MATERIALIZED VIEW materialized_view_replace");
    }

    @Test
    public void testCreateMaterializedViewWhenTableExists()
    {
        String schema = getSession().getSchema().orElseThrow();
        assertUpdate("CREATE TABLE test_create_materialized_view_when_table_exists (a INT, b INT)");
        assertThat(query("CREATE OR REPLACE MATERIALIZED VIEW test_create_materialized_view_when_table_exists AS SELECT sum(1) AS num_rows FROM base_table2"))
                .failure().hasMessage("Existing table is not a Materialized View: " + schema + ".test_create_materialized_view_when_table_exists");
        assertThat(query("CREATE MATERIALIZED VIEW IF NOT EXISTS test_create_materialized_view_when_table_exists AS SELECT sum(1) AS num_rows FROM base_table2"))
                .failure().hasMessage("Existing table is not a Materialized View: " + schema + ".test_create_materialized_view_when_table_exists");
        assertUpdate("DROP TABLE test_create_materialized_view_when_table_exists");
    }

    @Test
    public void testDropMaterializedViewCannotDropTable()
    {
        String schema = getSession().getSchema().orElseThrow();
        assertUpdate("CREATE TABLE test_drop_materialized_view_cannot_drop_table (a INT, b INT)");
        assertThat(query("DROP MATERIALIZED VIEW test_drop_materialized_view_cannot_drop_table"))
                .failure().hasMessageContaining("Materialized view 'iceberg." + schema + ".test_drop_materialized_view_cannot_drop_table' does not exist, but a table with that name exists");
        assertUpdate("DROP TABLE test_drop_materialized_view_cannot_drop_table");
    }

    @Test
    public void testRenameMaterializedViewCannotRenameTable()
    {
        String schema = getSession().getSchema().orElseThrow();
        assertUpdate("CREATE TABLE test_rename_materialized_view_cannot_rename_table (a INT, b INT)");
        assertThat(query("ALTER MATERIALIZED VIEW test_rename_materialized_view_cannot_rename_table RENAME TO new_materialized_view_name"))
                .failure().hasMessageContaining("Materialized View 'iceberg." + schema + ".test_rename_materialized_view_cannot_rename_table' does not exist, but a table with that name exists");
        assertUpdate("DROP TABLE test_rename_materialized_view_cannot_rename_table");
    }

    @Test
    public void testNestedMaterializedViews()
    {
        // Base table and materialized views for nested materialized view testing
        assertUpdate("CREATE TABLE base_table5(_bigint BIGINT, _date DATE) WITH (partitioning = ARRAY['_date'])");
        assertUpdate("INSERT INTO base_table5 VALUES (0, DATE '2019-09-08'), (1, DATE '2019-09-09'), (2, DATE '2019-09-09')", 3);
        assertUpdate("CREATE MATERIALIZED VIEW materialized_view_level1 WITH (partitioning = ARRAY['_date']) as select _date, count(_date) as num_dates from base_table5 group by 1");
        assertUpdate("CREATE MATERIALIZED VIEW materialized_view_level2 WITH (partitioning = ARRAY['_date']) as select _date, num_dates from materialized_view_level1");

        // Unrefreshed 2nd level materialized view .. resolves to base table
        assertThat(getExplainPlan("SELECT * FROM materialized_view_level2", ExplainType.Type.IO))
                .contains("base_table5");
        assertUpdate("REFRESH MATERIALIZED VIEW materialized_view_level2", 2);

        // Refreshed 2nd level materialized view .. resolves to storage table
        assertThat(getExplainPlan("SELECT * FROM materialized_view_level2", ExplainType.Type.IO))
                .doesNotContain("base_table5");

        // Re-refreshing 2nd level materialized view is a no-op
        assertUpdate("REFRESH MATERIALIZED VIEW materialized_view_level2", 0);

        // Insert into the base table
        assertUpdate("INSERT INTO base_table5 VALUES (3, DATE '2019-09-09'), (4, DATE '2019-09-10'), (5, DATE '2019-09-10')", 3);
        assertUpdate("REFRESH MATERIALIZED VIEW materialized_view_level2", 3);

        // Refreshing the 2nd level (outer-most) materialized view does not refresh the 1st level (inner) materialized view.
        assertThat(getExplainPlan("SELECT * FROM materialized_view_level1", ExplainType.Type.IO))
                .contains("base_table5");

        assertUpdate("DROP TABLE base_table5");
        assertUpdate("DROP MATERIALIZED VIEW materialized_view_level1");
        assertUpdate("DROP MATERIALIZED VIEW materialized_view_level2");
    }

    @Test
    public void testBucketPartitioning()
    {
        testBucketPartitioning("integer", "20050909");
        testBucketPartitioning("bigint", "200509091331001234");
        testBucketPartitioning("decimal(8,5)", "DECIMAL '876.54321'");
        testBucketPartitioning("decimal(28,21)", "DECIMAL '1234567.890123456789012345678'");
        testBucketPartitioning("date", "DATE '2005-09-09'");
        testBucketPartitioning("time(6)", "TIME '13:31:00.123456'");
        testBucketPartitioning("timestamp(6)", "TIMESTAMP '2005-09-10 13:31:00.123456'");
        testBucketPartitioning("timestamp(6) with time zone", "TIMESTAMP '2005-09-10 13:00:00.123456 Europe/Warsaw'");
        testBucketPartitioning("varchar", "VARCHAR 'Greetings from Warsaw!'");
        testBucketPartitioning("uuid", "UUID '406caec7-68b9-4778-81b2-a12ece70c8b1'");
        testBucketPartitioning("varbinary", "X'66696E6465706920726F636B7321'");
    }

    private void testBucketPartitioning(String dataType, String exampleValue)
    {
        // validate the example value type
        assertThat(query("SELECT " + exampleValue))
                .matches("SELECT CAST(%s AS %S)".formatted(exampleValue, dataType));

        assertUpdate("CREATE MATERIALIZED VIEW test_bucket_partitioning WITH (partitioning=ARRAY['bucket(col, 4)']) AS SELECT * FROM (VALUES CAST(NULL AS %s), %s) t(col)"
                .formatted(dataType, exampleValue));
        try {
            TableMetadata storageMetadata = getStorageTableMetadata("test_bucket_partitioning");
            assertThat(storageMetadata.spec().fields()).hasSize(1);
            PartitionField bucketPartitionField = getOnlyElement(storageMetadata.spec().fields());
            assertThat(bucketPartitionField.name()).isEqualTo("col_bucket");
            assertThat(bucketPartitionField.transform().toString()).isEqualTo("bucket[4]");

            assertThat(query("SELECT * FROM test_bucket_partitioning WHERE col = " + exampleValue))
                    .matches("SELECT " + exampleValue);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW test_bucket_partitioning");
        }
    }

    @Test
    public void testTruncatePartitioning()
    {
        testTruncatePartitioning("integer", "20050909");
        testTruncatePartitioning("bigint", "200509091331001234");
        testTruncatePartitioning("decimal(8,5)", "DECIMAL '876.54321'");
        testTruncatePartitioning("decimal(28,21)", "DECIMAL '1234567.890123456789012345678'");
        testTruncatePartitioning("varchar", "VARCHAR 'Greetings from Warsaw!'");
    }

    private void testTruncatePartitioning(String dataType, String exampleValue)
    {
        // validate the example value type
        assertThat(query("SELECT " + exampleValue))
                .matches("SELECT CAST(%s AS %S)".formatted(exampleValue, dataType));

        assertUpdate("CREATE MATERIALIZED VIEW test_truncate_partitioning WITH (partitioning=ARRAY['truncate(col, 4)']) AS SELECT * FROM (VALUES CAST(NULL AS %s), %s) t(col)"
                .formatted(dataType, exampleValue));
        try {
            TableMetadata storageMetadata = getStorageTableMetadata("test_truncate_partitioning");
            assertThat(storageMetadata.spec().fields()).hasSize(1);
            PartitionField bucketPartitionField = getOnlyElement(storageMetadata.spec().fields());
            assertThat(bucketPartitionField.name()).isEqualTo("col_trunc");
            assertThat(bucketPartitionField.transform().toString()).isEqualTo("truncate[4]");

            assertThat(query("SELECT * FROM test_truncate_partitioning WHERE col = " + exampleValue))
                    .matches("SELECT " + exampleValue);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW test_truncate_partitioning");
        }
    }

    @Test
    public void testTemporalPartitioning()
    {
        testTemporalPartitioning("year", "date", "DATE '2005-09-09'");
        testTemporalPartitioning("year", "timestamp(6)", "TIMESTAMP '2005-09-10 13:31:00.123456'");
        testTemporalPartitioning("year", "timestamp(6) with time zone", "TIMESTAMP '2005-09-10 13:00:00.123456 Europe/Warsaw'");
        testTemporalPartitioning("month", "date", "DATE '2005-09-09'");
        testTemporalPartitioning("month", "timestamp(6)", "TIMESTAMP '2005-09-10 13:31:00.123456'");
        testTemporalPartitioning("month", "timestamp(6) with time zone", "TIMESTAMP '2005-09-10 13:00:00.123456 Europe/Warsaw'");
        testTemporalPartitioning("day", "date", "DATE '2005-09-09'");
        testTemporalPartitioning("day", "timestamp(6)", "TIMESTAMP '2005-09-10 13:31:00.123456'");
        testTemporalPartitioning("day", "timestamp(6) with time zone", "TIMESTAMP '2005-09-10 13:00:00.123456 Europe/Warsaw'");
        testTemporalPartitioning("hour", "timestamp(6)", "TIMESTAMP '2005-09-10 13:31:00.123456'");
        testTemporalPartitioning("hour", "timestamp(6) with time zone", "TIMESTAMP '2005-09-10 13:00:00.123456 Europe/Warsaw'");
    }

    private void testTemporalPartitioning(String partitioning, String dataType, String exampleValue)
    {
        // validate the example value type
        assertThat(query("SELECT " + exampleValue))
                .matches("SELECT CAST(%s AS %S)".formatted(exampleValue, dataType));

        assertUpdate("CREATE MATERIALIZED VIEW test_temporal_partitioning WITH (partitioning=ARRAY['%s(col)']) AS SELECT * FROM (VALUES CAST(NULL AS %s), %s) t(col)"
                .formatted(partitioning, dataType, exampleValue));
        try {
            TableMetadata storageMetadata = getStorageTableMetadata("test_temporal_partitioning");
            assertThat(storageMetadata.spec().fields()).hasSize(1);
            PartitionField bucketPartitionField = getOnlyElement(storageMetadata.spec().fields());
            assertThat(bucketPartitionField.name()).isEqualTo("col_" + partitioning);
            assertThat(bucketPartitionField.transform().toString()).isEqualTo(partitioning);

            assertThat(query("SELECT * FROM test_temporal_partitioning WHERE col = " + exampleValue))
                    .matches("SELECT " + exampleValue);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW test_temporal_partitioning");
        }
    }

    @Test
    public void testMaterializedViewSnapshotSummariesHaveTrinoQueryId()
    {
        String materializedViewName = "test_materialized_view_snapshot_query_ids" + randomNameSuffix();
        String sourceTableName = "test_source_table_for_mat_view" + randomNameSuffix();
        assertUpdate(format("CREATE TABLE %s (a bigint, b bigint)", sourceTableName));
        assertUpdate(format("CREATE MATERIALIZED VIEW %s WITH (partitioning = ARRAY['a']) AS SELECT * FROM %s", materializedViewName, sourceTableName));

        try {
            assertUpdate(format("INSERT INTO %s VALUES (1, 1), (1, 4), (2, 2)", sourceTableName), 3);

            QueryId refreshQueryId = getDistributedQueryRunner()
                    .executeWithPlan(getSession(), format("REFRESH MATERIALIZED VIEW %s", materializedViewName))
                    .queryId();
            String savedQueryId = getStorageTableMetadata(materializedViewName).currentSnapshot().summary().get("trino_query_id");
            assertThat(savedQueryId).isEqualTo(refreshQueryId.getId());
        }
        finally {
            assertUpdate("DROP TABLE " + sourceTableName);
            assertUpdate("DROP MATERIALIZED VIEW " + materializedViewName);
        }
    }

    @Test
    public void testMaterializedViewStorageTypeCoercions()
    {
        String materializedViewName = "test_materialized_view_storage_type_coercion" + randomNameSuffix();
        String sourceTableName = "test_materialized_view_storage" + randomNameSuffix();

        assertUpdate(format("""
                CREATE TABLE %s (
                    t_3 time(3),
                    t_9 time(9),
                    ts_3 timestamp(3),
                    ts_9 timestamp(9),
                    tswtz_3 timestamp(3) with time zone,
                    tswtz_9 timestamp(9) with time zone
                )
                """, sourceTableName));
        assertUpdate(format("INSERT INTO %s VALUES (localtime, localtime, localtimestamp, localtimestamp, current_timestamp, current_timestamp)", sourceTableName), 1);

        assertUpdate(format("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s", materializedViewName, sourceTableName));

        assertThat(query(format("SELECT * FROM %s WHERE t_3 < localtime", materializedViewName))).succeeds();
        assertThat(query(format("SELECT * FROM %s WHERE t_9 < localtime", materializedViewName))).succeeds();
        assertThat(query(format("SELECT * FROM %s WHERE ts_3 < localtimestamp", materializedViewName))).succeeds();
        assertThat(query(format("SELECT * FROM %s WHERE ts_9 < localtimestamp", materializedViewName))).succeeds();
        assertThat(query(format("SELECT * FROM %s WHERE tswtz_3 < current_timestamp", materializedViewName))).succeeds();
        assertThat(query(format("SELECT * FROM %s WHERE tswtz_9 < current_timestamp", materializedViewName))).succeeds();

        assertUpdate(format("REFRESH MATERIALIZED VIEW %s", materializedViewName), 1);

        assertThat(query(format("SELECT * FROM %s WHERE t_3 < localtime", materializedViewName))).succeeds();
        assertThat(query(format("SELECT * FROM %s WHERE t_9 < localtime", materializedViewName))).succeeds();
        assertThat(query(format("SELECT * FROM %s WHERE ts_3 < localtimestamp", materializedViewName))).succeeds();
        assertThat(query(format("SELECT * FROM %s WHERE ts_9 < localtimestamp", materializedViewName))).succeeds();
        assertThat(query(format("SELECT * FROM %s WHERE tswtz_3 < current_timestamp", materializedViewName))).succeeds();
        assertThat(query(format("SELECT * FROM %s WHERE tswtz_9 < current_timestamp", materializedViewName))).succeeds();
    }

    @Test
    public void testDropLegacyMaterializedView()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        String materializedViewName = "test_drop_legacy_materialized_view" + randomNameSuffix();
        String sourceTableName = "test_source_table_for_mat_view" + randomNameSuffix();
        assertUpdate(format("CREATE TABLE %s (a bigint, b bigint)", sourceTableName));
        assertUpdate(format("CREATE MATERIALIZED VIEW iceberg_legacy_mv.%s.%s AS SELECT * FROM %s", schemaName, materializedViewName, sourceTableName));

        try {
            // Refresh with legacy enabled
            assertUpdate(format("INSERT INTO %s VALUES (1, 1), (1, 4), (2, 2)", sourceTableName), 3);
            assertUpdate(format("REFRESH MATERIALIZED VIEW iceberg_legacy_mv.%s.%s", schemaName, materializedViewName), 3);

            // Refresh with legacy disabled
            assertUpdate(format("INSERT INTO %s VALUES (10, 10), (10, 40), (20, 20)", sourceTableName), 3);
            assertUpdate("REFRESH MATERIALIZED VIEW " + materializedViewName, 6);

            String storageTableName = (String) computeScalar("SELECT storage_table FROM system.metadata.materialized_views WHERE catalog_name = CURRENT_CATALOG AND schema_name = CURRENT_SCHEMA AND name = '" + materializedViewName + "'");
            assertThat(storageTableName)
                    .isEqualTo(computeScalar("SELECT storage_table FROM system.metadata.materialized_views WHERE catalog_name = 'iceberg_legacy_mv' AND schema_name = CURRENT_SCHEMA AND name = '" + materializedViewName + "'"))
                    .startsWith("st_");

            assertThat(query("TABLE " + materializedViewName)).matches("TABLE " + sourceTableName);
            assertThat(query("TABLE " + storageTableName)).matches("TABLE " + sourceTableName);
            assertUpdate("DROP MATERIALIZED VIEW " + materializedViewName);
            assertThat(query("TABLE " + materializedViewName)).failure().hasMessageMatching(".* does not exist");
            assertThat(query("TABLE " + storageTableName)).failure().hasMessageMatching(".* does not exist");
        }
        finally {
            assertUpdate("DROP TABLE " + sourceTableName);
            assertUpdate(format("DROP MATERIALIZED VIEW IF EXISTS iceberg_legacy_mv.%s.%s", schemaName, materializedViewName));
        }
    }

    @Test
    public void testMaterializedViewCreatedFromTableFunction()
    {
        String viewName = "materialized_view_for_ptf_" + randomNameSuffix();
        assertUpdate("CREATE MATERIALIZED VIEW " + viewName + " AS SELECT * FROM TABLE(mock.system.sequence_function())");

        assertFreshness(viewName, "STALE");
        assertThat(computeActual("SELECT last_fresh_time FROM system.metadata.materialized_views WHERE catalog_name = CURRENT_CATALOG AND schema_name = CURRENT_SCHEMA AND name = '" + viewName + "'").getOnlyValue()).isNull();
        int result1 = (int) computeActual("SELECT * FROM " + viewName).getOnlyValue();

        int result2 = (int) computeActual("SELECT * FROM " + viewName).getOnlyValue();
        assertThat(result2).isNotEqualTo(result1); // differs because PTF sequence_function is called directly as mv is considered stale
        assertFreshness(viewName, "STALE");
        assertThat(computeActual("SELECT last_fresh_time FROM system.metadata.materialized_views WHERE catalog_name = CURRENT_CATALOG AND schema_name = CURRENT_SCHEMA AND name = '" + viewName + "'").getOnlyValue()).isNull();

        assertUpdate("REFRESH MATERIALIZED VIEW " + viewName, 1);
        assertFreshness(viewName, "UNKNOWN");
        ZonedDateTime lastFreshTime = (ZonedDateTime) computeActual("SELECT last_fresh_time FROM system.metadata.materialized_views WHERE catalog_name = CURRENT_CATALOG AND schema_name = CURRENT_SCHEMA AND name = '" + viewName + "'").getOnlyValue();
        assertThat(lastFreshTime).isNotNull();
        int result3 = (int) computeActual("SELECT * FROM " + viewName).getOnlyValue();
        assertThat(result3).isNotEqualTo(result2);  // mv is not stale anymore so all selects until next refresh returns same result
        int result4 = (int) computeActual("SELECT * FROM " + viewName).getOnlyValue();
        int result5 = (int) computeActual("SELECT * FROM " + viewName).getOnlyValue();
        assertThat(result4).isEqualTo(result3);
        assertThat(result4).isEqualTo(result5);

        assertUpdate("REFRESH MATERIALIZED VIEW " + viewName, 1);
        assertThat((ZonedDateTime) computeActual("SELECT last_fresh_time FROM system.metadata.materialized_views WHERE catalog_name = CURRENT_CATALOG AND schema_name = CURRENT_SCHEMA AND name = '" + viewName + "'").getOnlyValue()).isAfter(lastFreshTime);
        assertFreshness(viewName, "UNKNOWN");
        int result6 = (int) computeActual("SELECT * FROM " + viewName).getOnlyValue();
        assertThat(result6).isNotEqualTo(result5);
    }

    @Test
    public void testMaterializedViewCreatedFromTableFunctionAndTable()
    {
        String sourceTableName = "source_table_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + sourceTableName + " (VALUE INTEGER)");
        assertUpdate("INSERT INTO " + sourceTableName + " VALUES 2", 1);
        String viewName = "materialized_view_for_ptf_adn_table_" + randomNameSuffix();
        assertUpdate("CREATE MATERIALIZED VIEW " + viewName + " AS SELECT * FROM TABLE(mock.system.sequence_function()) CROSS JOIN " + sourceTableName);

        List<MaterializedRow> materializedRows = computeActual("SELECT * FROM " + viewName).getMaterializedRows();
        assertThat(materializedRows.size()).isEqualTo(1);
        assertThat(materializedRows.get(0).getField(1)).isEqualTo(2);
        int valueFromPtf1 = (int) materializedRows.get(0).getField(0);
        assertFreshness(viewName, "STALE");
        assertThat(computeActual("SELECT last_fresh_time FROM system.metadata.materialized_views WHERE catalog_name = CURRENT_CATALOG AND schema_name = CURRENT_SCHEMA AND name = '" + viewName + "'").getOnlyValue()).isNull();

        materializedRows = computeActual("SELECT * FROM " + viewName).getMaterializedRows();
        assertThat(materializedRows.size()).isEqualTo(1);
        assertThat(materializedRows.get(0).getField(1)).isEqualTo(2);
        int valueFromPtf2 = (int) materializedRows.get(0).getField(0);
        assertThat(valueFromPtf2).isNotEqualTo(valueFromPtf1); // differs because PTF sequence_function is called directly as mv is considered stale
        assertFreshness(viewName, "STALE");
        assertThat(computeActual("SELECT last_fresh_time FROM system.metadata.materialized_views WHERE catalog_name = CURRENT_CATALOG AND schema_name = CURRENT_SCHEMA AND name = '" + viewName + "'").getOnlyValue()).isNull();

        assertUpdate("REFRESH MATERIALIZED VIEW " + viewName, 1);
        assertFreshness(viewName, "UNKNOWN");
        ZonedDateTime lastFreshTime = (ZonedDateTime) computeActual("SELECT last_fresh_time FROM system.metadata.materialized_views WHERE catalog_name = CURRENT_CATALOG AND schema_name = CURRENT_SCHEMA AND name = '" + viewName + "'").getOnlyValue();
        assertThat(lastFreshTime).isNotNull();
        materializedRows = computeActual("SELECT * FROM " + viewName).getMaterializedRows();
        assertThat(materializedRows.size()).isEqualTo(1);
        assertThat(materializedRows.get(0).getField(1)).isEqualTo(2);
        int valueFromPtf3 = (int) materializedRows.get(0).getField(0);
        assertThat(valueFromPtf3).isNotEqualTo(valueFromPtf1);
        assertThat(valueFromPtf3).isNotEqualTo(valueFromPtf2);

        materializedRows = computeActual("SELECT * FROM " + viewName).getMaterializedRows();
        assertThat(materializedRows.size()).isEqualTo(1);
        assertThat(materializedRows.get(0).getField(1)).isEqualTo(2);
        int valueFromPtf4 = (int) materializedRows.get(0).getField(0);
        assertThat(valueFromPtf4).isNotEqualTo(valueFromPtf1);
        assertThat(valueFromPtf4).isNotEqualTo(valueFromPtf2);
        assertThat(valueFromPtf4).isEqualTo(valueFromPtf3); // mv is not stale anymore so all selects until next refresh returns same result
    }

    @Test
    public void testMaterializedViewCreatedFromTableFunctionWithGracePeriod()
            throws InterruptedException
    {
        String viewName = "materialized_view_for_ptf_with_grace_period_" + randomNameSuffix();
        assertUpdate("CREATE MATERIALIZED VIEW " + viewName + " GRACE PERIOD INTERVAL '1' SECOND AS SELECT * FROM TABLE(mock.system.sequence_function())");

        int result1 = (int) computeActual("SELECT * FROM " + viewName).getOnlyValue();
        int result2 = (int) computeActual("SELECT * FROM " + viewName).getOnlyValue();
        assertThat(result2).isNotEqualTo(result1);

        assertUpdate("REFRESH MATERIALIZED VIEW " + viewName, 1);
        int result3 = (int) computeActual("SELECT * FROM " + viewName).getOnlyValue();
        assertThat(result3).isNotEqualTo(result2);
        Thread.sleep(1001);
        int result4 = (int) computeActual("SELECT * FROM " + viewName).getOnlyValue();
        assertThat(result4).isNotEqualTo(result3);
    }

    protected String getColumnComment(String tableName, String columnName)
    {
        return (String) computeScalar("SELECT comment FROM information_schema.columns WHERE table_schema = '" + getSession().getSchema().orElseThrow() + "' AND table_name = '" + tableName + "' AND column_name = '" + columnName + "'");
    }

    private TableMetadata getStorageTableMetadata(String materializedViewName)
    {
        QueryRunner queryRunner = getQueryRunner();
        TrinoFileSystem fileSystemFactory = ((IcebergConnector) queryRunner.getCoordinator().getConnector("iceberg")).getInjector()
                .getInstance(TrinoFileSystemFactory.class)
                .create(ConnectorIdentity.ofUser("test"));
        Location metadataLocation = Location.of(getStorageMetadataLocation(materializedViewName));
        return TableMetadataParser.read(new ForwardingFileIo(fileSystemFactory), metadataLocation.toString());
    }

    private long getLatestSnapshotId(String tableName)
    {
        return (long) computeScalar(format("SELECT snapshot_id FROM \"%s$snapshots\" ORDER BY committed_at DESC FETCH FIRST 1 ROW WITH TIES", tableName));
    }

    private void assertFreshness(String viewName, String expected)
    {
        assertThat((String) computeScalar("SELECT freshness FROM system.metadata.materialized_views WHERE catalog_name = CURRENT_CATALOG AND schema_name = CURRENT_SCHEMA AND name = '" + viewName + "'")).isEqualTo(expected);
    }

    public static class SequenceTableFunction
            extends AbstractConnectorTableFunction
    {
        public SequenceTableFunction()
        {
            super("system", "sequence_function", List.of(), GENERIC_TABLE);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments, ConnectorAccessControl accessControl)
        {
            return TableFunctionAnalysis.builder()
                    .handle(new SequenceTableFunctionHandle())
                    .returnedType(new Descriptor(ImmutableList.of(new Descriptor.Field("next_value", Optional.of(INTEGER)))))
                    .build();
        }
    }

    public static class SequenceTableFunctionHandle
            implements ConnectorTableFunctionHandle {}

    public static class SequenceTableFunctionProcessorProvider
            implements TableFunctionProcessorProvider
    {
        private final SequenceFunctionProcessor sequenceFunctionProcessor = new SequenceFunctionProcessor();

        @Override
        public TableFunctionSplitProcessor getSplitProcessor(ConnectorSession session, ConnectorTableFunctionHandle handle, ConnectorSplit split)
        {
            sequenceFunctionProcessor.reset();
            return sequenceFunctionProcessor;
        }
    }

    public static class SequenceFunctionProcessor
            implements TableFunctionSplitProcessor
    {
        private static final AtomicInteger generator = new AtomicInteger(10);
        private final AtomicBoolean finished = new AtomicBoolean(false);

        @Override
        public TableFunctionProcessorState process()
        {
            if (finished.get()) {
                return FINISHED;
            }
            BlockBuilder builder = INTEGER.createBlockBuilder(null, 1);
            INTEGER.writeInt(builder, generator.getAndIncrement());
            finished.set(true);
            return produced(new Page(builder.build()));
        }

        public void reset()
        {
            finished.set(false);
        }
    }

    public record SequenceConnectorSplit()
            implements ConnectorSplit
    {
        private static final int INSTANCE_SIZE = instanceSize(SequenceConnectorSplit.class);

        @Override
        public Map<String, String> getSplitInfo()
        {
            return ImmutableMap.of();
        }

        @JsonIgnore
        @Override
        public SplitWeight getSplitWeight()
        {
            return SplitWeight.standard();
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE;
        }
    }
}
