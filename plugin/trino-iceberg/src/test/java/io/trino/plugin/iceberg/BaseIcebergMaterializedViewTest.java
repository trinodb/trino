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

import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.metadata.MaterializedViewDefinition;
import io.trino.metadata.QualifiedObjectName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.sql.tree.ExplainType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedRow;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionManager;
import org.assertj.core.api.Condition;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.testing.MaterializedResult.DEFAULT_PRECISION;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.DELETE_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.DROP_MATERIALIZED_VIEW;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.INSERT_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.REFRESH_MATERIALIZED_VIEW;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.RENAME_MATERIALIZED_VIEW;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.UPDATE_TABLE;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.anyOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public abstract class BaseIcebergMaterializedViewTest
        extends AbstractTestQueryFramework
{
    protected final String storageSchemaName = "testing_storage_schema_" + randomNameSuffix();

    protected abstract String getSchemaDirectory();

    @BeforeClass
    public void setUp()
    {
        assertUpdate("CREATE TABLE base_table1(_bigint BIGINT, _date DATE) WITH (partitioning = ARRAY['_date'])");
        assertUpdate("INSERT INTO base_table1 VALUES (0, DATE '2019-09-08'), (1, DATE '2019-09-09'), (2, DATE '2019-09-09')", 3);
        assertUpdate("INSERT INTO base_table1 VALUES (3, DATE '2019-09-09'), (4, DATE '2019-09-10'), (5, DATE '2019-09-10')", 3);
        assertQuery("SELECT count(*) FROM base_table1", "VALUES 6");

        assertUpdate("CREATE TABLE base_table2 (_varchar VARCHAR, _bigint BIGINT, _date DATE) WITH (partitioning = ARRAY['_bigint', '_date'])");
        assertUpdate("INSERT INTO base_table2 VALUES ('a', 0, DATE '2019-09-08'), ('a', 1, DATE '2019-09-08'), ('a', 0, DATE '2019-09-09')", 3);
        assertQuery("SELECT count(*) FROM base_table2", "VALUES 3");
    }

    @Test
    public void testShowTables()
    {
        String schema = getSession().getSchema().orElseThrow();
        assertUpdate("CREATE MATERIALIZED VIEW materialized_view_show_tables_test AS SELECT * FROM base_table1");
        SchemaTableName storageTableName = getStorageTable("iceberg", schema, "materialized_view_show_tables_test");

        Set<String> expectedTables = ImmutableSet.of("base_table1", "base_table2", "materialized_view_show_tables_test", storageTableName.getTableName());
        Set<String> actualTables = computeActual("SHOW TABLES").getOnlyColumnAsSet().stream()
                .map(String.class::cast)
                .collect(toImmutableSet());
        // containsAll rather than isEqualTo as the test is not singleThreaded
        assertThat(actualTables).containsAll(expectedTables);

        assertUpdate("DROP MATERIALIZED VIEW materialized_view_show_tables_test");
    }

    @Test
    public void testMaterializedViewsMetadata()
    {
        String catalogName = getSession().getCatalog().orElseThrow();
        String schemaName = getSession().getSchema().orElseThrow();
        String materializedViewName = format("test_materialized_view_%s", randomNameSuffix());

        computeActual("CREATE TABLE small_region AS SELECT * FROM tpch.tiny.region LIMIT 1");
        computeActual(format("CREATE MATERIALIZED VIEW %s AS SELECT * FROM small_region LIMIT 1", materializedViewName));

        // test storage table name
        assertQuery(
                format(
                        "SELECT storage_catalog, storage_schema, CONCAT(storage_schema, '.', storage_table)" +
                                "FROM system.metadata.materialized_views WHERE schema_name = '%s' AND name = '%s'",
                        // TODO (https://github.com/trinodb/trino/issues/9039) remove redundant schema_name filter
                        schemaName,
                        materializedViewName),
                format(
                        "VALUES ('%s', '%s', '%s')",
                        catalogName,
                        schemaName,
                        getStorageTable(catalogName, schemaName, materializedViewName)));

        // test freshness update
        assertQuery(
                // TODO (https://github.com/trinodb/trino/issues/9039) remove redundant schema_name filter
                format("SELECT freshness FROM system.metadata.materialized_views WHERE schema_name = '%s' AND name = '%s'", schemaName, materializedViewName),
                "VALUES 'STALE'");

        computeActual(format("REFRESH MATERIALIZED VIEW %s", materializedViewName));

        assertQuery(
                // TODO (https://github.com/trinodb/trino/issues/9039) remove redundant schema_name filter
                format("SELECT freshness FROM system.metadata.materialized_views WHERE schema_name = '%s' AND name = '%s'", schemaName, materializedViewName),
                "VALUES 'FRESH'");

        assertUpdate("DROP TABLE small_region");
        assertUpdate(format("DROP MATERIALIZED VIEW %s", materializedViewName));
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

        assertUpdate("CREATE MATERIALIZED VIEW materialized_view_with_property " +
                "WITH (\n" +
                "   partitioning = ARRAY['_date'],\n" +
                "   orc_bloom_filter_columns = ARRAY['_date'],\n" +
                "   orc_bloom_filter_fpp = 0.1) AS " +
                "SELECT _bigint, _date FROM base_table1");
        assertQuery("SELECT COUNT(*) FROM materialized_view_with_property", "VALUES 6");

        assertThat((String) computeScalar("SHOW CREATE MATERIALIZED VIEW materialized_view_with_property"))
                .matches(
                        "\\QCREATE MATERIALIZED VIEW iceberg." + schema + ".materialized_view_with_property\n" +
                                "WITH (\n" +
                                "   format = 'ORC',\n" +
                                "   format_version = 2,\n" +
                                "   location = '" + getSchemaDirectory() + "/st_\\E[0-9a-f]+-[0-9a-f]+\\Q',\n" +
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
        assertUpdate("DROP MATERIALIZED VIEW materialized_view_with_property");
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
    public void testRefreshAllowedWithRestrictedStorageTable()
    {
        String schema = getSession().getSchema().orElseThrow();
        assertUpdate("CREATE MATERIALIZED VIEW materialized_view_refresh AS SELECT * FROM base_table1");
        SchemaTableName storageTable = getStorageTable("iceberg", schema, "materialized_view_refresh");

        assertAccessAllowed(
                "REFRESH MATERIALIZED VIEW materialized_view_refresh",
                privilege(storageTable.getTableName(), INSERT_TABLE),
                privilege(storageTable.getTableName(), DELETE_TABLE),
                privilege(storageTable.getTableName(), UPDATE_TABLE),
                privilege(storageTable.getTableName(), SELECT_COLUMN));

        assertUpdate("DROP MATERIALIZED VIEW materialized_view_refresh");
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
        assertEquals(computeActual("SELECT * FROM materialized_view_no_part").getRowCount(), 6);
        assertThat(getExplainPlan("SELECT * FROM materialized_view_no_part", ExplainType.Type.IO))
                .contains("base_table1");
        assertUpdate("REFRESH MATERIALIZED VIEW materialized_view_no_part", 6);
        assertEquals(computeActual("SELECT * FROM materialized_view_no_part").getRowCount(), 6);
        assertThat(getExplainPlan("SELECT * FROM materialized_view_no_part", ExplainType.Type.IO)).doesNotContain("base_table1");

        assertEquals(computeActual("SELECT * FROM materialized_view_agg").getRowCount(), 3);
        assertThat(getExplainPlan("SELECT * FROM materialized_view_agg", ExplainType.Type.IO))
                .contains("base_table1");
        assertUpdate("REFRESH MATERIALIZED VIEW materialized_view_agg", 3);
        assertEquals(computeActual("SELECT * FROM materialized_view_agg").getRowCount(), 3);
        assertThat(getExplainPlan("SELECT * FROM materialized_view_agg", ExplainType.Type.IO))
                .doesNotContain("base_table1");
        assertQuery(session, "SELECT * FROM materialized_view_agg", "VALUES (DATE '2019-09-10', 2)," +
                "(DATE '2019-09-08', 1), (DATE '2019-09-09', 3)");

        assertEquals(computeActual("SELECT * FROM materialized_view_part").getRowCount(), 3);
        assertThat(getExplainPlan("SELECT * FROM materialized_view_part", ExplainType.Type.IO))
                .contains("base_table1");
        assertUpdate("REFRESH MATERIALIZED VIEW materialized_view_part", 3);
        assertEquals(computeActual("SELECT * FROM materialized_view_part").getRowCount(), 3);
        assertThat(getExplainPlan("SELECT * FROM materialized_view_part", ExplainType.Type.IO)).doesNotContain("base_table1");

        assertEquals(computeActual("SELECT * FROM materialized_view_join").getRowCount(), 5);
        assertThat(getExplainPlan("SELECT * FROM materialized_view_join", ExplainType.Type.IO)).contains("base_table1", "base_table2");
        assertUpdate("REFRESH MATERIALIZED VIEW materialized_view_join", 5);
        assertEquals(computeActual("SELECT * FROM materialized_view_join").getRowCount(), 5);
        assertThat(getExplainPlan("SELECT * FROM materialized_view_join", ExplainType.Type.IO)).doesNotContain("base_table1", "base_table2");

        assertEquals(computeActual("SELECT * FROM materialized_view_join_part").getRowCount(), 4);
        assertThat(getExplainPlan("SELECT * FROM materialized_view_join_part", ExplainType.Type.IO)).contains("base_table1", "base_table2");
        assertUpdate("REFRESH MATERIALIZED VIEW materialized_view_join_part", 4);
        assertEquals(computeActual("SELECT * FROM materialized_view_join_part").getRowCount(), 4);
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
                .contains("base_table3");

        Condition<String> containsTable3 = new Condition<>(p -> p.contains("base_table3"), "base_table3");
        Condition<String> containsTable4 = new Condition<>(p -> p.contains("base_table4"), "base_table4");
        assertThat(getExplainPlan("SELECT * FROM materialized_view_join_stale", ExplainType.Type.IO))
                .is(anyOf(containsTable3, containsTable4));

        assertThat(getExplainPlan("SELECT * FROM materialized_view_join_part_stale", ExplainType.Type.IO))
                .is(anyOf(containsTable3, containsTable4));

        assertUpdate("REFRESH MATERIALIZED VIEW materialized_view_part_stale", 3);
        assertUpdate("REFRESH MATERIALIZED VIEW materialized_view_join_stale", 5);
        assertUpdate("REFRESH MATERIALIZED VIEW materialized_view_join_part_stale", 4);

        assertThat(getExplainPlan("SELECT * FROM materialized_view_part_stale", ExplainType.Type.IO))
                .doesNotContain("base_table3");

        assertThat(getExplainPlan("SELECT * FROM materialized_view_join_stale", ExplainType.Type.IO))
                .doesNotContain("base_table3", "base_table4");

        assertThat(getExplainPlan("SELECT * FROM materialized_view_join_part_stale", ExplainType.Type.IO))
                .doesNotContain("base_table3", "base_table4");

        assertUpdate("DROP TABLE IF EXISTS base_table3");
        assertUpdate("DROP TABLE IF EXISTS base_table4");
        assertUpdate("DROP MATERIALIZED VIEW materialized_view_part_stale");
        assertUpdate("DROP MATERIALIZED VIEW materialized_view_join_stale");
        assertUpdate("DROP MATERIALIZED VIEW materialized_view_join_part_stale");
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
                        "   format = 'ORC',\n" +
                        "   format_version = 2,\n" +
                        "   location = '" + getSchemaDirectory() + "/st_\\E[0-9a-f]+-[0-9a-f]+\\Q',\n" +
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
        assertThatThrownBy(() -> query("CREATE OR REPLACE MATERIALIZED VIEW test_create_materialized_view_when_table_exists AS SELECT sum(1) AS num_rows FROM base_table2"))
                .hasMessage("Existing table is not a Materialized View: " + schema + ".test_create_materialized_view_when_table_exists");
        assertThatThrownBy(() -> query("CREATE MATERIALIZED VIEW IF NOT EXISTS test_create_materialized_view_when_table_exists AS SELECT sum(1) AS num_rows FROM base_table2"))
                .hasMessage("Existing table is not a Materialized View: " + schema + ".test_create_materialized_view_when_table_exists");
        assertUpdate("DROP TABLE test_create_materialized_view_when_table_exists");
    }

    @Test
    public void testDropMaterializedViewCannotDropTable()
    {
        String schema = getSession().getSchema().orElseThrow();
        assertUpdate("CREATE TABLE test_drop_materialized_view_cannot_drop_table (a INT, b INT)");
        assertThatThrownBy(() -> query("DROP MATERIALIZED VIEW test_drop_materialized_view_cannot_drop_table"))
                .hasMessageContaining("Materialized view 'iceberg." + schema + ".test_drop_materialized_view_cannot_drop_table' does not exist, but a table with that name exists");
        assertUpdate("DROP TABLE test_drop_materialized_view_cannot_drop_table");
    }

    @Test
    public void testRenameMaterializedViewCannotRenameTable()
    {
        String schema = getSession().getSchema().orElseThrow();
        assertUpdate("CREATE TABLE test_rename_materialized_view_cannot_rename_table (a INT, b INT)");
        assertThatThrownBy(() -> query("ALTER MATERIALIZED VIEW test_rename_materialized_view_cannot_rename_table RENAME TO new_materialized_view_name"))
                .hasMessageContaining("Materialized View 'iceberg." + schema + ".test_rename_materialized_view_cannot_rename_table' does not exist, but a table with that name exists");
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

        assertUpdate("DROP TABLE IF EXISTS base_table5");
        assertUpdate("DROP MATERIALIZED VIEW materialized_view_level1");
        assertUpdate("DROP MATERIALIZED VIEW materialized_view_level2");
    }

    @Test
    public void testStorageSchemaProperty()
    {
        String catalogName = getSession().getCatalog().orElseThrow();
        String schemaName = getSession().getSchema().orElseThrow();
        String viewName = "storage_schema_property_test";
        assertUpdate("CREATE SCHEMA IF NOT EXISTS " + catalogName + "." + storageSchemaName);
        assertUpdate(
                "CREATE MATERIALIZED VIEW " + viewName + " " +
                        "WITH (storage_schema = '" + storageSchemaName + "') AS " +
                        "SELECT * FROM base_table1");
        SchemaTableName storageTable = getStorageTable(catalogName, schemaName, viewName);
        assertThat(storageTable.getSchemaName()).isEqualTo(storageSchemaName);

        assertUpdate("REFRESH MATERIALIZED VIEW " + viewName, 6);
        assertThat(computeActual("SELECT * FROM " + viewName).getRowCount()).isEqualTo(6);
        assertThat(getExplainPlan("SELECT * FROM " + viewName, ExplainType.Type.IO))
                .doesNotContain("base_table1")
                .contains(storageSchemaName);

        assertThat((String) computeScalar("SHOW CREATE MATERIALIZED VIEW " + viewName))
                .contains("storage_schema = '" + storageSchemaName + "'");

        Set<String> storageSchemaTables = computeActual("SHOW TABLES IN " + storageSchemaName).getOnlyColumnAsSet().stream()
                .map(String.class::cast)
                .collect(toImmutableSet());
        assertThat(storageSchemaTables).contains(storageTable.getTableName());

        assertUpdate("DROP MATERIALIZED VIEW " + viewName);
        storageSchemaTables = computeActual("SHOW TABLES IN " + storageSchemaName).getOnlyColumnAsSet().stream()
                .map(String.class::cast)
                .collect(toImmutableSet());
        assertThat(storageSchemaTables).doesNotContain(storageTable.getTableName());

        assertThatThrownBy(() -> query(
                "CREATE MATERIALIZED VIEW " + viewName + " " +
                        "WITH (storage_schema = 'non_existent') AS " +
                        "SELECT * FROM base_table1"))
                .hasMessageContaining("non_existent not found");
        assertThatThrownBy(() -> query("DESCRIBE " + viewName))
                .hasMessageContaining(format("'iceberg.%s.%s' does not exist", schemaName, viewName));
    }

    private SchemaTableName getStorageTable(String catalogName, String schemaName, String objectName)
    {
        TransactionManager transactionManager = getQueryRunner().getTransactionManager();
        TransactionId transactionId = transactionManager.beginTransaction(false);
        Session session = getSession().beginTransactionId(transactionId, transactionManager, getQueryRunner().getAccessControl());
        Optional<MaterializedViewDefinition> materializedView = getQueryRunner().getMetadata()
                .getMaterializedView(session, new QualifiedObjectName(catalogName, schemaName, objectName));
        assertThat(materializedView).isPresent();
        return materializedView.get().getStorageTable().get().getSchemaTableName();
    }
}
