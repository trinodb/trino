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
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.tree.ExplainType.Type.LOGICAL;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergAggregationPushdown
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder().build();
    }

    private Session withoutAggregationPushdown()
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", "aggregation_pushdown_enabled", "false")
                .build();
    }

    private void assertAggregationPushedDown(@Language("SQL") String sql)
    {
        // hasPlan also verifies that results match those produced with pushdown disabled
        assertThat(query(sql))
                .hasPlan(output(project(node(TableScanNode.class))));
    }

    private void assertAggregationNotPushedDown(@Language("SQL") String sql)
    {
        assertThat(query(sql))
                .matches(withoutAggregationPushdown(), sql);
        assertThat(getExplainPlan(sql, LOGICAL)).contains("Aggregate");
    }

    @Test
    void testGlobalAggregation()
    {
        String table = "test_aggregation_pushdown_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (c_int integer, c_bigint bigint, c_date date, c_timestamp timestamp(6), c_timestamptz timestamp(6) with time zone, c_decimal decimal(20, 4), c_boolean boolean, c_varchar varchar)");
        assertUpdate("INSERT INTO " + table + " VALUES " +
                "(1, 100, DATE '2024-01-01', TIMESTAMP '2024-01-01 01:02:03.123456', TIMESTAMP '2024-01-01 01:02:03.123456 UTC', DECIMAL '12345678901234.5678', true, 'alice')," +
                "(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)", 2);
        assertUpdate("INSERT INTO " + table + " VALUES " +
                "(-5, 700, DATE '2023-06-15', TIMESTAMP '2023-06-15 23:59:59.999999', TIMESTAMP '2023-06-15 23:59:59.999999 UTC', DECIMAL '-2.0001', false, 'bob')", 1);

        assertAggregationPushedDown("SELECT count(*) FROM " + table);
        assertAggregationPushedDown("SELECT count(c_int), min(c_int), max(c_int) FROM " + table);
        assertAggregationPushedDown("SELECT min(c_bigint), max(c_bigint), min(c_date), max(c_date) FROM " + table);
        assertAggregationPushedDown("SELECT min(c_timestamp), max(c_timestamp), min(c_timestamptz), max(c_timestamptz) FROM " + table);
        assertAggregationPushedDown("SELECT min(c_decimal), max(c_decimal), min(c_boolean), max(c_boolean) FROM " + table);
        // count on varchar needs only null counts, which are exact regardless of bounds truncation
        assertAggregationPushedDown("SELECT count(c_varchar) FROM " + table);
        assertAggregationPushedDown("SELECT count(*), count(c_int), min(c_date), max(c_bigint) FROM " + table);

        assertThat(query("SELECT count(*), count(c_int), min(c_int), max(c_bigint) FROM " + table))
                .matches("VALUES (BIGINT '3', BIGINT '2', -5, BIGINT '700')");

        assertUpdate("DROP TABLE " + table);
    }

    @Test
    void testEmptyTable()
    {
        String table = "test_aggregation_pushdown_empty_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (c_int integer, c_varchar varchar)");

        assertAggregationPushedDown("SELECT count(*), count(c_int), min(c_int), max(c_int) FROM " + table);
        assertThat(query("SELECT count(*), min(c_int) FROM " + table))
                .matches("VALUES (BIGINT '0', CAST(NULL AS integer))");

        assertUpdate("DROP TABLE " + table);
    }

    @Test
    void testUnsupportedMinMaxTypes()
    {
        String table = "test_aggregation_pushdown_types_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (c_varchar varchar, c_varbinary varbinary, c_double double, c_real real, c_uuid uuid)");
        assertUpdate("INSERT INTO " + table + " VALUES ('alice', X'1234', 1.5, REAL '2.5', UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59')", 1);
        assertUpdate("INSERT INTO " + table + " VALUES ('bob', X'ff', nan(), REAL '-1.0', UUID '00000000-0000-0000-0000-000000000000')", 1);

        // Bounds may be truncated for varchar/varbinary, and NaN ordering differs for double/real
        assertAggregationNotPushedDown("SELECT min(c_varchar), max(c_varchar) FROM " + table);
        assertAggregationNotPushedDown("SELECT min(c_varbinary) FROM " + table);
        assertAggregationNotPushedDown("SELECT min(c_double), max(c_double) FROM " + table);
        assertAggregationNotPushedDown("SELECT min(c_real) FROM " + table);
        assertAggregationNotPushedDown("SELECT min(c_uuid) FROM " + table);
        // Trino's min/max use unordered comparison, so NaN loses to any real value
        assertThat(query("SELECT max(c_double) FROM " + table))
                .matches("VALUES 1.5e0");
        // count is supported for these types
        assertAggregationPushedDown("SELECT count(c_varchar), count(c_varbinary), count(c_double), count(c_real) FROM " + table);

        assertUpdate("DROP TABLE " + table);
    }

    @Test
    void testUnsupportedAggregations()
    {
        String table = "test_aggregation_pushdown_unsupported_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (c_int integer, c_part integer) WITH (partitioning = ARRAY['c_part'])");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 1), (3, 2)", 3);

        assertAggregationNotPushedDown("SELECT sum(c_int) FROM " + table);
        assertAggregationNotPushedDown("SELECT avg(c_int) FROM " + table);
        assertAggregationNotPushedDown("SELECT count(DISTINCT c_int) FROM " + table);
        assertAggregationNotPushedDown("SELECT count(c_int) FILTER (WHERE c_part = 1) FROM " + table);
        assertAggregationNotPushedDown("SELECT c_part, count(*) FROM " + table + " GROUP BY c_part");
        // one unsupported aggregation prevents pushdown of the whole aggregation node
        assertAggregationNotPushedDown("SELECT count(*), sum(c_int) FROM " + table);

        assertUpdate("DROP TABLE " + table);
    }

    @Test
    void testPartitionFilter()
    {
        String table = "test_aggregation_pushdown_partition_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (c_int integer, c_part integer) WITH (partitioning = ARRAY['c_part'])");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 1), (30, 2), (NULL, 2)", 4);

        // Filter on an identity partition column is enforced by the connector, so pushdown applies
        assertAggregationPushedDown("SELECT count(*), min(c_int), max(c_int) FROM " + table + " WHERE c_part = 1");
        assertThat(query("SELECT count(*), max(c_int) FROM " + table + " WHERE c_part = 2"))
                .matches("VALUES (BIGINT '2', 30)");

        // Filter on a non-partition column leaves a filter above the scan, so pushdown does not apply
        assertAggregationNotPushedDown("SELECT count(*) FROM " + table + " WHERE c_int > 1");

        assertUpdate("DROP TABLE " + table);
    }

    @Test
    void testDeletes()
    {
        String table = "test_aggregation_pushdown_deletes_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (c_int integer, c_part integer) WITH (partitioning = ARRAY['c_part'])");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 1), (30, 2), (40, 2)", 4);

        // Deleting a whole partition drops entire files and produces no delete files, so pushdown still applies
        assertUpdate("DELETE FROM " + table + " WHERE c_part = 2", 2);
        assertAggregationPushedDown("SELECT count(*), min(c_int), max(c_int) FROM " + table);
        assertThat(query("SELECT count(*), max(c_int) FROM " + table))
                .matches("VALUES (BIGINT '2', 2)");

        // Row-level deletes produce delete files, which make file statistics unusable
        assertUpdate("DELETE FROM " + table + " WHERE c_int = 1", 1);
        assertAggregationNotPushedDown("SELECT count(*), min(c_int), max(c_int) FROM " + table);
        assertThat(query("SELECT count(*), min(c_int) FROM " + table))
                .matches("VALUES (BIGINT '1', 2)");

        assertUpdate("DROP TABLE " + table);
    }

    @Test
    void testSchemaEvolution()
    {
        String table = "test_aggregation_pushdown_evolution_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (c_int integer)");
        assertUpdate("INSERT INTO " + table + " VALUES (1), (2)", 2);
        assertUpdate("ALTER TABLE " + table + " ADD COLUMN c_new bigint");

        // Files written before the column was added have no statistics for it, so pushdown falls back gracefully
        assertAggregationNotPushedDown("SELECT min(c_new), max(c_new) FROM " + table);
        assertThat(query("SELECT count(*), min(c_new) FROM " + table))
                .matches("VALUES (BIGINT '2', CAST(NULL AS bigint))");

        assertUpdate("INSERT INTO " + table + " VALUES (3, 300)", 1);
        assertThat(query("SELECT max(c_new) FROM " + table))
                .matches("VALUES BIGINT '300'");

        assertUpdate("DROP TABLE " + table);
    }

    @Test
    void testTimeTravel()
    {
        String table = "test_aggregation_pushdown_time_travel_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (c_int integer)");
        assertUpdate("INSERT INTO " + table + " VALUES (1), (2)", 2);
        long snapshotId = (long) computeScalar("SELECT snapshot_id FROM \"" + table + "$snapshots\" ORDER BY committed_at DESC LIMIT 1");
        assertUpdate("INSERT INTO " + table + " VALUES (30)", 1);

        assertAggregationPushedDown("SELECT count(*), max(c_int) FROM " + table + " FOR VERSION AS OF " + snapshotId);
        assertThat(query("SELECT count(*), max(c_int) FROM " + table + " FOR VERSION AS OF " + snapshotId))
                .matches("VALUES (BIGINT '2', 2)");
        assertThat(query("SELECT count(*), max(c_int) FROM " + table))
                .matches("VALUES (BIGINT '3', 30)");

        assertUpdate("DROP TABLE " + table);
    }

    @Test
    void testSessionPropertyDisabled()
    {
        String table = "test_aggregation_pushdown_disabled_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (c_int integer)");
        assertUpdate("INSERT INTO " + table + " VALUES (1), (2)", 2);

        assertThat(query(withoutAggregationPushdown(), "SELECT count(*), min(c_int) FROM " + table))
                .isNotFullyPushedDown(AggregationNode.class)
                .matches("VALUES (BIGINT '2', 1)");

        assertUpdate("DROP TABLE " + table);
    }

    @Test
    void testOrcTimestampBoundsNotUsed()
    {
        String table = "test_aggregation_pushdown_orc_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (c_int integer, c_timestamp timestamp(6)) WITH (format = 'ORC')");
        assertUpdate("INSERT INTO " + table + " VALUES (1, TIMESTAMP '2021-01-01 00:00:00.111111'), (2, TIMESTAMP '2021-01-01 00:00:00.222222')", 2);

        // ORC file statistics truncate timestamp bounds to millisecond precision, so they are not exact
        assertAggregationNotPushedDown("SELECT min(c_timestamp), max(c_timestamp) FROM " + table);
        assertThat(query("SELECT min(c_timestamp) FROM " + table))
                .matches("VALUES TIMESTAMP '2021-01-01 00:00:00.111111'");

        // ORC bounds for other supported types are exact
        assertAggregationPushedDown("SELECT count(*), count(c_timestamp), min(c_int), max(c_int) FROM " + table);

        assertUpdate("DROP TABLE " + table);
    }

    @Test
    void testNestedFieldsNotPushedDown()
    {
        String table = "test_aggregation_pushdown_nested_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " (c_row row(x integer, y integer))");
        assertUpdate("INSERT INTO " + table + " VALUES ROW(ROW(1, 2)), ROW(ROW(3, 4))", 2);

        assertAggregationNotPushedDown("SELECT min(c_row.x), max(c_row.y) FROM " + table);
        assertAggregationNotPushedDown("SELECT count(c_row) FROM " + table);

        assertUpdate("DROP TABLE " + table);
    }
}
