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
package io.trino.plugin.ducklake;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for the Ducklake connector.
 * Boots a full Trino server with the Ducklake plugin installed and executes
 * SQL queries end-to-end against DuckDB-generated test data.
 */
public class TestDucklakeIntegration
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DucklakeQueryRunner.builder().build();
    }

    // -- Metadata queries --

    @Test
    public void testShowSchemas()
    {
        assertQuerySucceeds("SHOW SCHEMAS");
        MaterializedResult result = computeActual("SHOW SCHEMAS");
        assertThat(result.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString()))
                .contains("test_schema");
    }

    @Test
    public void testShowTables()
    {
        MaterializedResult result = computeActual("SHOW TABLES FROM test_schema");
        assertThat(result.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString()))
                .contains("simple_table", "array_table", "partitioned_table",
                        "temporal_partitioned_table", "daily_partitioned_table", "nested_table");
    }

    @Test
    public void testDescribeSimpleTable()
    {
        MaterializedResult result = computeActual("DESCRIBE simple_table");
        assertThat(result.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString()))
                .contains("id", "name", "price", "active", "created_date");
    }

    // -- Basic reads --

    @Test
    public void testSelectSimpleTable()
    {
        MaterializedResult result = computeActual("SELECT * FROM simple_table");
        assertThat(result.getRowCount()).isEqualTo(5);
    }

    @Test
    public void testSelectCount()
    {
        assertQuery("SELECT count(*) FROM simple_table", "VALUES 5");
    }

    @Test
    public void testSelectWithPredicate()
    {
        MaterializedResult result = computeActual("SELECT * FROM simple_table WHERE price > 40.0");
        assertThat(result.getRowCount()).isEqualTo(2);
    }

    @Test
    public void testSelectWithAggregation()
    {
        assertQuerySucceeds("SELECT avg(price), min(price), max(price) FROM simple_table");
    }

    @Test
    public void testSelectWithGroupBy()
    {
        assertQuerySucceeds("SELECT active, count(*) FROM simple_table GROUP BY active");
    }

    @Test
    public void testSelectWithOrderByAndLimit()
    {
        MaterializedResult result = computeActual("SELECT id, name FROM simple_table ORDER BY id LIMIT 3");
        assertThat(result.getRowCount()).isEqualTo(3);
    }

    // -- Complex types --

    @Test
    public void testSelectStructColumn()
    {
        MaterializedResult result = computeActual("SELECT metadata FROM nested_table");
        assertThat(result.getRowCount()).isEqualTo(3);
    }

    @Test
    public void testSelectMapColumn()
    {
        MaterializedResult result = computeActual("SELECT tags FROM nested_table");
        assertThat(result.getRowCount()).isEqualTo(3);
    }

    @Test
    public void testSelectNestedArrayColumn()
    {
        MaterializedResult result = computeActual("SELECT nested_list FROM nested_table");
        assertThat(result.getRowCount()).isEqualTo(3);
    }

    @Test
    public void testSelectComplexStructColumn()
    {
        MaterializedResult result = computeActual("SELECT complex_struct FROM nested_table");
        assertThat(result.getRowCount()).isEqualTo(3);
    }

    // -- Partitioned tables --

    @Test
    public void testPartitionedTableFullScan()
    {
        assertQuery("SELECT count(*) FROM partitioned_table", "VALUES 5");
    }

    @Test
    public void testPartitionedTableWithPredicate()
    {
        MaterializedResult result = computeActual("SELECT * FROM partitioned_table WHERE region = 'US'");
        assertThat(result.getRowCount()).isEqualTo(2);
    }

    @Test
    public void testTemporalPartitionedTableRead()
    {
        assertQuery("SELECT count(*) FROM temporal_partitioned_table", "VALUES 6");
    }

    @Test
    public void testTemporalPartitionedTableWithPredicate()
    {
        MaterializedResult result = computeActual(
                "SELECT * FROM temporal_partitioned_table WHERE event_date = DATE '2023-06-10'");
        assertThat(result.getRowCount()).isGreaterThan(0);
    }

    @Test
    public void testDailyPartitionedTableRead()
    {
        assertQuery("SELECT count(*) FROM daily_partitioned_table", "VALUES 5");
    }

    // -- EXPLAIN --

    @Test
    public void testExplainSimpleSelect()
    {
        assertQuerySucceeds("EXPLAIN SELECT * FROM simple_table");
    }

    @Test
    public void testExplainWithPredicate()
    {
        assertQuerySucceeds("EXPLAIN SELECT * FROM simple_table WHERE price > 30.0");
    }

    // -- Joins (exercises dynamic filter path) --

    @Test
    public void testSelfJoin()
    {
        assertQuerySucceeds(
                "SELECT a.id, b.name FROM simple_table a JOIN simple_table b ON a.id = b.id");
    }

    @Test
    public void testCrossTableJoin()
    {
        assertQuerySucceeds(
                "SELECT s.name, p.region FROM simple_table s JOIN partitioned_table p ON s.id = p.id");
    }
}
