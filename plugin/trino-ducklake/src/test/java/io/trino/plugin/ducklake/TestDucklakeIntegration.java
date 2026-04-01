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
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for the Ducklake connector.
 * Boots a full Trino server with the Ducklake plugin installed and executes
 * SQL queries end-to-end against DuckDB-generated test data.
 *
 * Test tables (created by DucklakeCatalogGenerator):
 *   simple_table (5 rows)              — id, name, price, active, created_date
 *   array_table (5 rows)               — id, product_name, tags[], quantity
 *   partitioned_table (5 rows)         — id, name, region, amount (partitioned by region)
 *   temporal_partitioned_table (6 rows) — id, event_name, event_date, amount (partitioned by year/month)
 *   daily_partitioned_table (5 rows)   — id, event_name, event_date, amount (partitioned by year/month/day)
 *   nested_table (3 rows)              — id, metadata(struct), tags(map), nested_list, complex_struct
 *   wide_types_table (3 rows)          — tinyint, smallint, int, bigint, float, double, decimal, bool, varchar, date, timestamp, blob
 *   nullable_table (4 rows)            — all column types with NULLs
 *   empty_table (0 rows)              — empty result set testing
 *   schema_evolution_table (4 rows)    — column added after initial write
 *   aggregation_table (30 rows)        — category A/B/C, amount, quantity
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

    // ==================== Metadata queries ====================

    @Test
    public void testShowSchemas()
    {
        MaterializedResult result = computeActual("SHOW SCHEMAS");
        assertThat(result.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString()))
                .contains("test_schema", "information_schema");
    }

    @Test
    public void testShowTables()
    {
        MaterializedResult result = computeActual("SHOW TABLES FROM test_schema");
        List<String> tableNames = result.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString())
                .toList();
        assertThat(tableNames)
                .contains("simple_table", "array_table", "partitioned_table",
                        "temporal_partitioned_table", "daily_partitioned_table", "nested_table",
                        "wide_types_table", "nullable_table", "empty_table",
                        "schema_evolution_table", "aggregation_table");
    }

    @Test
    public void testDescribeSimpleTable()
    {
        MaterializedResult result = computeActual("DESCRIBE simple_table");
        assertThat(result.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString()))
                .containsExactly("id", "name", "price", "active", "created_date");
    }

    @Test
    public void testDescribeWideTypesTable()
    {
        MaterializedResult result = computeActual("DESCRIBE wide_types_table");
        assertThat(result.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString()))
                .containsExactly("col_tinyint", "col_smallint", "col_integer", "col_bigint",
                        "col_float", "col_double", "col_decimal", "col_boolean",
                        "col_varchar", "col_date", "col_timestamp", "col_blob");
    }

    @Test
    public void testDescribeNestedTable()
    {
        MaterializedResult result = computeActual("DESCRIBE nested_table");
        List<String> columns = result.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString())
                .toList();
        assertThat(columns).containsExactly("id", "metadata", "tags", "nested_list", "complex_struct");

        // Verify the type strings for complex columns
        List<String> types = result.getMaterializedRows().stream()
                .map(row -> row.getField(1).toString())
                .toList();
        assertThat(types.get(1)).contains("row");   // metadata is a struct -> row type
        assertThat(types.get(2)).contains("map");    // tags is a map
        assertThat(types.get(3)).contains("array");  // nested_list is array of arrays
    }

    // ==================== information_schema ====================

    @Test
    public void testInformationSchemaTables()
    {
        MaterializedResult result = computeActual(
                "SELECT table_name FROM information_schema.tables " +
                        "WHERE table_schema = 'test_schema' ORDER BY table_name");
        List<String> tables = result.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString())
                .toList();
        assertThat(tables).contains("simple_table", "aggregation_table", "empty_table");
    }

    @Test
    public void testInformationSchemaColumns()
    {
        MaterializedResult result = computeActual(
                "SELECT column_name, data_type FROM information_schema.columns " +
                        "WHERE table_schema = 'test_schema' AND table_name = 'simple_table' " +
                        "ORDER BY ordinal_position");
        assertThat(result.getRowCount()).isEqualTo(5);

        List<String> columnNames = result.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString())
                .toList();
        assertThat(columnNames).containsExactly("id", "name", "price", "active", "created_date");

        List<String> dataTypes = result.getMaterializedRows().stream()
                .map(row -> row.getField(1).toString())
                .toList();
        assertThat(dataTypes).containsExactly("integer", "varchar", "double", "boolean", "date");
    }

    @Test
    public void testInformationSchemaColumnsWideTypes()
    {
        MaterializedResult result = computeActual(
                "SELECT column_name, data_type FROM information_schema.columns " +
                        "WHERE table_schema = 'test_schema' AND table_name = 'wide_types_table' " +
                        "ORDER BY ordinal_position");
        assertThat(result.getRowCount()).isEqualTo(12);

        List<String> dataTypes = result.getMaterializedRows().stream()
                .map(row -> row.getField(1).toString())
                .toList();
        // tinyint, smallint, integer, bigint, real, double, decimal(10,2), boolean, varchar, date, timestamp(6), varbinary
        assertThat(dataTypes.get(0)).isEqualTo("tinyint");
        assertThat(dataTypes.get(1)).isEqualTo("smallint");
        assertThat(dataTypes.get(2)).isEqualTo("integer");
        assertThat(dataTypes.get(3)).isEqualTo("bigint");
        assertThat(dataTypes.get(4)).isEqualTo("real");
        assertThat(dataTypes.get(5)).isEqualTo("double");
        assertThat(dataTypes.get(6)).containsIgnoringCase("decimal");
        assertThat(dataTypes.get(7)).isEqualTo("boolean");
        assertThat(dataTypes.get(8)).isEqualTo("varchar");
        assertThat(dataTypes.get(9)).isEqualTo("date");
        assertThat(dataTypes.get(10)).containsIgnoringCase("timestamp");
        assertThat(dataTypes.get(11)).isEqualTo("varbinary");
    }

    @Test
    public void testInformationSchemaSchemata()
    {
        MaterializedResult result = computeActual(
                "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name");
        assertThat(result.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString()))
                .contains("test_schema", "information_schema");
    }

    // ==================== Basic reads ====================

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
    public void testSelectWithEqualityPredicate()
    {
        MaterializedResult result = computeActual("SELECT name FROM simple_table WHERE id = 3");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("Product C");
    }

    @Test
    public void testSelectWithBetweenPredicate()
    {
        MaterializedResult result = computeActual(
                "SELECT count(*) FROM simple_table WHERE price BETWEEN 20.0 AND 50.0");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(3L);
    }

    @Test
    public void testSelectWithInPredicate()
    {
        MaterializedResult result = computeActual(
                "SELECT count(*) FROM simple_table WHERE id IN (1, 3, 5)");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(3L);
    }

    @Test
    public void testSelectWithLikePredicate()
    {
        MaterializedResult result = computeActual(
                "SELECT count(*) FROM simple_table WHERE name LIKE 'Product %'");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(5L);
    }

    @Test
    public void testSelectWithBooleanPredicate()
    {
        MaterializedResult result = computeActual(
                "SELECT count(*) FROM simple_table WHERE active = true");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(3L);
    }

    @Test
    public void testSelectWithDatePredicate()
    {
        // Dates: 2024-01-15, 2024-02-20, 2024-03-10, 2024-01-05, 2024-02-28
        // Three are > 2024-02-01: 2024-02-20, 2024-03-10, 2024-02-28
        MaterializedResult result = computeActual(
                "SELECT count(*) FROM simple_table WHERE created_date > DATE '2024-02-01'");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(3L);
    }

    @Test
    public void testSelectWithAggregation()
    {
        MaterializedResult result = computeActual(
                "SELECT min(price), max(price), avg(price) FROM simple_table");
        assertThat(result.getRowCount()).isEqualTo(1);
        MaterializedRow row = result.getMaterializedRows().get(0);
        assertThat((Double) row.getField(0)).isEqualTo(19.99);
        assertThat((Double) row.getField(1)).isEqualTo(59.99);
    }

    @Test
    public void testSelectWithGroupBy()
    {
        MaterializedResult result = computeActual(
                "SELECT active, count(*) FROM simple_table GROUP BY active ORDER BY active");
        assertThat(result.getRowCount()).isEqualTo(2);
    }

    @Test
    public void testSelectWithOrderByAndLimit()
    {
        MaterializedResult result = computeActual(
                "SELECT id, name FROM simple_table ORDER BY id LIMIT 3");
        assertThat(result.getRowCount()).isEqualTo(3);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(2).getField(0)).isEqualTo(3);
    }

    @Test
    public void testSelectWithOffset()
    {
        MaterializedResult result = computeActual(
                "SELECT id FROM simple_table ORDER BY id OFFSET 3");
        assertThat(result.getRowCount()).isEqualTo(2);
    }

    @Test
    public void testSelectDistinct()
    {
        MaterializedResult result = computeActual(
                "SELECT DISTINCT active FROM simple_table ORDER BY active");
        assertThat(result.getRowCount()).isEqualTo(2);
    }

    // ==================== Wide types table ====================

    @Test
    public void testWideTypesTableRead()
    {
        assertQuery("SELECT count(*) FROM wide_types_table", "VALUES 3");
    }

    @Test
    public void testWideTypesIntegerTypes()
    {
        MaterializedResult result = computeActual(
                "SELECT col_tinyint, col_smallint, col_integer, col_bigint " +
                        "FROM wide_types_table ORDER BY col_integer");
        assertThat(result.getRowCount()).isEqualTo(3);
        // Negative row
        MaterializedRow negativeRow = result.getMaterializedRows().get(0);
        assertThat(((Number) negativeRow.getField(0)).byteValue()).isEqualTo((byte) -1);
        assertThat(((Number) negativeRow.getField(1)).shortValue()).isEqualTo((short) -100);
        assertThat(negativeRow.getField(2)).isEqualTo(-10000);
        assertThat(negativeRow.getField(3)).isEqualTo(-1000000000L);
    }

    @Test
    public void testWideTypesFloatingPoint()
    {
        MaterializedResult result = computeActual(
                "SELECT col_float, col_double, col_decimal " +
                        "FROM wide_types_table WHERE col_integer = 10000");
        assertThat(result.getRowCount()).isEqualTo(1);
    }

    @Test
    public void testWideTypesBooleanFilter()
    {
        MaterializedResult result = computeActual(
                "SELECT count(*) FROM wide_types_table WHERE col_boolean = true");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(2L);
    }

    @Test
    public void testWideTypesDateFilter()
    {
        MaterializedResult result = computeActual(
                "SELECT col_varchar FROM wide_types_table WHERE col_date = DATE '2024-01-01'");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("hello");
    }

    @Test
    public void testWideTypesTimestamp()
    {
        MaterializedResult result = computeActual(
                "SELECT col_timestamp FROM wide_types_table WHERE col_integer = 10000");
        assertThat(result.getRowCount()).isEqualTo(1);
    }

    @Test
    public void testWideTypesBlob()
    {
        MaterializedResult result = computeActual(
                "SELECT col_blob FROM wide_types_table WHERE col_integer = 10000");
        assertThat(result.getRowCount()).isEqualTo(1);
        // blob should be non-null
        assertThat(result.getMaterializedRows().get(0).getField(0)).isNotNull();
    }

    // ==================== NULL handling ====================

    @Test
    public void testNullableTableRead()
    {
        assertQuery("SELECT count(*) FROM nullable_table", "VALUES 4");
    }

    @Test
    public void testNullableTableNullCount()
    {
        // id column has one NULL (row 3)
        MaterializedResult result = computeActual(
                "SELECT count(*) FROM nullable_table WHERE id IS NULL");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(1L);
    }

    @Test
    public void testNullableTableNullNameCount()
    {
        // name column has NULLs in rows 2 and 4
        MaterializedResult result = computeActual(
                "SELECT count(*) FROM nullable_table WHERE name IS NULL");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(2L);
    }

    @Test
    public void testNullableTableNullComplexTypes()
    {
        // tags array is NULL in rows 2 and 4
        MaterializedResult result = computeActual(
                "SELECT count(*) FROM nullable_table WHERE tags IS NULL");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(2L);
    }

    @Test
    public void testNullableTableNullFiltering()
    {
        // Rows 1 and 3 have non-null name, price, active, created_date
        // But only row 1 has all non-null values including tags
        MaterializedResult result = computeActual(
                "SELECT id FROM nullable_table " +
                        "WHERE name IS NOT NULL AND price IS NOT NULL AND active IS NOT NULL " +
                        "AND created_date IS NOT NULL AND tags IS NOT NULL");
        assertThat(result.getRowCount()).isEqualTo(2);
    }

    @Test
    public void testNullableTableCoalesce()
    {
        MaterializedResult result = computeActual(
                "SELECT COALESCE(name, 'UNKNOWN') FROM nullable_table ORDER BY id NULLS LAST");
        List<String> names = result.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString())
                .toList();
        assertThat(names).contains("Present", "UNKNOWN", "NoId");
    }

    // ==================== Empty table ====================

    @Test
    public void testEmptyTableRead()
    {
        assertQueryReturnsEmptyResult("SELECT * FROM empty_table");
    }

    @Test
    public void testEmptyTableCount()
    {
        assertQuery("SELECT count(*) FROM empty_table", "VALUES 0");
    }

    @Test
    public void testEmptyTableAggregation()
    {
        MaterializedResult result = computeActual(
                "SELECT min(id), max(id), avg(value) FROM empty_table");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isNull();
        assertThat(result.getMaterializedRows().get(0).getField(1)).isNull();
        assertThat(result.getMaterializedRows().get(0).getField(2)).isNull();
    }

    // ==================== Complex types — dereference ====================

    @Test
    public void testStructFieldDereference()
    {
        MaterializedResult result = computeActual(
                "SELECT metadata.key, metadata.value FROM nested_table WHERE id = 1");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("color");
        assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo("red");
    }

    @Test
    public void testMapSubscript()
    {
        MaterializedResult result = computeActual(
                "SELECT tags['priority'] FROM nested_table WHERE id = 1");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
    }

    @Test
    public void testMapElementAtMissingKey()
    {
        // element_at returns NULL for missing keys (unlike subscript which throws)
        MaterializedResult result = computeActual(
                "SELECT element_at(tags, 'nonexistent') FROM nested_table WHERE id = 1");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isNull();
    }

    @Test
    public void testArraySubscript()
    {
        // nested_list for id=1 is [[1,2],[3,4]], so nested_list[1] is [1,2]
        MaterializedResult result = computeActual(
                "SELECT nested_list[1] FROM nested_table WHERE id = 1");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isNotNull();
    }

    @Test
    public void testComplexStructFieldDereference()
    {
        MaterializedResult result = computeActual(
                "SELECT complex_struct.name FROM nested_table WHERE id = 1");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("Alice");
    }

    @Test
    public void testComplexStructNestedArrayDereference()
    {
        // complex_struct.scores for id=1 is [90, 85, 92]
        MaterializedResult result = computeActual(
                "SELECT cardinality(complex_struct.scores) FROM nested_table WHERE id = 1");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(((Number) result.getMaterializedRows().get(0).getField(0)).longValue()).isEqualTo(3L);
    }

    @Test
    public void testComplexStructNestedMapDereference()
    {
        MaterializedResult result = computeActual(
                "SELECT complex_struct.attrs['dept'] FROM nested_table WHERE id = 2");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("sales");
    }

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

    @Test
    public void testArrayTableCardinality()
    {
        MaterializedResult result = computeActual(
                "SELECT id, cardinality(tags) FROM array_table ORDER BY id");
        assertThat(result.getRowCount()).isEqualTo(5);
        // id=1 has 3 tags, id=4 has 1 tag
        assertThat(((Number) result.getMaterializedRows().get(0).getField(1)).longValue()).isEqualTo(3L);
        assertThat(((Number) result.getMaterializedRows().get(3).getField(1)).longValue()).isEqualTo(1L);
    }

    @Test
    public void testArrayTableUnnest()
    {
        MaterializedResult result = computeActual(
                "SELECT a.id, t.tag FROM array_table a CROSS JOIN UNNEST(a.tags) AS t(tag) " +
                        "WHERE a.id = 1 ORDER BY t.tag");
        assertThat(result.getRowCount()).isEqualTo(3);
    }

    // ==================== Partitioned tables ====================

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
    public void testPartitionedTableWithNonMatchingPredicate()
    {
        assertQueryReturnsEmptyResult("SELECT * FROM partitioned_table WHERE region = 'NONEXISTENT'");
    }

    @Test
    public void testPartitionedTableDistinctPartitions()
    {
        MaterializedResult result = computeActual(
                "SELECT DISTINCT region FROM partitioned_table ORDER BY region");
        assertThat(result.getRowCount()).isEqualTo(3);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("APAC");
        assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo("EU");
        assertThat(result.getMaterializedRows().get(2).getField(0)).isEqualTo("US");
    }

    @Test
    public void testPartitionedTableAggregationPerPartition()
    {
        MaterializedResult result = computeActual(
                "SELECT region, sum(amount), count(*) FROM partitioned_table " +
                        "GROUP BY region ORDER BY region");
        assertThat(result.getRowCount()).isEqualTo(3);
    }

    @Test
    public void testTemporalPartitionedTableRead()
    {
        assertQuery("SELECT count(*) FROM temporal_partitioned_table", "VALUES 6");
    }

    @Test
    public void testTemporalPartitionedTableWithDatePredicate()
    {
        MaterializedResult result = computeActual(
                "SELECT count(*) FROM temporal_partitioned_table WHERE event_date = DATE '2023-06-10'");
        assertThat((Long) result.getMaterializedRows().get(0).getField(0)).isGreaterThanOrEqualTo(1L);
    }

    @Test
    public void testTemporalPartitionedTableWithYearPredicate()
    {
        MaterializedResult result = computeActual(
                "SELECT count(*) FROM temporal_partitioned_table WHERE year(event_date) = 2023");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(4L);
    }

    @Test
    public void testTemporalPartitionedTableWithMonthPredicate()
    {
        MaterializedResult result = computeActual(
                "SELECT count(*) FROM temporal_partitioned_table WHERE month(event_date) = 1");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(2L);
    }

    @Test
    public void testDailyPartitionedTableRead()
    {
        assertQuery("SELECT count(*) FROM daily_partitioned_table", "VALUES 5");
    }

    @Test
    public void testDailyPartitionedTableByExactDate()
    {
        MaterializedResult result = computeActual(
                "SELECT count(*) FROM daily_partitioned_table WHERE event_date = DATE '2023-06-15'");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(2L);
    }

    @Test
    public void testDailyPartitionedTableDateRange()
    {
        MaterializedResult result = computeActual(
                "SELECT count(*) FROM daily_partitioned_table " +
                        "WHERE event_date >= DATE '2023-06-15' AND event_date <= DATE '2023-06-20'");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(3L);
    }

    // ==================== Schema evolution ====================

    @Test
    public void testSchemaEvolutionTableRead()
    {
        assertQuery("SELECT count(*) FROM schema_evolution_table", "VALUES 4");
    }

    @Test
    public void testSchemaEvolutionTableColumns()
    {
        MaterializedResult result = computeActual("DESCRIBE schema_evolution_table");
        assertThat(result.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString()))
                .containsExactly("id", "original_col", "added_col");
    }

    @Test
    public void testSchemaEvolutionOldRowsHaveNullForNewColumn()
    {
        // Rows 1 and 2 were inserted before added_col was added
        MaterializedResult result = computeActual(
                "SELECT id, added_col FROM schema_evolution_table WHERE id <= 2 ORDER BY id");
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows().get(0).getField(1)).isNull();
        assertThat(result.getMaterializedRows().get(1).getField(1)).isNull();
    }

    @Test
    public void testSchemaEvolutionNewRowsHaveValues()
    {
        MaterializedResult result = computeActual(
                "SELECT id, added_col FROM schema_evolution_table WHERE id > 2 ORDER BY id");
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo(300);
        assertThat(result.getMaterializedRows().get(1).getField(1)).isEqualTo(400);
    }

    @Test
    public void testSchemaEvolutionSelectAll()
    {
        MaterializedResult result = computeActual(
                "SELECT * FROM schema_evolution_table ORDER BY id");
        assertThat(result.getRowCount()).isEqualTo(4);
        // Verify all columns are present including new one
        assertThat(result.getTypes()).hasSize(3);
    }

    // ==================== Aggregation table ====================

    @Test
    public void testAggregationTableCount()
    {
        assertQuery("SELECT count(*) FROM aggregation_table", "VALUES 30");
    }

    @Test
    public void testAggregationTableGroupByCategory()
    {
        MaterializedResult result = computeActual(
                "SELECT category, count(*), sum(quantity) FROM aggregation_table " +
                        "GROUP BY category ORDER BY category");
        assertThat(result.getRowCount()).isEqualTo(3);
        // Each category has 10 rows (30 total / 3 categories)
        for (MaterializedRow row : result.getMaterializedRows()) {
            assertThat(row.getField(1)).isEqualTo(10L);
        }
    }

    @Test
    public void testAggregationTableHaving()
    {
        MaterializedResult result = computeActual(
                "SELECT category, avg(amount) AS avg_amt FROM aggregation_table " +
                        "GROUP BY category HAVING avg(amount) > 0 ORDER BY category");
        assertThat(result.getRowCount()).isEqualTo(3);
    }

    @Test
    public void testAggregationTableMinMax()
    {
        MaterializedResult result = computeActual(
                "SELECT min(id), max(id), min(amount), max(amount) FROM aggregation_table");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo(30);
    }

    @Test
    public void testAggregationTableWindowFunction()
    {
        MaterializedResult result = computeActual(
                "SELECT id, category, " +
                        "row_number() OVER (PARTITION BY category ORDER BY id) AS rn " +
                        "FROM aggregation_table WHERE id <= 9 ORDER BY category, id");
        assertThat(result.getRowCount()).isEqualTo(9);
    }

    // ==================== EXPLAIN and planning ====================

    @Test
    public void testExplainSimpleSelect()
    {
        MaterializedResult result = computeActual("EXPLAIN SELECT * FROM simple_table");
        String plan = result.getMaterializedRows().get(0).getField(0).toString();
        assertThat(plan).contains("TableScan");
        assertThat(plan).containsIgnoringCase("ducklake");
    }

    @Test
    public void testExplainWithPredicate()
    {
        MaterializedResult result = computeActual(
                "EXPLAIN SELECT * FROM simple_table WHERE price > 30.0");
        String plan = result.getMaterializedRows().get(0).getField(0).toString();
        assertThat(plan).containsAnyOf("TableScan", "ScanFilter");
        assertThat(plan).contains("price");
    }

    @Test
    public void testExplainDistributed()
    {
        MaterializedResult result = computeActual(
                "EXPLAIN (TYPE DISTRIBUTED) SELECT * FROM simple_table");
        String plan = result.getMaterializedRows().get(0).getField(0).toString();
        assertThat(plan).contains("TableScan");
    }

    @Test
    public void testExplainJoin()
    {
        MaterializedResult result = computeActual(
                "EXPLAIN SELECT s.name, p.region FROM simple_table s " +
                        "JOIN partitioned_table p ON s.id = p.id");
        String plan = result.getMaterializedRows().get(0).getField(0).toString();
        assertThat(plan).contains("Join");
    }

    @Test
    public void testExplainAnalyze()
    {
        MaterializedResult result = computeActual(
                "EXPLAIN ANALYZE SELECT count(*) FROM simple_table");
        String plan = result.getMaterializedRows().get(0).getField(0).toString();
        assertThat(plan).contains("TableScan");
    }

    @Test
    public void testExplainAnalyzeWithPartitionPredicate()
    {
        MaterializedResult result = computeActual(
                "EXPLAIN ANALYZE SELECT * FROM partitioned_table WHERE region = 'US'");
        String plan = result.getMaterializedRows().get(0).getField(0).toString();
        // The plan should show the scan happened
        assertThat(plan).contains("TableScan");
    }

    // ==================== Joins (exercises dynamic filter path) ====================

    @Test
    public void testSelfJoin()
    {
        MaterializedResult result = computeActual(
                "SELECT a.id, b.name FROM simple_table a JOIN simple_table b ON a.id = b.id ORDER BY a.id");
        assertThat(result.getRowCount()).isEqualTo(5);
    }

    @Test
    public void testCrossTableJoin()
    {
        MaterializedResult result = computeActual(
                "SELECT s.name, p.region FROM simple_table s JOIN partitioned_table p ON s.id = p.id ORDER BY s.id");
        assertThat(result.getRowCount()).isEqualTo(5);
    }

    @Test
    public void testLeftJoin()
    {
        MaterializedResult result = computeActual(
                "SELECT s.id, n.metadata FROM simple_table s " +
                        "LEFT JOIN nested_table n ON s.id = n.id ORDER BY s.id");
        assertThat(result.getRowCount()).isEqualTo(5);
        // Only ids 1-3 have matches in nested_table
        assertThat(result.getMaterializedRows().get(0).getField(1)).isNotNull(); // id=1
        assertThat(result.getMaterializedRows().get(3).getField(1)).isNull();    // id=4
    }

    @Test
    public void testJoinWithSubquery()
    {
        MaterializedResult result = computeActual(
                "SELECT s.name FROM simple_table s " +
                        "WHERE s.id IN (SELECT id FROM partitioned_table WHERE region = 'US')");
        assertThat(result.getRowCount()).isEqualTo(2);
    }

    @Test
    public void testJoinBetweenPartitionedTables()
    {
        MaterializedResult result = computeActual(
                "SELECT p.name, t.event_name " +
                        "FROM partitioned_table p JOIN temporal_partitioned_table t ON p.id = t.id " +
                        "ORDER BY p.id");
        assertThat(result.getRowCount()).isGreaterThan(0);
    }

    @Test
    public void testDynamicFilterJoinSmallBuildSide()
    {
        // Small build-side table (nested_table, 3 rows) joined to larger probe-side
        // This exercises the dynamic filter code path
        MaterializedResult result = computeActual(
                "SELECT a.id, a.amount FROM aggregation_table a " +
                        "JOIN nested_table n ON a.id = n.id");
        assertThat(result.getRowCount()).isEqualTo(3);
    }

    // ==================== Set operations ====================

    @Test
    public void testUnionAll()
    {
        MaterializedResult result = computeActual(
                "SELECT id, name FROM simple_table WHERE id <= 2 " +
                        "UNION ALL " +
                        "SELECT id, event_name FROM temporal_partitioned_table WHERE id <= 2");
        assertThat(result.getRowCount()).isEqualTo(4);
    }

    @Test
    public void testUnionDistinct()
    {
        MaterializedResult result = computeActual(
                "SELECT active FROM simple_table " +
                        "UNION " +
                        "SELECT col_boolean FROM wide_types_table");
        assertThat(result.getRowCount()).isEqualTo(2); // true and false
    }

    @Test
    public void testExceptAll()
    {
        MaterializedResult result = computeActual(
                "SELECT id FROM simple_table " +
                        "EXCEPT " +
                        "SELECT id FROM nested_table");
        assertThat(result.getRowCount()).isEqualTo(2); // ids 4 and 5
    }

    @Test
    public void testIntersect()
    {
        MaterializedResult result = computeActual(
                "SELECT id FROM simple_table " +
                        "INTERSECT " +
                        "SELECT id FROM nested_table");
        assertThat(result.getRowCount()).isEqualTo(3); // ids 1, 2, 3
    }

    // ==================== Edge cases ====================

    @Test
    public void testFalsePredicateReturnsEmpty()
    {
        assertQueryReturnsEmptyResult("SELECT * FROM simple_table WHERE 1 = 0");
    }

    @Test
    public void testLimitZero()
    {
        assertQueryReturnsEmptyResult("SELECT * FROM simple_table LIMIT 0");
    }

    @Test
    public void testSelectStar()
    {
        MaterializedResult result = computeActual("SELECT * FROM simple_table");
        assertThat(result.getTypes()).hasSize(5);
        assertThat(result.getRowCount()).isEqualTo(5);
    }

    @Test
    public void testSelectConstant()
    {
        assertQuery("SELECT 1 FROM simple_table", "VALUES 1, 1, 1, 1, 1");
    }

    @Test
    public void testCaseExpression()
    {
        MaterializedResult result = computeActual(
                "SELECT id, CASE WHEN price > 40.0 THEN 'expensive' ELSE 'cheap' END AS tier " +
                        "FROM simple_table ORDER BY id");
        assertThat(result.getRowCount()).isEqualTo(5);
        assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo("cheap");    // 19.99
        assertThat(result.getMaterializedRows().get(4).getField(1)).isEqualTo("expensive"); // 59.99
    }

    @Test
    public void testCastExpression()
    {
        MaterializedResult result = computeActual(
                "SELECT CAST(id AS VARCHAR) FROM simple_table WHERE id = 1");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("1");
    }

    @Test
    public void testSubquery()
    {
        MaterializedResult result = computeActual(
                "SELECT * FROM simple_table WHERE price = (SELECT max(price) FROM simple_table)");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(5);
    }

    @Test
    public void testCorrelatedSubquery()
    {
        MaterializedResult result = computeActual(
                "SELECT s.id, s.name FROM simple_table s " +
                        "WHERE EXISTS (SELECT 1 FROM partitioned_table p WHERE p.id = s.id AND p.region = 'US')");
        assertThat(result.getRowCount()).isEqualTo(2);
    }

    @Test
    public void testWithClause()
    {
        MaterializedResult result = computeActual(
                "WITH expensive AS (SELECT * FROM simple_table WHERE price > 40.0) " +
                        "SELECT count(*) FROM expensive");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(2L);
    }

    // ==================== Write operations should fail ====================

    @Test
    public void testInsertNotSupported()
    {
        assertQueryFails(
                "INSERT INTO simple_table VALUES (99, 'test', 1.0, true, DATE '2024-01-01')",
                ".*not supported.*|.*This connector does not support.*");
    }

    @Test
    public void testDeleteNotSupported()
    {
        assertQueryFails(
                "DELETE FROM simple_table WHERE id = 1",
                ".*not supported.*|.*This connector does not support.*");
    }

    @Test
    public void testCreateTableNotSupported()
    {
        assertQueryFails(
                "CREATE TABLE test_schema.new_table (id INTEGER)",
                ".*not supported.*|.*This connector does not support.*");
    }

    @Test
    public void testDropTableNotSupported()
    {
        assertQueryFails(
                "DROP TABLE simple_table",
                ".*not supported.*|.*This connector does not support.*");
    }
}
