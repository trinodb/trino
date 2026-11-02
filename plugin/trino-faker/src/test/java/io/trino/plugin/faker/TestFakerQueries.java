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
package io.trino.plugin.faker;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.H2QueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryAssertions;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.trino.plugin.faker.ColumnInfo.ALLOWED_VALUES_PROPERTY;
import static io.trino.plugin.faker.FakerSplitManager.MAX_ROWS_PER_SPLIT;
import static io.trino.spi.StandardErrorCode.INVALID_COLUMN_PROPERTY;
import static io.trino.spi.StandardErrorCode.INVALID_COLUMN_REFERENCE;
import static io.trino.testing.assertions.Assert.assertEqualsIgnoreOrder;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

final class TestFakerQueries
        extends AbstractTestQueryFramework
{
    private static final String DETERMINISM_TEST_COLUMNS =
            """
            (
                id BIGINT NOT NULL WITH (min = '1', max = '1000', step = '1'),
                a BIGINT NOT NULL,
                b VARCHAR NOT NULL,
                c DATE,
                d VARCHAR NOT NULL WITH (generator = '#{Name.first_name}'),
                e UUID NOT NULL
            )
            """;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return FakerQueryRunner.builder().build();
    }

    @Test
    void testShowSchemas()
    {
        // Using 'contains' method because other tests may create schemas concurrently
        assertThat(computeActual("SHOW SCHEMAS FROM faker").getOnlyColumnAsSet())
                .contains("default", "information_schema");
    }

    @Test
    void testShowTables()
    {
        assertUpdate("CREATE TABLE faker.default.test (id INTEGER, name VARCHAR)");
        assertTableColumnNames("faker.default.test", "id", "name");
    }

    @Test
    void testTableComment()
    {
        try (TestTable table = newTrinoTable("table_comment", "(id INTEGER, name VARCHAR)")) {
            assertUpdate("COMMENT ON TABLE " + table.getName() + " IS 'test comment'");
            assertThat(getTableComment(table.getName())).isEqualTo("test comment");

            assertUpdate("COMMENT ON TABLE " + table.getName() + " IS ''");
            assertThat(getTableComment(table.getName())).isEmpty();

            assertUpdate("COMMENT ON TABLE " + table.getName() + " IS NULL");
            assertThat(getTableComment(table.getName())).isNull();
        }
    }

    @Test
    void testColumnComment()
    {
        try (TestTable table = newTrinoTable("comment", "(id INTEGER, name VARCHAR)")) {
            assertUpdate("COMMENT ON COLUMN %s.name IS 'comment text'".formatted(table.getName()));
            assertQuery("SHOW COLUMNS FROM " + table.getName(), "VALUES ('id', 'integer', '', ''), ('name', 'varchar', '', 'comment text')");
        }
    }

    @Test
    void testCannotCommentRowId()
    {
        try (TestTable table = newTrinoTable("cannot_comment", "(id INTEGER, name VARCHAR)")) {
            assertThat(query("COMMENT ON COLUMN \"%s\".\"$row_id\" IS 'comment text'".formatted(table.getName())))
                    .failure()
                    .hasErrorCode(INVALID_COLUMN_REFERENCE)
                    .hasMessageContaining("Cannot set comment for $row_id column");
        }
    }

    @Test
    void testParametricIntervals()
    {
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "interval_micros",
                "(v interval second(2, 6) WITH (allowed_values = ARRAY['0.000001'])) WITH (null_probability = 0.0)")) {
            assertQuery("SELECT CAST(min(v) AS varchar), CAST(max(v) AS varchar) FROM " + table.getName(),
                    "VALUES ('0.000001', '0.000001')");
        }
        assertQueryFails(
                "CREATE TABLE unsupported_interval (v interval second(2, 9))",
                "Faker does not support day-time intervals with fractional precision greater than 6: interval second\\(2, 9\\)");
        assertQueryFails(
                "CREATE TABLE unsupported_nested_interval (v row(x array(map(varchar, interval second(2, 9)))))",
                "Faker does not support day-time intervals with fractional precision greater than 6: interval second\\(2, 9\\)");
    }

    @Test
    void testSelectFromTable()
    {
        List<TestDataType> testCases = ImmutableList.<TestDataType>builder()
                .add(new TestDataType("rnd_bigint", "bigint", "count(distinct rnd_bigint)", "1000"))
                .add(new TestDataType("rnd_integer", "integer", "count(distinct rnd_integer)", "1000"))
                .add(new TestDataType("rnd_smallint", "smallint", "count(rnd_smallint)", "1000"))
                .add(new TestDataType("rnd_tinyint", "tinyint", "count(rnd_tinyint)", "1000"))
                .add(new TestDataType("rnd_boolean", "boolean", "count(distinct rnd_boolean)", "2"))
                .add(new TestDataType("rnd_date", "date", "count(rnd_date)", "1000"))
                .add(new TestDataType("rnd_decimal1", "decimal", "count(distinct rnd_decimal1)", "1000"))
                .add(new TestDataType("rnd_decimal2", "decimal(18,5)", "count(distinct rnd_decimal2)", "1000"))
                .add(new TestDataType("rnd_decimal3", "decimal(38,0)", "count(distinct rnd_decimal3)", "1000"))
                .add(new TestDataType("rnd_decimal4", "decimal(38,38)", "count(distinct rnd_decimal4)", "1000"))
                .add(new TestDataType("rnd_decimal5", "decimal(5,2)", "count(rnd_decimal5)", "1000"))
                .add(new TestDataType("rnd_real", "real", "count(rnd_real)", "1000"))
                .add(new TestDataType("rnd_double", "double", "count(distinct rnd_double)", "1000"))
                .add(new TestDataType("rnd_interval1", "interval day(2) to second(3)", "count(distinct rnd_interval1)", "1000"))
                .add(new TestDataType("rnd_interval2", "interval year(9) to month", "count(distinct rnd_interval2)", "1000"))
                .add(new TestDataType("rnd_timestamp", "timestamp", "count(distinct rnd_timestamp)", "1000"))
                .add(new TestDataType("rnd_timestamp0", "timestamp(0)", "count(distinct rnd_timestamp0)", "1000"))
                .add(new TestDataType("rnd_timestamp6", "timestamp(6)", "count(distinct rnd_timestamp6)", "1000"))
                .add(new TestDataType("rnd_timestamp9", "timestamp(9)", "count(distinct rnd_timestamp9)", "1000"))
                .add(new TestDataType("rnd_timestamptz", "timestamp with time zone", "count(distinct rnd_timestamptz)", "1000"))
                .add(new TestDataType("rnd_timestamptz0", "timestamp(0) with time zone", "count(distinct rnd_timestamptz0)", "1000"))
                .add(new TestDataType("rnd_timestamptz6", "timestamp(6) with time zone", "count(distinct rnd_timestamptz6)", "1000"))
                .add(new TestDataType("rnd_timestamptz9", "timestamp(9) with time zone", "count(distinct rnd_timestamptz9)", "1000"))
                .add(new TestDataType("rnd_time", "time", "count(rnd_time)", "1000"))
                .add(new TestDataType("rnd_time0", "time(0)", "count(rnd_time0)", "1000"))
                .add(new TestDataType("rnd_time6", "time(6)", "count(distinct rnd_time6)", "1000"))
                .add(new TestDataType("rnd_time9", "time(9)", "count(distinct rnd_time9)", "1000"))
                .add(new TestDataType("rnd_timetz", "time with time zone", "count(rnd_timetz)", "1000"))
                .add(new TestDataType("rnd_timetz0", "time(0) with time zone", "count(rnd_timetz0)", "1000"))
                .add(new TestDataType("rnd_timetz6", "time(6) with time zone", "count(distinct rnd_timetz6)", "1000"))
                .add(new TestDataType("rnd_timetz9", "time(9) with time zone", "count(distinct rnd_timetz9)", "1000"))
                .add(new TestDataType("rnd_timetz12", "time(12) with time zone", "count(distinct rnd_timetz12)", "1000"))
                .add(new TestDataType("rnd_varbinary", "varbinary", "count(distinct rnd_varbinary)", "1000"))
                .add(new TestDataType("rnd_varchar", "varchar", "count(rnd_varchar)", "1000"))
                .add(new TestDataType("rnd_nvarchar", "varchar(1000)", "count(rnd_nvarchar)", "1000"))
                .add(new TestDataType("rnd_char", "char", "count(distinct rnd_char)", "19"))
                .add(new TestDataType("rnd_nchar", "char(1000)", "count(distinct rnd_nchar)", "1000"))
                .add(new TestDataType("rnd_ipaddress", "ipaddress", "count(distinct rnd_ipaddress)", "1000"))
                .add(new TestDataType("rnd_uuid", "uuid", "count(distinct rnd_uuid)", "1000"))
                .add(new TestDataType("rnd_array_int", "array(integer)", "count(distinct cardinality(rnd_array_int))", "6"))
                .add(new TestDataType("rnd_array_varchar", "array(varchar)", "count(distinct cardinality(rnd_array_varchar))", "6"))
                .add(new TestDataType("rnd_map_int", "map(integer, integer)", "count(distinct cardinality(rnd_map_int))", "6"))
                .add(new TestDataType("rnd_map_varchar", "map(varchar, varchar)", "count(distinct cardinality(rnd_map_varchar))", "6"))
                .add(new TestDataType("rnd_row", "row(integer, varchar)", "count(distinct rnd_row)", "1000"))
                .add(new TestDataType("rnd_json", "json", "count_if(json_format(rnd_json) = 'null')", "1000"))
                .build();

        for (TestDataType testCase : testCases) {
            try (TestTable table = newTrinoTable("types_" + testCase.name(), "(%s)".formatted(testCase.columnSchema()))) {
                assertQuery("SELECT %s FROM %s".formatted(testCase.queryExpression(), table.getName()), "VALUES (%s)".formatted(testCase.expectedValue()));
            }
        }
    }

    @Test
    void testSelectBoundedVarchar()
    {
        try (TestTable table = newTrinoTable("bounded_varchar", "(single_char varchar(1), short_text varchar(5), empty varchar(0))")) {
            assertQuery(
                    "SELECT count(DISTINCT single_char) > 1, min(length(single_char)), min(length(short_text)), max(length(short_text)), count(DISTINCT length(short_text)), max(length(empty)) FROM " + table.getName(),
                    "VALUES (true, 1, 1, 5, 5, 0)");
        }
    }

    @Test
    void testSelectLimit()
    {
        try (TestTable table = newTrinoTable("single_column", "(rnd_bigint bigint NOT NULL) WITH (seed = 0)")) {
            assertQuery("SELECT count(rnd_bigint) FROM (SELECT rnd_bigint FROM %s LIMIT 5) a".formatted(table.getName()),
                    "VALUES (5)");

            assertQuery(
                    """
                    SELECT count(rnd_bigint)
                    FROM (SELECT rnd_bigint FROM %s LIMIT %d) a""".formatted(table.getName(), 2 * MAX_ROWS_PER_SPLIT),
                    "VALUES (%d)".formatted(2 * MAX_ROWS_PER_SPLIT));

            assertQuery("SELECT count(distinct rnd_bigint) FROM %s LIMIT 5".formatted(table.getName()),
                    "VALUES (1000)");

            assertQuery(
                    """
                    SELECT count(rnd_bigint)
                    FROM (SELECT rnd_bigint FROM %s LIMIT %d) a""".formatted(table.getName(), MAX_ROWS_PER_SPLIT),
                    "VALUES (%d)".formatted(MAX_ROWS_PER_SPLIT));

            // generating data should be deterministic
            String testQuery =
                    """
                    SELECT to_hex(checksum(rnd_bigint))
                    FROM (SELECT rnd_bigint FROM %s LIMIT %d) a""".formatted(table.getName(), 3 * MAX_ROWS_PER_SPLIT);
            assertQuery(testQuery, "VALUES ('8EB90377A1FAA953')");
            assertQuery(testQuery, "VALUES ('8EB90377A1FAA953')");
            assertQuery(testQuery, "VALUES ('8EB90377A1FAA953')");

            // there should be no overlap between data generated from different splits
            assertQuery(
                    """
                    SELECT count(1)
                    FROM (SELECT rnd_bigint FROM %s LIMIT %d) a
                    JOIN (SELECT rnd_bigint FROM %s LIMIT %d) b ON a.rnd_bigint = b.rnd_bigint""".formatted(table.getName(), 2 * MAX_ROWS_PER_SPLIT, table.getName(), 5 * MAX_ROWS_PER_SPLIT),
                    "VALUES (%d)".formatted(2 * MAX_ROWS_PER_SPLIT));
        }
    }

    @Test
    void testSelectedColumnsDoNotChangeValues()
    {
        try (TestTable table = newTrinoTable("selected_columns", DETERMINISM_TEST_COLUMNS)) {
            MaterializedResult allColumns = computeActual("SELECT id, a, b, c, d, e FROM " + table.getName());
            assertEqualsIgnoreOrder(
                    computeActual("SELECT id, b, c, e FROM " + table.getName()).getMaterializedRows(),
                    allColumns.project("id", "b", "c", "e").getMaterializedRows());
        }
    }

    @Test
    void testFilterDoesNotChangeValues()
    {
        try (TestTable table = newTrinoTable("filter", DETERMINISM_TEST_COLUMNS)) {
            MaterializedResult unfiltered = computeActual("SELECT id, b, c, d, e FROM " + table.getName());
            MaterializedResult filtered = computeActual("SELECT id, b, c, d, e FROM %s WHERE a > 0".formatted(table.getName()));
            assertThat(filtered.getRowCount()).isBetween(1, unfiltered.getRowCount() - 1);
            assertThat(unfiltered.getMaterializedRows()).containsAll(filtered.getMaterializedRows());
        }
    }

    @Test
    void testLimitDoesNotChangeValues()
    {
        try (TestTable table = newTrinoTable("limit", DETERMINISM_TEST_COLUMNS)) {
            assertEqualsIgnoreOrder(
                    computeActual("SELECT * FROM %s LIMIT 10".formatted(table.getName())).getMaterializedRows(),
                    computeActual("SELECT * FROM %s WHERE id <= 10".formatted(table.getName())).getMaterializedRows());
        }
    }

    @Test
    void testSeed()
    {
        try (TestTable first = newTrinoTable("seed_first", DETERMINISM_TEST_COLUMNS);
                TestTable second = newTrinoTable("seed_second", DETERMINISM_TEST_COLUMNS);
                TestTable firstWithSeed = newTrinoTable("seed_explicit_first", DETERMINISM_TEST_COLUMNS + " WITH (seed = 42)");
                TestTable secondWithSeed = newTrinoTable("seed_explicit_second", DETERMINISM_TEST_COLUMNS + " WITH (seed = 42)")) {
            assertQuery(countMatchingRows(first.getName(), second.getName()), "VALUES 0");
            assertQuery(countMatchingRows(firstWithSeed.getName(), secondWithSeed.getName()), "VALUES 1000");
        }

        assertUpdate("CREATE TABLE recreated " + DETERMINISM_TEST_COLUMNS);
        MaterializedResult rows = computeActual("SELECT * FROM recreated");
        assertUpdate("DROP TABLE recreated");
        assertUpdate("CREATE TABLE recreated " + DETERMINISM_TEST_COLUMNS);
        assertEqualsIgnoreOrder(computeActual("SELECT * FROM recreated").getMaterializedRows(), rows.getMaterializedRows());
        assertUpdate("DROP TABLE recreated");

        // names that are equal when joined with a period
        assertUpdate("CREATE SCHEMA \"seed.schema\"");
        assertUpdate("CREATE SCHEMA seed");
        assertUpdate("CREATE TABLE \"seed.schema\".tbl " + DETERMINISM_TEST_COLUMNS);
        assertUpdate("CREATE TABLE seed.\"schema.tbl\" " + DETERMINISM_TEST_COLUMNS);
        assertQuery(countMatchingRows("\"seed.schema\".tbl", "seed.\"schema.tbl\""), "VALUES 0");
        assertUpdate("DROP SCHEMA \"seed.schema\" CASCADE");
        assertUpdate("DROP SCHEMA seed CASCADE");
    }

    private static String countMatchingRows(String firstTable, String secondTable)
    {
        return """
               SELECT count(*)
               FROM %s x
               JOIN %s y ON x.id = y.id AND x.a = y.a AND x.b = y.b AND x.d = y.d AND x.e = y.e
               """.formatted(firstTable, secondTable);
    }

    @Test
    void testSelectDefaultTableLimit()
    {
        try (TestTable table = newTrinoTable("default_table_limit", "(rnd_bigint bigint NOT NULL) WITH (default_limit = 100)")) {
            assertQuery("SELECT count(distinct rnd_bigint) FROM " + table.getName(), "VALUES (100)");
        }
    }

    @Test
    public void selectOnlyNulls()
    {
        try (TestTable table = newTrinoTable("only_nulls", "(rnd_bigint bigint) WITH (null_probability = 1.0)")) {
            assertQuery("SELECT count(distinct rnd_bigint) FROM " + table.getName(), "VALUES (0)");
        }
    }

    @Test
    void testSelectGenerator()
    {
        try (TestTable table = newTrinoTable("generators",
                """
                (
                    name VARCHAR NOT NULL WITH (generator = '#{Name.first_name} #{Name.last_name}'),
                    age_years INTEGER NOT NULL
                )
                WITH (seed = 0)
                """)) {
            assertQuery("SELECT name FROM " + table.getName() + " LIMIT 1", "VALUES ('Coralee Wintheiser')");
            assertQuery("SELECT count(DISTINCT name) > 500 FROM " + table.getName(), "VALUES true");
        }
    }

    @Test
    void testSelectGeneratorTruncatedToColumnLength()
    {
        try (TestTable table = newTrinoTable("bounded_generators",
                """
                (
                    name_varchar VARCHAR(3) NOT NULL WITH (generator = '#{Name.first_name}'),
                    name_char CHAR(2) NOT NULL WITH (generator = '#{Name.first_name}'),
                    multibyte_varchar VARCHAR(3) NOT NULL WITH (generator = 'Żółw #{Name.first_name}'),
                    multibyte_char CHAR(2) NOT NULL WITH (generator = 'Żółw #{Name.first_name}'),
                    trailing_space_char CHAR(2) NOT NULL WITH (generator = 'A #{Name.first_name}')
                )
                """)) {
            assertThat(computeActual("SELECT name_varchar, name_char FROM " + table.getName()).getMaterializedRows())
                    .isNotEmpty()
                    .allSatisfy(row -> {
                        assertThat((String) row.getField(0)).hasSizeBetween(1, 3);
                        assertThat((String) row.getField(1)).hasSize(2);
                    });
            assertThat(query("SELECT DISTINCT multibyte_varchar, multibyte_char, trailing_space_char FROM " + table.getName()))
                    .matches("VALUES (CAST('Żół' AS VARCHAR(3)), CAST('Żó' AS CHAR(2)), CAST('A' AS CHAR(2)))");
        }
    }

    @Test
    void testSelectTypedGenerator()
    {
        try (TestTable table = newTrinoTable("typed_generators",
                """
                (
                    rnd_bigint BIGINT NOT NULL WITH (generator = '#{Number.digits ''5''}'),
                    rnd_integer INTEGER NOT NULL WITH (generator = '#{Number.numberBetween ''1'',''10''}'),
                    rnd_decimal DECIMAL(5,2) NOT NULL WITH (generator = '#{Commerce.price}'),
                    rnd_boolean BOOLEAN NOT NULL WITH (generator = '#{Bool.bool}'),
                    rnd_date DATE NOT NULL WITH (generator = '#{Date.birthday ''yyyy-MM-dd''}'),
                    rnd_ipaddress IPADDRESS NOT NULL WITH (generator = '#{Internet.ipV4Address}'),
                    rnd_uuid UUID NOT NULL WITH (generator = '#{Internet.uuid}')
                )
                """)) {
            assertQuery(
                    """
                    SELECT
                        min(rnd_bigint) >= 0 AND max(rnd_bigint) <= 99999,
                        count(DISTINCT rnd_integer),
                        min(rnd_integer),
                        max(rnd_integer),
                        min(rnd_decimal) >= 0 AND max(rnd_decimal) <= 100,
                        count(DISTINCT rnd_boolean),
                        min(rnd_date) > current_date - INTERVAL '100' YEAR AND max(rnd_date) < current_date - INTERVAL '10' YEAR,
                        count(DISTINCT rnd_ipaddress),
                        count(DISTINCT rnd_uuid)
                    FROM %s
                    """.formatted(table.getName()),
                    "VALUES (true, 9, 1, 9, true, 2, true, 1000, 1000)");
        }

        assertQueryFails(
                "CREATE TABLE invalid_generator_value (rnd_integer INTEGER WITH (generator = '#{Name.first_name}'))",
                "The `generator` property of column `rnd_integer` produced `\\w+`, which is not a valid integer value");
        assertQueryFails(
                "CREATE TABLE out_of_range_generator_value (rnd_tinyint TINYINT WITH (generator = '#{Number.numberBetween ''1000'',''2000''}'))",
                "The `generator` property of column `rnd_tinyint` produced `\\d+`, which is not a valid tinyint value");
        assertQueryFails(
                "CREATE TABLE structural_generator (rnd_row ROW(x VARCHAR) WITH (generator = '#{Name.first_name}'))",
                "The `generator` property cannot be set for ROW, ARRAY, MAP, or JSON columns");
        assertQueryFails(
                "CREATE TABLE generator_with_min (rnd_integer INTEGER WITH (generator = '#{Number.numberBetween ''1'',''10''}', min = '1'))",
                "The `generator` property cannot be set together with `min`, `max`, and `step` properties");
    }

    @Test
    void testSelectLocale()
            throws Exception
    {
        try (
                QueryRunner queryRunner = FakerQueryRunner.builder().setFakerProperties(Map.of("faker.locale", "pl-PL")).build();
                H2QueryRunner h2QueryRunner = new H2QueryRunner();
                TestTable table = new TestTable(queryRunner::execute, "locale",
                        """
                        (
                            name VARCHAR NOT NULL WITH (generator = '#{Name.first_name} #{Name.last_name}'),
                            age_years INTEGER NOT NULL
                        )
                        WITH (seed = 0)
                        """)) {
            QueryAssertions.assertQuery(
                    queryRunner,
                    getSession(),
                    "SELECT name FROM " + table.getName() + " LIMIT 1",
                    h2QueryRunner,
                    "VALUES ('Stefania Raczyński')",
                    false,
                    false);
        }
    }

    @Test
    void testSelectFunctions()
    {
        @Language("SQL")
        String testQuery = "SELECT faker.default.random_string('#{options.option ''a'', ''b''}') IN ('a', 'b')";
        assertQuery(testQuery, "VALUES (true)");
    }

    @Test
    void testSelectFunctionsLocale()
    {
        getQueryRunner().createCatalog("faker_pl", "faker", Map.of("faker.locale", "pl-PL"));
        assertThat(computeScalar("SELECT faker_pl.default.random_string('#{Address.default_country}')"))
                .isEqualTo("Polska");
    }

    @Test
    void testListFunctionsRespectsSchemaName()
    {
        assertUpdate("CREATE SCHEMA faker.other_schema");
        assertThat(query("SHOW FUNCTIONS FROM faker.other_schema"))
                .result()
                .isEmpty();
        assertUpdate("DROP SCHEMA faker.other_schema");
    }

    @Test
    void testResolveFunctionRespectsSchemaName()
    {
        assertUpdate("CREATE SCHEMA faker.function_schema");
        assertQueryFails(
                "SELECT faker.function_schema.random_string('#{Name.first_name}')",
                ".*Function 'faker.function_schema.random_string' not registered");
        assertUpdate("DROP SCHEMA faker.function_schema");
    }

    @Test
    void testSelectRangeProperties()
    {
        // inclusive ranges that produce only 2 values
        // high boundary float value obtained using `Math.nextUp((float) 0.0)`
        List<TestDataType> testCases = ImmutableList.<TestDataType>builder()
                .add(new TestDataType("rnd_bigint", "bigint", Map.of("min", "0", "max", "1"), "count(distinct rnd_bigint)", "2"))
                .add(new TestDataType("rnd_integer", "integer", Map.of("min", "0", "max", "1"), "count(distinct rnd_integer)", "2"))
                .add(new TestDataType("rnd_smallint", "smallint", Map.of("min", "0", "max", "1"), "count(distinct rnd_smallint)", "2"))
                .add(new TestDataType("rnd_tinyint", "tinyint", Map.of("min", "0", "max", "1"), "count(distinct rnd_tinyint)", "2"))
                .add(new TestDataType("rnd_date", "date", Map.of("min", "2022-03-01", "max", "2022-03-02"), "count(distinct rnd_date)", "2"))
                .add(new TestDataType("rnd_decimal1", "decimal", Map.of("min", "0", "max", "1"), "count(distinct rnd_decimal1)", "2"))
                .add(new TestDataType("rnd_decimal2", "decimal(18,5)", Map.of("min", "0.00000", "max", "0.00001"), "count(distinct rnd_decimal2)", "2"))
                .add(new TestDataType("rnd_decimal3", "decimal(38,0)", Map.of("min", "0", "max", "1"), "count(distinct rnd_decimal3)", "2"))
                .add(new TestDataType("rnd_decimal4", "decimal(38,38)", Map.of("min", "0.00000000000000000000000000000000000000", "max", "0.00000000000000000000000000000000000001"), "count(distinct rnd_decimal4)", "2"))
                .add(new TestDataType("rnd_decimal5", "decimal(5,2)", Map.of("min", "0.00", "max", "0.01"), "count(distinct rnd_decimal5)", "2"))
                .add(new TestDataType("rnd_real", "real", Map.of("min", "0.0", "max", "1.4E-45"), "count(distinct rnd_real)", "2"))
                .add(new TestDataType("rnd_double", "double", Map.of("min", "0.0", "max", "4.9E-324"), "count(distinct rnd_double)", "2"))
                .add(new TestDataType("rnd_interval1", "interval day(2) to second(3)", Map.of("min", "0.000", "max", "0.001"), "count(distinct rnd_interval1)", "2"))
                .add(new TestDataType("rnd_interval2", "interval year(9) to month", Map.of("min", "0", "max", "1"), "count(distinct rnd_interval2)", "2"))
                .add(new TestDataType("rnd_timestamp", "timestamp", Map.of("min", "2022-03-21 00:00:00.000", "max", "2022-03-21 00:00:00.001"), "count(distinct rnd_timestamp)", "2"))
                .add(new TestDataType("rnd_timestamp0", "timestamp(0)", Map.of("min", "2022-03-21 00:00:00", "max", "2022-03-21 00:00:01"), "count(distinct rnd_timestamp0)", "2"))
                .add(new TestDataType("rnd_timestamp6", "timestamp(6)", Map.of("min", "2022-03-21 00:00:00.000000", "max", "2022-03-21 00:00:00.000001"), "count(distinct rnd_timestamp6)", "2"))
                .add(new TestDataType("rnd_timestamp9", "timestamp(9)", Map.of("min", "2022-03-21 00:00:00.000000000", "max", "2022-03-21 00:00:00.000000001"), "count(distinct rnd_timestamp9)", "2"))
                .add(new TestDataType("rnd_timestamptz", "timestamp with time zone", Map.of("min", "2022-03-21 00:00:00.000 +01:00", "max", "2022-03-21 00:00:00.001 +01:00"), "count(distinct rnd_timestamptz)", "2"))
                .add(new TestDataType("rnd_timestamptz0", "timestamp(0) with time zone", Map.of("min", "2022-03-21 00:00:00 +01:00", "max", "2022-03-21 00:00:01 +01:00"), "count(distinct rnd_timestamptz0)", "2"))
                .add(new TestDataType("rnd_timestamptz6", "timestamp(6) with time zone", Map.of("min", "2022-03-21 00:00:00.000000 +01:00", "max", "2022-03-21 00:00:00.000001 +01:00"), "count(distinct rnd_timestamptz6)", "2"))
                .add(new TestDataType("rnd_timestamptz9", "timestamp(9) with time zone", Map.of("min", "2022-03-21 00:00:00.000000000 +01:00", "max", "2022-03-21 00:00:00.000000001 +01:00"), "count(distinct rnd_timestamptz9)", "2"))
                // an inclusive high bound at the top unit of a millisecond or microsecond must carry
                // into the next one, and at maximum precision the fraction must survive normalization
                .add(new TestDataType("rnd_timestamp9_carry", "timestamp(9)", Map.of("min", "2022-03-21 00:00:00.000000998", "max", "2022-03-21 00:00:00.000000999"), "count(distinct rnd_timestamp9_carry)", "2"))
                .add(new TestDataType("rnd_timestamp9_micro", "timestamp(9)", Map.of("min", "2022-03-21 00:00:00.000000999", "max", "2022-03-21 00:00:00.000001001"), "count(distinct rnd_timestamp9_micro)", "3"))
                .add(new TestDataType("rnd_timestamp12", "timestamp(12)", Map.of("min", "2022-03-21 00:00:00.000000000001", "max", "2022-03-21 00:00:00.000000000002"), "count(distinct rnd_timestamp12)", "2"))
                .add(new TestDataType("rnd_timestamptz6_milli", "timestamp(6) with time zone", Map.of("min", "2022-03-21 00:00:00.000999 +01:00", "max", "2022-03-21 00:00:00.001000 +01:00"), "count(distinct rnd_timestamptz6_milli)", "2"))
                .add(new TestDataType("rnd_timestamptz9_milli", "timestamp(9) with time zone", Map.of("min", "2022-03-21 00:00:00.000999999 +01:00", "max", "2022-03-21 00:00:00.001000001 +01:00"), "count(distinct rnd_timestamptz9_milli)", "3"))
                .add(new TestDataType("rnd_timestamptz6_carry", "timestamp(6) with time zone", Map.of("min", "2022-03-21 00:00:00.000998 +01:00", "max", "2022-03-21 00:00:00.000999 +01:00"), "count(distinct rnd_timestamptz6_carry)", "2"))
                .add(new TestDataType("rnd_timestamptz12", "timestamp(12) with time zone", Map.of("min", "2022-03-21 00:00:00.000000000001 +01:00", "max", "2022-03-21 00:00:00.000000000002 +01:00"), "count(distinct rnd_timestamptz12)", "2"))
                .add(new TestDataType("rnd_time", "time", Map.of("min", "01:02:03.456", "max", "01:02:03.457"), "count(distinct rnd_time)", "2"))
                .add(new TestDataType("rnd_time0", "time(0)", Map.of("min", "01:02:03", "max", "01:02:04"), "count(distinct rnd_time0)", "2"))
                .add(new TestDataType("rnd_time6", "time(6)", Map.of("min", "01:02:03.000456", "max", "01:02:03.000457"), "count(distinct rnd_time6)", "2"))
                .add(new TestDataType("rnd_time9", "time(9)", Map.of("min", "01:02:03.000000456", "max", "01:02:03.000000457"), "count(distinct rnd_time9)", "2"))
                .add(new TestDataType("rnd_timetz", "time with time zone", Map.of("min", "01:02:03.456 +01:00", "max", "01:02:03.457 +01:00"), "count(distinct rnd_timetz)", "2"))
                .add(new TestDataType("rnd_timetz0", "time(0) with time zone", Map.of("min", "01:02:03 +01:00", "max", "01:02:04 +01:00"), "count(distinct rnd_timetz0)", "2"))
                .add(new TestDataType("rnd_timetz6", "time(6) with time zone", Map.of("min", "01:02:03.000456 +01:00", "max", "01:02:03.000457 +01:00"), "count(distinct rnd_timetz6)", "2"))
                .add(new TestDataType("rnd_timetz9", "time(9) with time zone", Map.of("min", "01:02:03.000000456 +01:00", "max", "01:02:03.000000457 +01:00"), "count(distinct rnd_timetz9)", "2"))
                .add(new TestDataType("rnd_timetz12", "time(12) with time zone", Map.of("min", "01:02:03.000000000456 +01:00", "max", "01:02:03.000000000457 +01:00"), "count(distinct rnd_timetz12)", "2"))
                .build();

        for (TestDataType testCase : testCases) {
            try (TestTable table = newTrinoTable("range_small_" + testCase.name(), "(%s)".formatted(testCase.columnSchema()))) {
                assertQuery("SELECT %s FROM %s".formatted(testCase.queryExpression(), table.getName()), "VALUES (%s)".formatted(testCase.expectedValue()));
            }
        }

        // inclusive ranges at the max high bound that produce only 2 values
        // the max value of floating point types is never sampled, so they use wider ranges
        testCases = ImmutableList.<TestDataType>builder()
                .add(new TestDataType("rnd_bigint", "bigint", Map.of("min", "9223372036854775806", "max", "9223372036854775807"), "count(distinct rnd_bigint)", "2"))
                .add(new TestDataType("rnd_integer", "integer", Map.of("min", "2147483646", "max", "2147483647"), "count(distinct rnd_integer)", "2"))
                .add(new TestDataType("rnd_smallint", "smallint", Map.of("min", "32766", "max", "32767"), "count(distinct rnd_smallint)", "2"))
                .add(new TestDataType("rnd_tinyint", "tinyint", Map.of("min", "126", "max", "127"), "count(distinct rnd_tinyint)", "2"))
                .add(new TestDataType("rnd_date", "date", Map.of("min", "5881580-07-10", "max", "5881580-07-11"), "count(distinct rnd_date)", "2"))
                .add(new TestDataType("rnd_decimal1", "decimal", Map.of("min", "99999999999999999999999999999999999998", "max", "99999999999999999999999999999999999999"), "count(distinct rnd_decimal1)", "2"))
                .add(new TestDataType("rnd_decimal2", "decimal(18,5)", Map.of("min", "9999999999999.99998", "max", "9999999999999.99999"), "count(distinct rnd_decimal2)", "2"))
                .add(new TestDataType("rnd_real", "real", Map.of("min", "0.0", "max", "3.4028235E38"), "count(distinct rnd_real) > 1", "true"))
                .add(new TestDataType("rnd_double", "double", Map.of("min", "0.0", "max", "1.7976931348623157E308"), "count(distinct rnd_double) > 1", "true"))
                .add(new TestDataType("rnd_timestamp", "timestamp", Map.of("min", "294247-01-10 04:00:54.774", "max", "294247-01-10 04:00:54.775"), "count(distinct rnd_timestamp)", "2"))
                .add(new TestDataType("rnd_timestamp6", "timestamp(6)", Map.of("min", "294247-01-10 04:00:54.775806", "max", "294247-01-10 04:00:54.775807"), "count(distinct rnd_timestamp6)", "2"))
                .add(new TestDataType("rnd_timestamp9", "timestamp(9)", Map.of("min", "294247-01-10 04:00:54.775807998", "max", "294247-01-10 04:00:54.775807999"), "count(distinct rnd_timestamp9)", "2"))
                .add(new TestDataType("rnd_timestamp12", "timestamp(12)", Map.of("min", "294247-01-10 04:00:54.775807999998", "max", "294247-01-10 04:00:54.775807999999"), "count(distinct rnd_timestamp12)", "2"))
                .add(new TestDataType("rnd_time", "time", Map.of("min", "23:59:59.998", "max", "23:59:59.999"), "count(distinct rnd_time)", "2"))
                .add(new TestDataType("rnd_time12", "time(12)", Map.of("min", "23:59:59.999999999998", "max", "23:59:59.999999999999"), "count(distinct rnd_time12)", "2"))
                .build();

        for (TestDataType testCase : testCases) {
            try (TestTable table = newTrinoTable("range_top_" + testCase.name(), "(%s)".formatted(testCase.columnSchema()))) {
                assertQuery("SELECT %s FROM %s".formatted(testCase.queryExpression(), table.getName()), "VALUES (%s)".formatted(testCase.expectedValue()));
                assertQuery(
                        "SELECT count_if(%1$s < CAST('%2$s' AS %4$s) OR %1$s > CAST('%3$s' AS %4$s)) FROM %5$s".formatted(
                                testCase.name(), testCase.properties().get("min"), testCase.properties().get("max"), testCase.type(), table.getName()),
                        "VALUES 0");
            }
        }

        // ranges wider than the max value of a long are sampled uniformly, so the lowest bits vary, and values reach the top of the range
        try (TestTable table = newTrinoTable("range_wide", "(rnd_bigint bigint NOT NULL WITH (min = '-1', max = '9223372036854775807')) WITH (default_limit = 10000)")) {
            assertQuery(
                    "SELECT count(DISTINCT rnd_bigint % 1024) > 1000, count_if(rnd_bigint > 9223372036854775807 - 1000000000000000000) BETWEEN 500 AND 1700 FROM " + table.getName(),
                    "VALUES (true, true)");
        }

        // inclusive range to get the min low bound
        testCases = ImmutableList.<TestDataType>builder()
                .add(new TestDataType("rnd_bigint", "bigint", Map.of("max", "-9223372036854775808"), "count(distinct rnd_bigint)", "1"))
                .add(new TestDataType("rnd_integer", "integer", Map.of("max", "-2147483648"), "count(distinct rnd_integer)", "1"))
                .add(new TestDataType("rnd_smallint", "smallint", Map.of("max", "-32768"), "count(distinct rnd_smallint)", "1"))
                .add(new TestDataType("rnd_tinyint", "tinyint", Map.of("max", "-128"), "count(distinct rnd_tinyint)", "1"))
                .add(new TestDataType("rnd_date", "date", Map.of("max", "-5877641-06-23"), "count(distinct rnd_date)", "1"))
                .add(new TestDataType("rnd_decimal1", "decimal", Map.of("max", "-99999999999999999999999999999999999999"), "count(distinct rnd_decimal1)", "1"))
                .add(new TestDataType("rnd_decimal2", "decimal(18,5)", Map.of("max", "-9999999999999.99999"), "count(distinct rnd_decimal2)", "1"))
                .add(new TestDataType("rnd_decimal3", "decimal(38,0)", Map.of("max", "-99999999999999999999999999999999999999"), "count(distinct rnd_decimal3)", "1"))
                .add(new TestDataType("rnd_decimal4", "decimal(38,38)", Map.of("max", "-0.99999999999999999999999999999999999999"), "count(distinct rnd_decimal4)", "1"))
                .add(new TestDataType("rnd_decimal5", "decimal(5,2)", Map.of("max", "-999.99"), "count(distinct rnd_decimal5)", "1"))
                .add(new TestDataType("rnd_real", "real", Map.of("max", "1.4E-45"), "count(distinct rnd_real)", "1"))
                .add(new TestDataType("rnd_double", "double", Map.of("max", "4.9E-324"), "count(distinct rnd_double)", "1"))
                // interval literals can't represent smallest possible values allowed by the engine, so they're not included here
                // can't test timestamps because their extreme values cannot be expressed as literals
                .add(new TestDataType("rnd_time", "time", Map.of("max", "00:00:00.000"), "count(distinct rnd_time)", "1"))
                .add(new TestDataType("rnd_time0", "time(0)", Map.of("max", "00:00:00"), "count(distinct rnd_time0)", "1"))
                .add(new TestDataType("rnd_time6", "time(6)", Map.of("max", "00:00:00.000000"), "count(distinct rnd_time6)", "1"))
                .add(new TestDataType("rnd_time9", "time(9)", Map.of("max", "00:00:00.000000000"), "count(distinct rnd_time9)", "1"))
                .add(new TestDataType("rnd_timetz", "time with time zone", Map.of("max", "00:00:00.000 +01:00"), "count(distinct rnd_timetz)", "1"))
                .add(new TestDataType("rnd_timetz0", "time(0) with time zone", Map.of("max", "00:00:00 +01:00"), "count(distinct rnd_timetz0)", "1"))
                .add(new TestDataType("rnd_timetz6", "time(6) with time zone", Map.of("max", "00:00:00.000000 +01:00"), "count(distinct rnd_timetz6)", "1"))
                .add(new TestDataType("rnd_timetz9", "time(9) with time zone", Map.of("max", "00:00:00.000000000 +01:00"), "count(distinct rnd_timetz9)", "1"))
                .add(new TestDataType("rnd_timetz12", "time(12) with time zone", Map.of("max", "00:00:00.000000000000 +01:00"), "count(distinct rnd_timetz12)", "1"))
                .build();

        for (TestDataType testCase : testCases) {
            try (TestTable table = newTrinoTable("range_max_" + testCase.name(), "(%s)".formatted(testCase.columnSchema()))) {
                assertQuery("SELECT %s FROM %s".formatted(testCase.queryExpression(), table.getName()), "VALUES (%s)".formatted(testCase.expectedValue()));
            }
        }

        // exclusive range to get the max high bound
        testCases = ImmutableList.<TestDataType>builder()
                .add(new TestDataType("rnd_bigint", "bigint", Map.of("min", "9223372036854775807"), "count(distinct rnd_bigint)", "1"))
                .add(new TestDataType("rnd_integer", "integer", Map.of("min", "2147483647"), "count(distinct rnd_integer)", "1"))
                .add(new TestDataType("rnd_smallint", "smallint", Map.of("min", "32767"), "count(distinct rnd_smallint)", "1"))
                .add(new TestDataType("rnd_tinyint", "tinyint", Map.of("min", "127"), "count(distinct rnd_tinyint)", "1"))
                .add(new TestDataType("rnd_date", "date", Map.of("min", "5881580-07-11"), "count(distinct rnd_date)", "1"))
                .add(new TestDataType("rnd_decimal1", "decimal", Map.of("min", "99999999999999999999999999999999999999"), "count(distinct rnd_decimal1)", "1"))
                .add(new TestDataType("rnd_decimal2", "decimal(18,5)", Map.of("min", "9999999999999.99999"), "count(distinct rnd_decimal2)", "1"))
                .add(new TestDataType("rnd_decimal3", "decimal(38,0)", Map.of("min", "99999999999999999999999999999999999999"), "count(distinct rnd_decimal3)", "1"))
                .add(new TestDataType("rnd_decimal4", "decimal(38,38)", Map.of("min", "0.99999999999999999999999999999999999999"), "count(distinct rnd_decimal4)", "1"))
                .add(new TestDataType("rnd_decimal5", "decimal(5,2)", Map.of("min", "999.99"), "count(distinct rnd_decimal5)", "1"))
                .add(new TestDataType("rnd_real", "real", Map.of("min", "3.4028235E38"), "count(distinct rnd_real)", "1"))
                .add(new TestDataType("rnd_double", "double", Map.of("min", "1.7976931348623157E308"), "count(distinct rnd_double)", "1"))
                // interval literals can't represent smallest possible values allowed by the engine, so they're not included here
                // can't test timestamps because their extreme values cannot be expressed as literals
                .add(new TestDataType("rnd_time", "time", Map.of("min", "23:59:59.999"), "count(distinct rnd_time)", "1"))
                .add(new TestDataType("rnd_time0", "time(0)", Map.of("min", "23:59:59"), "count(distinct rnd_time0)", "1"))
                .add(new TestDataType("rnd_time6", "time(6)", Map.of("min", "23:59:59.999999"), "count(distinct rnd_time6)", "1"))
                .add(new TestDataType("rnd_time9", "time(9)", Map.of("min", "23:59:59.999999999"), "count(distinct rnd_time9)", "1"))
                .add(new TestDataType("rnd_timetz", "time with time zone", Map.of("min", "23:59:59.999 +01:00"), "count(distinct rnd_timetz)", "1"))
                .add(new TestDataType("rnd_timetz0", "time(0) with time zone", Map.of("min", "23:59:59 +01:00"), "count(distinct rnd_timetz0)", "1"))
                .add(new TestDataType("rnd_timetz6", "time(6) with time zone", Map.of("min", "23:59:59.999999 +01:00"), "count(distinct rnd_timetz6)", "1"))
                .add(new TestDataType("rnd_timetz9", "time(9) with time zone", Map.of("min", "23:59:59.999999999 +01:00"), "count(distinct rnd_timetz9)", "1"))
                .add(new TestDataType("rnd_timetz12", "time(12) with time zone", Map.of("min", "23:59:59.999999999999 +01:00"), "count(distinct rnd_timetz12)", "1"))
                .build();

        for (TestDataType testCase : testCases) {
            try (TestTable table = newTrinoTable("range_min_" + testCase.name(), "(%s)".formatted(testCase.columnSchema()))) {
                assertQuery("SELECT %s FROM %s".formatted(testCase.queryExpression(), table.getName()), "VALUES (%s)".formatted(testCase.expectedValue()));
            }
        }
    }

    @Test
    void testSelectDefaultTemporalRange()
    {
        // without min and max, values are generated from 1970-01-01 (inclusive) to 2100-01-01 (exclusive), in UTC,
        // and a fixed seed makes sure the generated values include both the first and the last year
        List<TestDataType> testCases = ImmutableList.<TestDataType>builder()
                .add(new TestDataType("rnd_date", "date", "year(min(rnd_date)) = 1970 AND year(max(rnd_date)) = 2099 AND count(distinct rnd_date) > 900", "true"))
                .add(new TestDataType("rnd_timestamp", "timestamp", "year(min(rnd_timestamp)) = 1970 AND year(max(rnd_timestamp)) = 2099", "true"))
                .add(new TestDataType("rnd_timestamp0", "timestamp(0)", "year(min(rnd_timestamp0)) = 1970 AND year(max(rnd_timestamp0)) = 2099", "true"))
                .add(new TestDataType("rnd_timestamp6", "timestamp(6)", "year(min(rnd_timestamp6)) = 1970 AND year(max(rnd_timestamp6)) = 2099", "true"))
                .add(new TestDataType("rnd_timestamp9", "timestamp(9)", "year(min(rnd_timestamp9)) = 1970 AND year(max(rnd_timestamp9)) = 2099", "true"))
                .add(new TestDataType("rnd_timestamp12", "timestamp(12)", "year(min(rnd_timestamp12)) = 1970 AND year(max(rnd_timestamp12)) = 2099", "true"))
                .add(new TestDataType("rnd_timestamptz", "timestamp with time zone", "year(min(rnd_timestamptz)) = 1970 AND year(max(rnd_timestamptz)) = 2099 AND bool_and(timezone_hour(rnd_timestamptz) = 0 AND timezone_minute(rnd_timestamptz) = 0)", "true"))
                .add(new TestDataType("rnd_timestamptz0", "timestamp(0) with time zone", "year(min(rnd_timestamptz0)) = 1970 AND year(max(rnd_timestamptz0)) = 2099 AND bool_and(timezone_hour(rnd_timestamptz0) = 0 AND timezone_minute(rnd_timestamptz0) = 0)", "true"))
                .add(new TestDataType("rnd_timestamptz6", "timestamp(6) with time zone", "year(min(rnd_timestamptz6)) = 1970 AND year(max(rnd_timestamptz6)) = 2099 AND bool_and(timezone_hour(rnd_timestamptz6) = 0 AND timezone_minute(rnd_timestamptz6) = 0)", "true"))
                .add(new TestDataType("rnd_timestamptz9", "timestamp(9) with time zone", "year(min(rnd_timestamptz9)) = 1970 AND year(max(rnd_timestamptz9)) = 2099 AND bool_and(timezone_hour(rnd_timestamptz9) = 0 AND timezone_minute(rnd_timestamptz9) = 0)", "true"))
                .add(new TestDataType("rnd_timestamptz12", "timestamp(12) with time zone", "year(min(rnd_timestamptz12)) = 1970 AND year(max(rnd_timestamptz12)) = 2099 AND bool_and(timezone_hour(rnd_timestamptz12) = 0 AND timezone_minute(rnd_timestamptz12) = 0)", "true"))
                .build();

        for (TestDataType testCase : testCases) {
            try (TestTable table = newTrinoTable("default_range_" + testCase.name(), "(%s) WITH (seed = 0)".formatted(testCase.columnSchema()))) {
                assertQuery("SELECT %s FROM %s".formatted(testCase.queryExpression(), table.getName()), "VALUES (%s)".formatted(testCase.expectedValue()));
            }
        }

        // with only one of min and max, the other bound defaults to the same window, unless it would make the range empty
        testCases = ImmutableList.<TestDataType>builder()
                .add(new TestDataType("rnd_date_min", "date", Map.of("min", "2050-01-01"), "min(rnd_date_min) >= DATE '2050-01-01' AND year(max(rnd_date_min)) = 2099", "true"))
                .add(new TestDataType("rnd_date_max", "date", Map.of("max", "2000-01-01"), "year(min(rnd_date_max)) = 1970 AND max(rnd_date_max) <= DATE '2000-01-01'", "true"))
                .add(new TestDataType("rnd_date_narrow_max", "date", Map.of("max", "1970-01-05"), "min(rnd_date_narrow_max) = DATE '1970-01-01' AND max(rnd_date_narrow_max) <= DATE '1970-01-05'", "true"))
                .add(new TestDataType("rnd_date_late_min", "date", Map.of("min", "2200-01-01"), "min(rnd_date_late_min) >= DATE '2200-01-01' AND year(max(rnd_date_late_min)) > 10000", "true"))
                .add(new TestDataType("rnd_date_early_max", "date", Map.of("max", "1960-01-01"), "year(min(rnd_date_early_max)) < -10000 AND max(rnd_date_early_max) <= DATE '1960-01-01'", "true"))
                .add(new TestDataType("rnd_timestamp_min", "timestamp", Map.of("min", "2050-01-01 00:00:00.000"), "min(rnd_timestamp_min) >= TIMESTAMP '2050-01-01 00:00:00.000' AND year(max(rnd_timestamp_min)) = 2099", "true"))
                .add(new TestDataType("rnd_timestamp_late_min", "timestamp", Map.of("min", "2200-01-01 00:00:00.000"), "min(rnd_timestamp_late_min) >= TIMESTAMP '2200-01-01 00:00:00.000' AND year(max(rnd_timestamp_late_min)) > 10000", "true"))
                .add(new TestDataType("rnd_timestamp9_max", "timestamp(9)", Map.of("max", "2000-01-01 00:00:00.000000000"), "year(min(rnd_timestamp9_max)) = 1970 AND max(rnd_timestamp9_max) <= TIMESTAMP '2000-01-01 00:00:00.000000000'", "true"))
                .add(new TestDataType("rnd_timestamp9_early_max", "timestamp(9)", Map.of("max", "1960-01-01 00:00:00.000000000"), "year(min(rnd_timestamp9_early_max)) < -10000 AND max(rnd_timestamp9_early_max) <= TIMESTAMP '1960-01-01 00:00:00.000000000'", "true"))
                .add(new TestDataType("rnd_timestamptz_min", "timestamp with time zone", Map.of("min", "2050-01-01 00:00:00.000 +01:00"), "min(rnd_timestamptz_min) >= TIMESTAMP '2050-01-01 00:00:00.000 +01:00' AND max(rnd_timestamptz_min) < TIMESTAMP '2100-01-01 00:00:00.000 UTC' AND bool_and(timezone_hour(rnd_timestamptz_min) = 1 AND timezone_minute(rnd_timestamptz_min) = 0)", "true"))
                .add(new TestDataType("rnd_timestamptz_early_max", "timestamp with time zone", Map.of("max", "1960-01-01 00:00:00.000 +01:00"), "year(min(rnd_timestamptz_early_max)) < -10000 AND max(rnd_timestamptz_early_max) <= TIMESTAMP '1960-01-01 00:00:00.000 +01:00'", "true"))
                .add(new TestDataType("rnd_timestamptz9_max", "timestamp(9) with time zone", Map.of("max", "2000-01-01 00:00:00.000000000 +01:00"), "min(rnd_timestamptz9_max) >= TIMESTAMP '1970-01-01 00:00:00.000000000 UTC' AND max(rnd_timestamptz9_max) <= TIMESTAMP '2000-01-01 00:00:00.000000000 +01:00' AND bool_and(timezone_hour(rnd_timestamptz9_max) = 1 AND timezone_minute(rnd_timestamptz9_max) = 0)", "true"))
                .add(new TestDataType("rnd_timestamptz9_late_min", "timestamp(9) with time zone", Map.of("min", "2200-01-01 00:00:00.000000000 +01:00"), "min(rnd_timestamptz9_late_min) >= TIMESTAMP '2200-01-01 00:00:00.000000000 +01:00' AND year(max(rnd_timestamptz9_late_min)) > 10000", "true"))
                .build();

        for (TestDataType testCase : testCases) {
            try (TestTable table = newTrinoTable("default_range_" + testCase.name(), "(%s) WITH (seed = 0)".formatted(testCase.columnSchema()))) {
                assertQuery("SELECT %s FROM %s".formatted(testCase.queryExpression(), table.getName()), "VALUES (%s)".formatted(testCase.expectedValue()));
            }
        }
    }

    @Test
    void testSelectValuesProperty()
    {
        // inclusive ranges that produce only 2 values
        // obtained using `Math.nextUp((float) 0.0)`
        List<TestDataType> testCases = ImmutableList.<TestDataType>builder()
                .add(new TestDataType("rnd_bigint", "bigint", Map.of("allowed_values", "ARRAY['0', '1']"), "count(distinct rnd_bigint)", "2"))
                .add(new TestDataType("rnd_integer", "integer", Map.of("allowed_values", "ARRAY['0', '1']"), "count(distinct rnd_integer)", "2"))
                .add(new TestDataType("rnd_smallint", "smallint", Map.of("allowed_values", "ARRAY['0', '1']"), "count(distinct rnd_smallint)", "2"))
                .add(new TestDataType("rnd_tinyint", "tinyint", Map.of("allowed_values", "ARRAY['0', '1']"), "count(distinct rnd_tinyint)", "2"))
                .add(new TestDataType("rnd_boolean", "boolean", Map.of("allowed_values", "ARRAY['true', 'false']"), "count(distinct rnd_boolean)", "2"))
                .add(new TestDataType("rnd_date", "date", Map.of("allowed_values", "ARRAY['2022-03-01', '2022-03-02']"), "count(distinct rnd_date)", "2"))
                .add(new TestDataType("rnd_decimal1", "decimal", Map.of("allowed_values", "ARRAY['0', '1']"), "count(distinct rnd_decimal1)", "2"))
                .add(new TestDataType("rnd_decimal2", "decimal(18,5)", Map.of("allowed_values", "ARRAY['0.00000', '0.00001']"), "count(distinct rnd_decimal2)", "2"))
                .add(new TestDataType("rnd_decimal3", "decimal(38,0)", Map.of("allowed_values", "ARRAY['0', '1']"), "count(distinct rnd_decimal3)", "2"))
                .add(new TestDataType("rnd_decimal4", "decimal(38,38)", Map.of("allowed_values", "ARRAY['0.00000000000000000000000000000000000000', '0.00000000000000000000000000000000000001']"), "count(distinct rnd_decimal4)", "2"))
                .add(new TestDataType("rnd_decimal5", "decimal(5,2)", Map.of("allowed_values", "ARRAY['0.00', '0.01']"), "count(distinct rnd_decimal5)", "2"))
                .add(new TestDataType("rnd_real", "real", Map.of("allowed_values", "ARRAY['0.0', '1.4E-45']"), "count(distinct rnd_real)", "2"))
                .add(new TestDataType("rnd_double", "double", Map.of("allowed_values", "ARRAY['0.0', '4.9E-324']"), "count(distinct rnd_double)", "2"))
                .add(new TestDataType("rnd_interval1", "interval day(2) to second(3)", Map.of("allowed_values", "ARRAY['0.000', '0.001']"), "count(distinct rnd_interval1)", "2"))
                .add(new TestDataType("rnd_interval2", "interval year(9) to month", Map.of("allowed_values", "ARRAY['0', '1']"), "count(distinct rnd_interval2)", "2"))
                .add(new TestDataType("rnd_timestamp", "timestamp", Map.of("allowed_values", "ARRAY['2022-03-21 00:00:00.000', '2022-03-21 00:00:00.001']"), "count(distinct rnd_timestamp)", "2"))
                .add(new TestDataType("rnd_timestamp0", "timestamp(0)", Map.of("allowed_values", "ARRAY['2022-03-21 00:00:00', '2022-03-21 00:00:01']"), "count(distinct rnd_timestamp0)", "2"))
                .add(new TestDataType("rnd_timestamp6", "timestamp(6)", Map.of("allowed_values", "ARRAY['2022-03-21 00:00:00.000000', '2022-03-21 00:00:00.000001']"), "count(distinct rnd_timestamp6)", "2"))
                .add(new TestDataType("rnd_timestamp9", "timestamp(9)", Map.of("allowed_values", "ARRAY['2022-03-21 00:00:00.000000000', '2022-03-21 00:00:00.000000001']"), "count(distinct rnd_timestamp9)", "2"))
                .add(new TestDataType("rnd_timestamptz", "timestamp with time zone", Map.of("allowed_values", "ARRAY['2022-03-21 00:00:00.000 +01:00', '2022-03-21 00:00:00.001 +01:00']"), "count(distinct rnd_timestamptz)", "2"))
                .add(new TestDataType("rnd_timestamptz0", "timestamp(0) with time zone", Map.of("allowed_values", "ARRAY['2022-03-21 00:00:00 +01:00', '2022-03-21 00:00:01 +01:00']"), "count(distinct rnd_timestamptz0)", "2"))
                .add(new TestDataType("rnd_timestamptz6", "timestamp(6) with time zone", Map.of("allowed_values", "ARRAY['2022-03-21 00:00:00.000000 +01:00', '2022-03-21 00:00:00.000001 +01:00']"), "count(distinct rnd_timestamptz6)", "2"))
                .add(new TestDataType("rnd_timestamptz9", "timestamp(9) with time zone", Map.of("allowed_values", "ARRAY['2022-03-21 00:00:00.000000000 +01:00', '2022-03-21 00:00:00.000000001 +01:00']"), "count(distinct rnd_timestamptz9)", "2"))
                .add(new TestDataType("rnd_time", "time", Map.of("allowed_values", "ARRAY['01:02:03.456', '01:02:03.457']"), "count(distinct rnd_time)", "2"))
                .add(new TestDataType("rnd_time0", "time(0)", Map.of("allowed_values", "ARRAY['01:02:03', '01:02:04']"), "count(distinct rnd_time0)", "2"))
                .add(new TestDataType("rnd_time6", "time(6)", Map.of("allowed_values", "ARRAY['01:02:03.000456', '01:02:03.000457']"), "count(distinct rnd_time6)", "2"))
                .add(new TestDataType("rnd_time9", "time(9)", Map.of("allowed_values", "ARRAY['01:02:03.000000456', '01:02:03.000000457']"), "count(distinct rnd_time9)", "2"))
                .add(new TestDataType("rnd_timetz", "time with time zone", Map.of("allowed_values", "ARRAY['01:02:03.456 +01:00', '01:02:03.457 +01:00']"), "count(distinct rnd_timetz)", "2"))
                .add(new TestDataType("rnd_timetz0", "time(0) with time zone", Map.of("allowed_values", "ARRAY['01:02:03 +01:00', '01:02:04 +01:00']"), "count(distinct rnd_timetz0)", "2"))
                .add(new TestDataType("rnd_timetz6", "time(6) with time zone", Map.of("allowed_values", "ARRAY['01:02:03.000456 +01:00', '01:02:03.000457 +01:00']"), "count(distinct rnd_timetz6)", "2"))
                .add(new TestDataType("rnd_timetz9", "time(9) with time zone", Map.of("allowed_values", "ARRAY['01:02:03.000000456 +01:00', '01:02:03.000000457 +01:00']"), "count(distinct rnd_timetz9)", "2"))
                .add(new TestDataType("rnd_timetz12", "time(12) with time zone", Map.of("allowed_values", "ARRAY['01:02:03.000000000456 +01:00', '01:02:03.000000000457 +01:00']"), "count(distinct rnd_timetz12)", "2"))
                .add(new TestDataType("rnd_varbinary", "varbinary", Map.of("allowed_values", "ARRAY['ff', '00']"), "count(distinct rnd_varbinary)", "2"))
                .add(new TestDataType("rnd_varchar", "varchar", Map.of("allowed_values", "ARRAY['aa', 'bb']"), "count(distinct rnd_varchar)", "2"))
                .add(new TestDataType("rnd_nvarchar", "varchar(1000)", Map.of("allowed_values", "ARRAY['aa', 'bb']"), "count(distinct rnd_nvarchar)", "2"))
                .add(new TestDataType("rnd_ipaddress", "ipaddress", Map.of("allowed_values", "ARRAY['0.0.0.0', '1.2.3.4']"), "count(distinct rnd_ipaddress)", "2"))
                .add(new TestDataType("rnd_uuid", "uuid", Map.of("allowed_values", "ARRAY['1fc74d96-0216-449b-a145-455578a9eaa5', '3ee49ede-0026-45e4-ba06-08404f794557']"), "count(distinct rnd_uuid)", "2"))
                .build();

        for (TestDataType testCase : testCases) {
            try (TestTable table = newTrinoTable("values_" + testCase.name(), "(%s)".formatted(testCase.columnSchema()))) {
                assertQuery("SELECT %s FROM %s".formatted(testCase.queryExpression(), table.getName()), "VALUES (%s)".formatted(testCase.expectedValue()));
            }
        }

        assertQueryFails(
                "CREATE TABLE empty_values (value bigint WITH (allowed_values = ARRAY[]))",
                "The `allowed_values` property must contain at least one value");
    }

    @Test
    void testOutOfRangeIntegerProperties()
    {
        assertQueryFails(
                "CREATE TABLE out_of_range_integer (v integer WITH (min = '2147483648'))",
                "The `min` property must be a valid integer literal");
        assertQueryFails(
                "CREATE TABLE out_of_range_smallint (v smallint WITH (max = '-32769'))",
                "The `max` property must be a valid smallint literal");
        assertQueryFails(
                "CREATE TABLE out_of_range_tinyint (v tinyint WITH (allowed_values = ARRAY['0', '128']))",
                "The `allowed_values` property must only contain valid tinyint literals, failed to parse `128`");
    }

    @Test
    void testDecimalProperties()
    {
        try (TestTable table = newTrinoTable("decimal_properties", "(v decimal(5, 2) NOT NULL WITH (allowed_values = ARRAY['0.1', '1', '2.345']))")) {
            assertQuery("SELECT DISTINCT v FROM " + table.getName(), "VALUES 0.10, 1.00, 2.35");
        }
        assertQueryFails(
                "CREATE TABLE out_of_range_decimal (v decimal(5, 2) WITH (max = '1000'))",
                "The `max` property must be a valid decimal\\(5,2\\) literal");
    }

    @Test
    void testSelectStepProperties()
    {
        // small step in small ranges that produce only 10 unique values for 1000 rows
        List<TestDataType> testCases = ImmutableList.<TestDataType>builder()
                .add(new TestDataType("rnd_bigint", "bigint", Map.of("min", "0", "max", "9", "step", "1"), "count(distinct rnd_bigint)", "10"))
                .add(new TestDataType("rnd_integer", "integer", Map.of("min", "0", "max", "9", "step", "1"), "count(distinct rnd_integer)", "10"))
                // after the last full step, the max value is generated, which table statistics also count
                .add(new TestDataType("rnd_integer_step2", "integer", Map.of("min", "0", "max", "9", "step", "2"), "count(distinct rnd_integer_step2)", "6"))
                .add(new TestDataType("rnd_smallint", "smallint", Map.of("min", "0", "max", "9", "step", "1"), "count(distinct rnd_smallint)", "10"))
                .add(new TestDataType("rnd_tinyint", "tinyint", Map.of("min", "0", "max", "9", "step", "1"), "count(distinct rnd_tinyint)", "10"))
                .add(new TestDataType("rnd_date", "date", Map.of("min", "2022-03-01", "max", "2022-03-10", "step", "1d"), "count(distinct rnd_date)", "10"))
                .add(new TestDataType("rnd_decimal1", "decimal", Map.of("min", "0", "max", "9", "step", "1"), "count(distinct rnd_decimal1)", "10"))
                .add(new TestDataType("rnd_decimal2", "decimal(18,5)", Map.of("min", "0.00000", "max", "0.00009", "step", "0.00001"), "count(distinct rnd_decimal2)", "10"))
                .add(new TestDataType("rnd_decimal3", "decimal(38,0)", Map.of("min", "0", "max", "9", "step", "1"), "count(distinct rnd_decimal3)", "10"))
                .add(new TestDataType("rnd_decimal4", "decimal(38,38)", Map.of("min", "0.00000000000000000000000000000000000000", "max", "0.00000000000000000000000000000000000009", "step", "0.00000000000000000000000000000000000001"), "count(distinct rnd_decimal4)", "10"))
                .add(new TestDataType("rnd_decimal5", "decimal(5,2)", Map.of("min", "0.00", "max", "1.09", "step", "0.01"), "count(distinct rnd_decimal5)", "110"))
                .add(new TestDataType("rnd_real", "real", Map.of("min", "0.0", "max", "1.3E-44", "step", "1.4E-45"), "count(distinct rnd_real)", "10"))
                .add(new TestDataType("rnd_double", "double", Map.of("min", "0.0", "max", "4.4E-323", "step", "4.9E-324"), "count(distinct rnd_double)", "10"))
                .add(new TestDataType("rnd_interval1", "interval day(2) to second(3)", Map.of("min", "0.000", "max", "0.009", "step", "0.001"), "count(distinct rnd_interval1)", "10"))
                .add(new TestDataType("rnd_interval2", "interval year(9) to month", Map.of("min", "0", "max", "9", "step", "1"), "count(distinct rnd_interval2)", "10"))
                .add(new TestDataType("rnd_timestamp", "timestamp", Map.of("min", "2022-03-21 00:00:00.000", "max", "2022-03-21 00:00:00.009", "step", "1ms"), "count(distinct rnd_timestamp)", "10"))
                .add(new TestDataType("rnd_timestamp0", "timestamp(0)", Map.of("min", "2022-03-21 00:00:00", "max", "2022-03-21 00:00:09", "step", "1s"), "count(distinct rnd_timestamp0)", "10"))
                .add(new TestDataType("rnd_timestamp6", "timestamp(6)", Map.of("min", "2022-03-21 00:00:00.000000", "max", "2022-03-21 00:00:00.000009", "step", "1us"), "count(distinct rnd_timestamp6)", "10"))
                .add(new TestDataType("rnd_timestamp9", "timestamp(9)", Map.of("min", "2022-03-21 00:00:00.000000000", "max", "2022-03-21 00:00:00.000009000", "step", "1us"), "count(distinct rnd_timestamp9)", "10"))
                .add(new TestDataType("rnd_timestamptz", "timestamp with time zone", Map.of("min", "2022-03-21 00:00:00.000 +01:00", "max", "2022-03-21 00:00:00.009 +01:00", "step", "1ms"), "count(distinct rnd_timestamptz)", "10"))
                .add(new TestDataType("rnd_timestamptz0", "timestamp(0) with time zone", Map.of("min", "2022-03-21 00:00:00 +01:00", "max", "2022-03-21 00:00:09 +01:00", "step", "1s"), "count(distinct rnd_timestamptz0)", "10"))
                .add(new TestDataType("rnd_timestamptz6", "timestamp(6) with time zone", Map.of("min", "2022-03-21 00:00:00.000000 +01:00", "max", "2022-03-21 00:00:00.009000 +01:00", "step", "1ms"), "count(distinct rnd_timestamptz6)", "10"))
                .add(new TestDataType("rnd_timestamptz9", "timestamp(9) with time zone", Map.of("min", "2022-03-21 00:00:00.000000000 +01:00", "max", "2022-03-21 00:00:00.009000000 +01:00", "step", "1ms"), "count(distinct rnd_timestamptz9)", "10"))
                .add(new TestDataType("rnd_time", "time", Map.of("min", "01:02:03.456", "max", "01:02:03.465", "step", "1ms"), "count(distinct rnd_time)", "10"))
                .add(new TestDataType("rnd_time0", "time(0)", Map.of("min", "01:02:03", "max", "01:02:12", "step", "1s"), "count(distinct rnd_time0)", "10"))
                .add(new TestDataType("rnd_time6", "time(6)", Map.of("min", "01:02:03.000456", "max", "01:02:03.000465", "step", "1us"), "count(distinct rnd_time6)", "10"))
                .add(new TestDataType("rnd_time9", "time(9)", Map.of("min", "01:02:03.000000456", "max", "01:02:03.000000465", "step", "1ns"), "count(distinct rnd_time9)", "10"))
                .add(new TestDataType("rnd_timetz", "time with time zone", Map.of("min", "01:02:03.456 +01:00", "max", "01:02:03.465 +01:00", "step", "1ms"), "count(distinct rnd_timetz)", "10"))
                .add(new TestDataType("rnd_timetz0", "time(0) with time zone", Map.of("min", "01:02:03 +01:00", "max", "01:02:12 +01:00", "step", "1s"), "count(distinct rnd_timetz0)", "10"))
                .add(new TestDataType("rnd_timetz6", "time(6) with time zone", Map.of("min", "01:02:03.000456 +01:00", "max", "01:02:03.000465 +01:00", "step", "1us"), "count(distinct rnd_timetz6)", "10"))
                .add(new TestDataType("rnd_timetz9", "time(9) with time zone", Map.of("min", "01:02:03.000000456 +01:00", "max", "01:02:03.000000465 +01:00", "step", "1ns"), "count(distinct rnd_timetz9)", "10"))
                .add(new TestDataType("rnd_timetz12", "time(12) with time zone", Map.of("min", "01:02:03.000000000456 +01:00", "max", "01:02:03.000000009456 +01:00", "step", "1ns"), "count(distinct rnd_timetz12)", "10"))
                .build();

        for (TestDataType testCase : testCases) {
            try (TestTable table = newTrinoTable("step_small_" + testCase.name(), "(%s)".formatted(testCase.columnSchema()))) {
                assertQuery("SELECT %s FROM %s".formatted(testCase.queryExpression(), table.getName()), "VALUES (%s)".formatted(testCase.expectedValue()));
            }
        }

        // small step in small ranges at the max high bound that produce only 10 unique values for 1000 rows
        testCases = ImmutableList.<TestDataType>builder()
                .add(new TestDataType("rnd_bigint", "bigint", Map.of("min", "9223372036854775798", "max", "9223372036854775807", "step", "1"), "count(distinct rnd_bigint) = 10 AND min(rnd_bigint) = 9223372036854775798 AND max(rnd_bigint) = 9223372036854775807", "true"))
                .add(new TestDataType("rnd_integer", "integer", Map.of("min", "2147483638", "max", "2147483647", "step", "1"), "count(distinct rnd_integer) = 10 AND min(rnd_integer) = 2147483638 AND max(rnd_integer) = 2147483647", "true"))
                .add(new TestDataType("rnd_date", "date", Map.of("min", "5881580-07-02", "max", "5881580-07-11", "step", "1d"), "count(distinct rnd_date) = 10 AND min(rnd_date) = DATE '5881580-07-02' AND max(rnd_date) = DATE '5881580-07-11'", "true"))
                .add(new TestDataType("rnd_timestamp6", "timestamp(6)", Map.of("min", "294247-01-10 04:00:54.775798", "max", "294247-01-10 04:00:54.775807", "step", "1us"), "count(distinct rnd_timestamp6) = 10 AND min(rnd_timestamp6) = TIMESTAMP '294247-01-10 04:00:54.775798' AND max(rnd_timestamp6) = TIMESTAMP '294247-01-10 04:00:54.775807'", "true"))
                .add(new TestDataType("rnd_timestamp9", "timestamp(9)", Map.of("min", "294247-01-10 04:00:54.775798000", "max", "294247-01-10 04:00:54.775807999", "step", "1us"), "count(distinct rnd_timestamp9) = 11 AND min(rnd_timestamp9) = TIMESTAMP '294247-01-10 04:00:54.775798000' AND max(rnd_timestamp9) = TIMESTAMP '294247-01-10 04:00:54.775807999'", "true"))
                .build();

        for (TestDataType testCase : testCases) {
            try (TestTable table = newTrinoTable("step_top_" + testCase.name(), "(%s)".formatted(testCase.columnSchema()))) {
                assertQuery("SELECT %s FROM %s".formatted(testCase.queryExpression(), table.getName()), "VALUES (%s)".formatted(testCase.expectedValue()));
            }
        }

        // steps shorter than the unit of the stored value, like a microsecond or a millisecond
        testCases = ImmutableList.<TestDataType>builder()
                .add(new TestDataType("rnd_timestamp9", "timestamp(9)", Map.of("min", "2020-01-01 00:00:00.000000000", "step", "1ns"), "count(distinct rnd_timestamp9) = 1000 AND max(rnd_timestamp9) = TIMESTAMP '2020-01-01 00:00:00.000000999'", "true"))
                .add(new TestDataType("rnd_timestamp12", "timestamp(12)", Map.of("min", "2020-01-01 00:00:00.000000000001", "step", "1ns"), "count(distinct rnd_timestamp12) = 1000 AND max(rnd_timestamp12) = TIMESTAMP '2020-01-01 00:00:00.000000999001'", "true"))
                .add(new TestDataType("rnd_timestamptz6", "timestamp(6) with time zone", Map.of("min", "2020-01-01 00:00:00.000000 UTC", "step", "1us"), "count(distinct rnd_timestamptz6) = 1000 AND max(rnd_timestamptz6) = TIMESTAMP '2020-01-01 00:00:00.000999 UTC'", "true"))
                .add(new TestDataType("rnd_timestamptz9", "timestamp(9) with time zone", Map.of("min", "2020-01-01 00:00:00.000000001 UTC", "max", "2020-01-01 00:00:00.000000500 UTC", "step", "1ns"), "count(distinct rnd_timestamptz9) = 500 AND max(rnd_timestamptz9) = TIMESTAMP '2020-01-01 00:00:00.000000500 UTC'", "true"))
                .build();

        for (TestDataType testCase : testCases) {
            try (TestTable table = newTrinoTable("step_fraction_" + testCase.name(), "(%s)".formatted(testCase.columnSchema()))) {
                assertQuery("SELECT %s FROM %s".formatted(testCase.queryExpression(), table.getName()), "VALUES (%s)".formatted(testCase.expectedValue()));
            }
        }

        // without a max high bound, sequences stop at the max value of the type
        testCases = ImmutableList.<TestDataType>builder()
                .add(new TestDataType("rnd_bigint", "bigint", Map.of("min", "9223372036854775807", "step", "1"), "count(distinct rnd_bigint) = 1 AND min(rnd_bigint) = 9223372036854775807", "true"))
                .add(new TestDataType("rnd_integer", "integer", Map.of("min", "2147483638", "step", "1"), "count(distinct rnd_integer) = 10 AND max(rnd_integer) = 2147483647", "true"))
                .add(new TestDataType("rnd_smallint", "smallint", Map.of("min", "32758", "step", "1"), "count(distinct rnd_smallint) = 10 AND max(rnd_smallint) = 32767", "true"))
                .add(new TestDataType("rnd_tinyint", "tinyint", Map.of("min", "118", "step", "1"), "count(distinct rnd_tinyint) = 10 AND max(rnd_tinyint) = 127", "true"))
                .build();

        for (TestDataType testCase : testCases) {
            try (TestTable table = newTrinoTable("step_unbounded_top_" + testCase.name(), "(%s)".formatted(testCase.columnSchema()))) {
                assertQuery("SELECT %s FROM %s".formatted(testCase.queryExpression(), table.getName()), "VALUES (%s)".formatted(testCase.expectedValue()));
            }
        }

        // long sequences with steps large enough to overflow when multiplied by the row number
        testCases = ImmutableList.<TestDataType>builder()
                .add(new TestDataType("rnd_date", "date", Map.of("min", "2000-01-01", "step", "1d"), "count(distinct rnd_date) = 200000 AND min(rnd_date) = DATE '2000-01-01' AND max(rnd_date) = DATE '2000-01-01' + INTERVAL '199999' DAY", "true"))
                .add(new TestDataType("rnd_timestamp", "timestamp", Map.of("min", "2000-01-01 00:00:00.000", "step", "1d"), "count(distinct rnd_timestamp) = 200000 AND min(rnd_timestamp) = TIMESTAMP '2000-01-01 00:00:00.000' AND max(rnd_timestamp) = TIMESTAMP '2000-01-01 00:00:00.000' + INTERVAL '199999' DAY", "true"))
                .add(new TestDataType("rnd_bigint", "bigint", Map.of("min", "0", "step", "100000000000000"), "count(distinct rnd_bigint) = 92235 AND min(rnd_bigint) = 0", "true"))
                .add(new TestDataType("rnd_bigint_wide", "bigint", Map.of("min", "-9223372036854775808", "max", "9223372036854775807", "step", "9223372036854775807"), "count(distinct rnd_bigint_wide) = 4 AND max(rnd_bigint_wide) = 9223372036854775807", "true"))
                .add(new TestDataType("rnd_decimal_wide", "decimal(38,0)", Map.of("min", "-99999999999999999999999999999999999999", "max", "99999999999999999999999999999999999999", "step", "99999999999999999999999999999999999999"), "count(distinct rnd_decimal_wide) = 3 AND max(rnd_decimal_wide) = DECIMAL '99999999999999999999999999999999999999'", "true"))
                .build();

        for (TestDataType testCase : testCases) {
            try (TestTable table = newTrinoTable("step_long_" + testCase.name(), "(%s) WITH (default_limit = 200000)".formatted(testCase.columnSchema()))) {
                assertQuery("SELECT %s FROM %s".formatted(testCase.queryExpression(), table.getName()), "VALUES (%s)".formatted(testCase.expectedValue()));
            }
        }

        // without a min low bound, sequences start at the min value of the type, or at 1970-01-01 for date and timestamp types, unless the max is earlier
        testCases = ImmutableList.<TestDataType>builder()
                .add(new TestDataType("rnd_bigint", "bigint", Map.of("max", "0", "step", "1"), "min(rnd_bigint) = -9223372036854775808 AND max(rnd_bigint) < 0", "true"))
                .add(new TestDataType("rnd_date", "date", Map.of("max", "2022-03-21", "step", "1d"), "min(rnd_date) = DATE '1970-01-01' AND max(rnd_date) = DATE '1970-01-01' + INTERVAL '999' DAY", "true"))
                .add(new TestDataType("rnd_timestamp", "timestamp", Map.of("max", "2022-03-21 00:00:00.000", "step", "1s"), "min(rnd_timestamp) = TIMESTAMP '1970-01-01 00:00:00.000' AND max(rnd_timestamp) < TIMESTAMP '2022-03-21 00:00:00.000'", "true"))
                .add(new TestDataType("rnd_timestamp9", "timestamp(9)", Map.of("max", "2022-03-21 00:00:00.000000000", "step", "1s"), "min(rnd_timestamp9) = TIMESTAMP '1970-01-01 00:00:00.000000000' AND max(rnd_timestamp9) < TIMESTAMP '2022-03-21 00:00:00.000000000'", "true"))
                .add(new TestDataType("rnd_timestamptz", "timestamp with time zone", Map.of("max", "2022-03-21 00:00:00.000 +01:00", "step", "1s"), "min(rnd_timestamptz) = TIMESTAMP '1970-01-01 00:00:00.000 UTC' AND max(rnd_timestamptz) < TIMESTAMP '2022-03-21 00:00:00.000 +01:00'", "true"))
                .add(new TestDataType("rnd_timestamptz9", "timestamp(9) with time zone", Map.of("max", "2022-03-21 00:00:00.000000000 +01:00", "step", "1s"), "min(rnd_timestamptz9) = TIMESTAMP '1970-01-01 00:00:00.000000000 UTC' AND max(rnd_timestamptz9) < TIMESTAMP '2022-03-21 00:00:00.000000000 +01:00'", "true"))
                // sequences don't stop at the default high bound, so they keep generating unique values
                .add(new TestDataType("rnd_date_unbounded", "date", Map.of("step", "100d"), "min(rnd_date_unbounded) = DATE '1970-01-01' AND year(max(rnd_date_unbounded)) > 2100 AND count(distinct rnd_date_unbounded) = 1000", "true"))
                .add(new TestDataType("rnd_timestamp_early", "timestamp", Map.of("max", "1960-01-01 00:00:00.000", "step", "1s"), "year(min(rnd_timestamp_early)) < -290000 AND max(rnd_timestamp_early) < TIMESTAMP '1960-01-01 00:00:00.000'", "true"))
                .add(new TestDataType("rnd_timestamp0_early", "timestamp(0)", Map.of("max", "1960-01-01 00:00:00", "step", "1s"), "year(min(rnd_timestamp0_early)) < -290000 AND max(rnd_timestamp0_early) < TIMESTAMP '1960-01-01 00:00:00'", "true"))
                .build();

        for (TestDataType testCase : testCases) {
            try (TestTable table = newTrinoTable("step_bottom_" + testCase.name(), "(%s)".formatted(testCase.columnSchema()))) {
                assertQuery("SELECT %s FROM %s".formatted(testCase.queryExpression(), table.getName()), "VALUES (%s)".formatted(testCase.expectedValue()));
            }
        }
    }

    @Test
    void testTableStatistics()
    {
        String schema =
                """
                (
                  bigint_range bigint NOT NULL WITH (min = '0', max = '9'),
                  bigint_capped bigint NOT NULL WITH (min = '0', max = '1000'),
                  bigint_full bigint NOT NULL WITH (min = '-9223372036854775808', max = '0'),
                  bigint_nulls bigint WITH (min = '0', max = '9', null_probability = 1),
                  integer_step integer NOT NULL WITH (min = '0', max = '9', step = '2'),
                  smallint_step smallint NOT NULL WITH (min = '0', max = '10', step = '5'),
                  tinyint_step tinyint NOT NULL WITH (min = '-5', max = '5', step = '1'),
                  decimal_short decimal(5, 2) NOT NULL WITH (min = '0.00', max = '0.09'),
                  decimal_long decimal(38, 0) NOT NULL WITH (min = '0', max = '9'),
                  real_range real NOT NULL WITH (min = '0', max = '1'),
                  double_range double NOT NULL WITH (min = '0', max = '1'),
                  date_step date NOT NULL WITH (min = '2022-03-01', max = '2022-03-10', step = '1d'),
                  date_partial_step date NOT NULL WITH (min = '2022-03-01', max = '2022-03-10', step = '36h'),
                  timestamp_step timestamp(3) NOT NULL WITH (min = '2022-03-21 00:00:00.000', max = '2022-03-21 00:00:00.009', step = '1ms'),
                  timestamp0_step timestamp(0) NOT NULL WITH (min = '2022-03-21 00:00:00', max = '2022-03-21 00:00:09', step = '2s'),
                  timestamptz_step timestamp(3) with time zone NOT NULL WITH (min = '2022-03-21 00:00:00.000 +01:00', max = '2022-03-21 00:00:00.009 +01:00', step = '1ms'),
                  time_step time(3) NOT NULL WITH (min = '01:02:03.456', max = '01:02:03.465', step = '1ms'),
                  time12_step time(12) NOT NULL WITH (min = '01:02:03.000000000000', max = '01:02:03.000000009000', step = '1ns'),
                  time12_late time(12) NOT NULL WITH (min = '23:59:59.999999999998', max = '23:59:59.999999999999'),
                  timetz_range time(3) with time zone NOT NULL WITH (min = '01:02:03.456 +01:00', max = '01:02:03.465 +01:00'),
                  interval_day_step interval day(2) to second(3) NOT NULL WITH (min = '0.000', max = '0.009', step = '0.001'),
                  interval_year_range interval year(9) to month NOT NULL WITH (min = '0', max = '9'),
                  allowed bigint NOT NULL WITH (allowed_values = ARRAY['1', '2', '3']),
                  allowed_duplicates bigint NOT NULL WITH (allowed_values = ARRAY['1', '1', '2']),
                  unbounded bigint NOT NULL
                ) WITH (default_limit = 100)
                """;
        try (TestTable table = newTrinoTable("statistics", schema)) {
            assertQuery(
                    "SHOW STATS FOR " + table.getName(),
                    """
                    VALUES
                      ('bigint_range', NULL, 10, 0, NULL, '0', '9'),
                      ('bigint_capped', NULL, 100, 0, NULL, '0', '1000'),
                      ('bigint_full', NULL, 100, 0, NULL, '-9223372036854775808', '0'),
                      ('bigint_nulls', 0, 0, 1, NULL, NULL, NULL),
                      ('integer_step', NULL, 6, 0, NULL, '0', '9'),
                      ('smallint_step', NULL, 3, 0, NULL, '0', '10'),
                      ('tinyint_step', NULL, 11, 0, NULL, '-5', '5'),
                      ('decimal_short', NULL, 10, 0, NULL, '0.0', '0.09'),
                      ('decimal_long', NULL, NULL, 0, NULL, '0.0', '9.0'),
                      ('real_range', NULL, NULL, 0, NULL, '0.0', '1.0'),
                      ('double_range', NULL, NULL, 0, NULL, '0.0', '1.0'),
                      ('date_step', NULL, 10, 0, NULL, '2022-03-01', '2022-03-10'),
                      ('date_partial_step', NULL, NULL, 0, NULL, '2022-03-01', '2022-03-10'),
                      ('timestamp_step', NULL, 10, 0, NULL, '2022-03-21 00:00:00.000', '2022-03-21 00:00:00.009'),
                      ('timestamp0_step', NULL, 6, 0, NULL, '2022-03-21 00:00:00', '2022-03-21 00:00:09'),
                      ('timestamptz_step', NULL, 10, 0, NULL, '2022-03-20 23:00:00.000 UTC', '2022-03-20 23:00:00.009 UTC'),
                      ('time_step', NULL, 10, 0, NULL, NULL, NULL),
                      ('time12_step', NULL, 10, 0, NULL, NULL, NULL),
                      ('time12_late', NULL, 2, 0, NULL, NULL, NULL),
                      ('timetz_range', NULL, NULL, 0, NULL, NULL, NULL),
                      ('interval_day_step', NULL, 10, 0, NULL, NULL, NULL),
                      ('interval_year_range', NULL, 10, 0, NULL, NULL, NULL),
                      ('allowed', NULL, 3, 0, NULL, NULL, NULL),
                      ('allowed_duplicates', NULL, 2, 0, NULL, NULL, NULL),
                      ('unbounded', NULL, NULL, 0, NULL, NULL, NULL),
                      (NULL, NULL, NULL, NULL, 100, NULL, NULL)
                    """);
        }

        try (TestTable table = newTrinoTable("statistics_capped", "(bigint_range bigint NOT NULL WITH (min = '0', max = '9'), allowed bigint NOT NULL WITH (allowed_values = ARRAY['1', '2', '3'])) WITH (default_limit = 2)")) {
            assertQuery(
                    "SHOW STATS FOR " + table.getName(),
                    """
                    VALUES
                      ('bigint_range', NULL, 2, 0, NULL, '0', '9'),
                      ('allowed', NULL, 2, 0, NULL, NULL, NULL),
                      (NULL, NULL, NULL, NULL, 2, NULL, NULL)
                    """);
        }
    }

    @Test
    void testNonPositiveStepProperty()
    {
        // durations can't be negative, so only a zero step is tested for date and time types
        List<String> columnSchemas = ImmutableList.of(
                "bigint WITH (step = '-1')",
                "bigint WITH (step = '0')",
                "integer WITH (step = '-1')",
                "smallint WITH (step = '-1')",
                "tinyint WITH (step = '-1')",
                "decimal(18,5) WITH (step = '-0.00001')",
                "decimal(38,0) WITH (step = '-1')",
                "decimal(38,0) WITH (step = '0')",
                "real WITH (step = '-1.4E-45')",
                "real WITH (step = '-0.0')",
                "real WITH (step = 'NaN')",
                "real WITH (step = 'Infinity')",
                "double WITH (step = '-4.9E-324')",
                "double WITH (step = '0.0')",
                "double WITH (step = 'NaN')",
                "double WITH (step = 'Infinity')",
                "interval day(2) to second(3) WITH (step = '-0.001')",
                "interval year(9) to month WITH (step = '0')",
                "date WITH (step = '0d')",
                "timestamp WITH (step = '0ms')",
                "timestamp(9) WITH (step = '0ms')",
                "timestamp with time zone WITH (step = '0ms')",
                "timestamp(9) with time zone WITH (step = '0ms')",
                "time WITH (step = '0ms')",
                "time with time zone WITH (step = '0ms')",
                "time(12) with time zone WITH (step = '0ms')");

        for (String columnSchema : columnSchemas) {
            assertThat(query("CREATE TABLE step_not_positive (value %s)".formatted(columnSchema)))
                    .failure()
                    .hasErrorCode(INVALID_COLUMN_PROPERTY)
                    .hasMessage("The `step` property must be greater than zero and finite");
        }
    }

    @Test
    void testDateTimePropertiesMorePreciseThanColumn()
    {
        Map<String, String> columnSchemas = ImmutableMap.<String, String>builder()
                .put("timestamp(3) WITH (max = '2020-01-01 00:00:00.0005')", "The `max` property must be a valid timestamp(3) literal")
                .put("time(0) WITH (min = '01:02:03.5')", "The `min` property must be a valid time(0) literal")
                .put("time(9) with time zone WITH (max = '01:02:03.0000000001 +01:00')", "The `max` property must be a valid time(9) with time zone literal")
                .put("timestamp(0) with time zone WITH (allowed_values = ARRAY['2020-01-01 00:00:00.1 UTC'])",
                        "The `allowed_values` property must only contain valid timestamp(0) with time zone literals, failed to parse `2020-01-01 00:00:00.1 UTC`")
                .buildOrThrow();

        for (Map.Entry<String, String> entry : columnSchemas.entrySet()) {
            assertThat(query("CREATE TABLE too_precise (value %s)".formatted(entry.getKey())))
                    .failure()
                    .hasErrorCode(INVALID_COLUMN_PROPERTY)
                    .hasMessage(entry.getValue());
        }

        // trailing zeros don't add precision
        try (TestTable table = newTrinoTable("trailing_zeros", "(value timestamp(0) NOT NULL WITH (min = '2020-01-01 00:00:00.000', max = '2020-01-01 00:00:09.000'))")) {
            assertQuery("SELECT count(DISTINCT value) FROM " + table.getName(), "VALUES 10");
        }
    }

    @Test
    void testStepPropertyShorterThanPrecision()
    {
        Map<String, String> columnSchemas = ImmutableMap.<String, String>builder()
                .put("date WITH (step = '12h')", "The `step` property for a date column must be at least 1d")
                .put("timestamp(0) WITH (step = '500ms')", "The `step` property for a timestamp(0) column must be at least 1s")
                .put("timestamp(3) WITH (step = '1us')", "The `step` property for a timestamp(3) column must be at least 1ms")
                .put("timestamp(6) with time zone WITH (step = '1ns')", "The `step` property for a timestamp(6) with time zone column must be at least 1us")
                .put("time(1) WITH (step = '10ms')", "The `step` property for a time(1) column must be at least 100ms")
                .put("time(3) with time zone WITH (step = '1us')", "The `step` property for a time(3) with time zone column must be at least 1ms")
                .buildOrThrow();

        for (Map.Entry<String, String> entry : columnSchemas.entrySet()) {
            assertThat(query("CREATE TABLE step_too_short (value %s)".formatted(entry.getKey())))
                    .failure()
                    .hasErrorCode(INVALID_COLUMN_PROPERTY)
                    .hasMessage(entry.getValue());
        }
    }

    @Test
    void testStepPropertyUnsupportedType()
    {
        for (String type : ImmutableList.of("boolean", "uuid", "ipaddress", "json", "array(bigint)")) {
            assertThat(query("CREATE TABLE step_not_supported (value %s WITH (step = '1'))".formatted(type)))
                    .failure()
                    .hasErrorCode(INVALID_COLUMN_PROPERTY)
                    .hasMessage("The `step` property cannot be set for %s columns".formatted(type));
        }
    }

    @Test
    void testNormalDistribution()
    {
        List<DistributionTestCase> testCases = ImmutableList.of(
                new DistributionTestCase("bigint", "0", "1000"),
                new DistributionTestCase("bigint", "-9223372036854775808", "9223372036854775807"),
                new DistributionTestCase("integer", "-1000", "1000"),
                new DistributionTestCase("smallint", "0", "100"),
                new DistributionTestCase("tinyint", "-128", "127"),
                new DistributionTestCase("decimal(10,2)", "0.00", "100.00"),
                new DistributionTestCase("decimal(38,2)", "0.00", "100.00"),
                new DistributionTestCase("decimal(38,0)", "-99999999999999999999999999999999999999", "99999999999999999999999999999999999999"),
                new DistributionTestCase("real", "0.0", "1.0"),
                new DistributionTestCase("double", "-1.0", "1.0"),
                new DistributionTestCase("double", "-1.7976931348623157E308", "1.7976931348623157E308"),
                new DistributionTestCase("date", "2020-01-01", "2020-12-31", "CAST(date_diff('day', DATE '1970-01-01', %s) AS double)"),
                new DistributionTestCase("timestamp(0)", "2020-01-01 00:00:00", "2020-01-02 00:00:00", "CAST(date_diff('millisecond', TIMESTAMP '1970-01-01', %s) AS double)"),
                new DistributionTestCase("timestamp(3)", "2020-01-01 00:00:00.000", "2020-01-02 00:00:00.000", "CAST(date_diff('millisecond', TIMESTAMP '1970-01-01', %s) AS double)"),
                new DistributionTestCase("timestamp(6)", "2020-01-01 00:00:00.000000", "2020-01-02 00:00:00.000000", "CAST(date_diff('millisecond', TIMESTAMP '1970-01-01', %s) AS double)"),
                new DistributionTestCase("timestamp(9)", "2020-01-01 00:00:00.000000000", "2020-01-02 00:00:00.000000000", "CAST(date_diff('millisecond', TIMESTAMP '1970-01-01', %s) AS double)"));

        for (DistributionTestCase testCase : testCases) {
            try (TestTable table = newTrinoTable("normal_distribution", "(%s) WITH (default_limit = 10000)".formatted(testCase.columnSchema("normal")))) {
                // with a uniform distribution, the outer tenths of the range would have twice as many values as the middle one
                assertQuery(
                        """
                        SELECT
                            min(x) >= %1$s AND max(x) <= %2$s,
                            count_if(position BETWEEN 0.45 AND 0.55) > 4 * count_if(position < 0.1 OR position > 0.9),
                            count_if(position < 0.1 OR position > 0.9) > 0,
                            count_if(position BETWEEN 0.45 AND 0.55) BETWEEN 0.15 * count(*) AND 0.35 * count(*)
                        FROM (SELECT x, %3$s AS position FROM %4$s)
                        """.formatted(testCase.minValue(), testCase.maxValue(), testCase.position(), table.getName()),
                        "VALUES (true, true, true, true)");
            }
        }
    }

    @Test
    void testZipfDistribution()
    {
        List<DistributionTestCase> testCases = ImmutableList.of(
                new DistributionTestCase("bigint", "0", "99"),
                new DistributionTestCase("integer", "-50", "49"),
                new DistributionTestCase("smallint", "0", "99"),
                new DistributionTestCase("tinyint", "-128", "127"),
                new DistributionTestCase("date", "2020-01-01", "2020-04-09"),
                new DistributionTestCase("timestamp(0)", "2020-01-01 00:00:00", "2020-01-01 00:01:39"),
                new DistributionTestCase("timestamp(3)", "2020-01-01 00:00:00.000", "2020-01-01 00:00:00.099"),
                new DistributionTestCase("timestamp(6)", "2020-01-01 00:00:00.000000", "2020-01-01 00:00:00.000099"));

        for (DistributionTestCase testCase : testCases) {
            try (TestTable table = newTrinoTable("zipf_distribution", "(%s) WITH (default_limit = 10000)".formatted(testCase.columnSchema("zipf")))) {
                assertQuery(
                        """
                        SELECT
                            min(x) = %1$s AND max(x) <= %2$s,
                            max_by(x, frequency) = %1$s,
                            CAST(max(frequency) FILTER (WHERE rank = 1) AS double) / max(frequency) FILTER (WHERE rank = 2) BETWEEN 1.7 AND 2.3,
                            count(*) > 50
                        FROM (
                            SELECT x, frequency, row_number() OVER (ORDER BY x) AS rank
                            FROM (SELECT x, count(*) AS frequency FROM %3$s GROUP BY x))
                        """.formatted(testCase.minValue(), testCase.maxValue(), table.getName()),
                        "VALUES (true, true, true, true)");
            }
        }

        // with too many values to compare their frequencies, only check that the min value is the most frequent
        testCases = ImmutableList.of(
                new DistributionTestCase("bigint", "-9223372036854775808", "9223372036854775807"),
                new DistributionTestCase("bigint", "9223372036854775806", "9223372036854775807"),
                new DistributionTestCase("integer", "-2147483648", "2147483647"),
                new DistributionTestCase("date", "-5877641-06-23", "5881580-07-11"),
                new DistributionTestCase("timestamp(6)", "1970-01-01 00:00:00.000000", "294247-01-10 04:00:54.775807"));

        for (DistributionTestCase testCase : testCases) {
            try (TestTable table = newTrinoTable("zipf_distribution", "(%s) WITH (default_limit = 10000)".formatted(testCase.columnSchema("zipf")))) {
                assertQuery(
                        """
                        SELECT
                            min(x) >= %1$s AND max(x) <= %2$s,
                            max_by(x, frequency) = %1$s,
                            count(*) > 1
                        FROM (SELECT x, count(*) AS frequency FROM %3$s GROUP BY x)
                        """.formatted(testCase.minValue(), testCase.maxValue(), table.getName()),
                        "VALUES (true, true, true)");
            }
        }

        // fractions of a microsecond are uniform, so no single value is the most frequent, and only the bounds can be checked
        testCases = ImmutableList.of(
                new DistributionTestCase("timestamp(9)", "2020-01-01 00:00:00.000000000", "2020-01-02 00:00:00.000000000"),
                new DistributionTestCase("timestamp(12)", "2020-01-01 00:00:00.000000000000", "2020-01-01 00:00:00.000001000000"));

        for (DistributionTestCase testCase : testCases) {
            try (TestTable table = newTrinoTable("zipf_distribution", "(%s) WITH (default_limit = 10000)".formatted(testCase.columnSchema("zipf")))) {
                assertQuery(
                        "SELECT min(x) >= %s AND max(x) <= %s FROM %s".formatted(testCase.minValue(), testCase.maxValue(), table.getName()),
                        "VALUES true");
            }
        }
    }

    @Test
    void testZipfDistributionAllowedValues()
    {
        record TestCase(String type, String allowedValues, String mostFrequentValues) {}

        // values are ranked in the order they are listed, not sorted

        List<TestCase> testCases = ImmutableList.of(
                new TestCase("bigint", "ARRAY['3', '1', '2', '5', '4']", "ARRAY[3, 1, 2, 5, 4]"),
                new TestCase("double", "ARRAY['0.5', 'NaN', '-1.0', '1.0', '0.0']", "ARRAY[0.5, nan(), -1.0, 1.0, 0.0]"),
                new TestCase("varchar", "ARRAY['gold', 'silver', 'bronze', 'iron', 'wood']", "ARRAY['gold', 'silver', 'bronze', 'iron', 'wood']"),
                new TestCase("char(6)", "ARRAY['gold', 'silver', 'bronze', 'iron', 'wood']", "ARRAY[CAST('gold' AS char(6)), CAST('silver' AS char(6)), CAST('bronze' AS char(6)), CAST('iron' AS char(6)), CAST('wood' AS char(6))]"),
                new TestCase("boolean", "ARRAY['true', 'false']", "ARRAY[true, false]"),
                new TestCase("uuid", "ARRAY['3ee49ede-0026-45e4-ba06-08404f794557', '1fc74d96-0216-449b-a145-455578a9eaa5']", "ARRAY[UUID '3ee49ede-0026-45e4-ba06-08404f794557', UUID '1fc74d96-0216-449b-a145-455578a9eaa5']"));

        for (TestCase testCase : testCases) {
            String columnSchema = "x %s NOT NULL WITH (allowed_values = %s, distribution = 'zipf')".formatted(testCase.type(), testCase.allowedValues());
            try (TestTable table = newTrinoTable("zipf_allowed_values", "(%s) WITH (default_limit = 10000)".formatted(columnSchema))) {
                // every value is generated, ordered by frequency as listed, and the first value is twice as frequent as the second one
                assertQuery(
                        """
                        SELECT
                            array_agg(x ORDER BY frequency DESC) IS NOT DISTINCT FROM (%1$s),
                            CAST(max(frequency) AS double) / max_by(frequency, x IS NOT DISTINCT FROM (%1$s)[2]) BETWEEN 1.7 AND 2.3,
                            count(*) = cardinality(%3$s)
                        FROM (SELECT x, count(*) AS frequency FROM %2$s GROUP BY x)
                        """.formatted(testCase.mostFrequentValues(), table.getName(), testCase.allowedValues()),
                        "VALUES (true, true, true)");
            }
        }

        // values that are equal, but not identical, are ranked separately
        testCases = ImmutableList.of(
                new TestCase("double", "ARRAY['0.0', '-0.0', '1.0']", "'0E0,-0E0,1.0E0'"),
                new TestCase("timestamp(3) with time zone", "ARRAY['2020-01-01 00:00:00.000 UTC', '2020-01-01 01:00:00.000 +01:00']", "'2020-01-01 00:00:00.000 UTC,2020-01-01 01:00:00.000 +01:00'"));

        for (TestCase testCase : testCases) {
            String columnSchema = "x %s NOT NULL WITH (allowed_values = %s, distribution = 'zipf')".formatted(testCase.type(), testCase.allowedValues());
            try (TestTable table = newTrinoTable("zipf_allowed_values", "(%s) WITH (default_limit = 10000)".formatted(columnSchema))) {
                assertQuery(
                        "SELECT array_join(array_agg(value ORDER BY frequency DESC), ',') FROM (SELECT CAST(x AS varchar) AS value, count(*) AS frequency FROM %s GROUP BY 1)".formatted(table.getName()),
                        "VALUES %s".formatted(testCase.mostFrequentValues()));
            }
        }
    }

    @Test
    void testInvalidDistributionProperty()
    {
        assertThat(query("CREATE TABLE invalid_distribution (value bigint WITH (min = '0', max = '10', distribution = 'pareto'))"))
                .failure()
                .hasErrorCode(INVALID_COLUMN_PROPERTY)
                .hasMessageContaining("Invalid value [pareto]. Valid values: [UNIFORM, NORMAL, ZIPF]");

        Map<String, String> columnSchemas = ImmutableMap.<String, String>builder()
                .put("bigint WITH (distribution = 'normal')", "The `normal` distribution requires the `min` and `max` properties")
                .put("bigint WITH (min = '0', distribution = 'normal')", "The `normal` distribution requires the `min` and `max` properties")
                .put("bigint WITH (allowed_values = ARRAY['0', '1'], distribution = 'normal')", "The `normal` distribution cannot be used together with the `allowed_values` property")
                .put("double WITH (min = '-Infinity', max = '0', distribution = 'normal')", "The `normal` distribution requires finite `min` and `max` values")
                .put("real WITH (min = '0', max = 'Infinity', distribution = 'normal')", "The `normal` distribution requires finite `min` and `max` values")
                .put("bigint WITH (max = '10', distribution = 'zipf')", "The `zipf` distribution requires the `min` and `max` properties, or the `allowed_values` property")
                .put("bigint WITH (min = '0', max = '10', step = '1', distribution = 'zipf')", "The `zipf` distribution cannot be used together with the `step` or `generator` properties")
                .put("date WITH (min = '2020-01-01', max = '2020-12-31', step = '1d', distribution = 'normal')", "The `normal` distribution cannot be used together with the `step` or `generator` properties")
                .put("varchar WITH (generator = '#{Name.first_name}', distribution = 'zipf')", "The `zipf` distribution cannot be used together with the `step` or `generator` properties")
                .put("varchar WITH (distribution = 'zipf')", "The `zipf` distribution can only be used for varchar columns together with the `allowed_values` property")
                .put("varchar WITH (allowed_values = ARRAY['a', 'b'], distribution = 'normal')", "The `normal` distribution cannot be used for varchar columns")
                .put("char(3) WITH (distribution = 'normal')", "The `normal` distribution cannot be used for char(3) columns")
                .put("varbinary WITH (distribution = 'zipf')", "The `zipf` distribution can only be used for varbinary columns together with the `allowed_values` property")
                .put("boolean WITH (distribution = 'normal')", "The `normal` distribution cannot be used for boolean columns")
                .put("uuid WITH (distribution = 'zipf')", "The `zipf` distribution can only be used for uuid columns together with the `allowed_values` property")
                .put("ipaddress WITH (distribution = 'normal')", "The `normal` distribution cannot be used for ipaddress columns")
                .put("row(a bigint) WITH (distribution = 'normal')", "The `normal` distribution cannot be used for row(\"a\" bigint) columns")
                .put("array(bigint) WITH (distribution = 'zipf')", "The `zipf` distribution can only be used for array(bigint) columns together with the `allowed_values` property")
                .put("real WITH (min = '0.0', max = '1.0', distribution = 'zipf')", "The `zipf` distribution can only be used for real columns together with the `allowed_values` property")
                .put("double WITH (min = '0.0', max = '1.0', distribution = 'zipf')", "The `zipf` distribution can only be used for double columns together with the `allowed_values` property")
                .put("decimal(10,2) WITH (min = '0.00', max = '1.00', distribution = 'zipf')", "The `zipf` distribution can only be used for decimal(10,2) columns together with the `allowed_values` property")
                .put("interval day to second WITH (min = '0', max = '1', distribution = 'normal')", "The `normal` distribution cannot be used for interval day(2) to second(6) columns")
                .put("timestamp with time zone WITH (min = '2020-01-01 00:00:00.000 UTC', max = '2020-01-02 00:00:00.000 UTC', distribution = 'normal')", "The `normal` distribution cannot be used for timestamp(3) with time zone columns")
                .put("time WITH (min = '00:00:00.000', max = '01:00:00.000', distribution = 'normal')", "The `normal` distribution cannot be used for time(3) columns")
                .buildOrThrow();

        for (Map.Entry<String, String> entry : columnSchemas.entrySet()) {
            assertThat(query("CREATE TABLE invalid_distribution (value %s)".formatted(entry.getKey())))
                    .failure()
                    .hasErrorCode(INVALID_COLUMN_PROPERTY)
                    .hasMessage(entry.getValue());
        }
    }

    private record TestDataType(String name, String type, Map<String, String> properties, String queryExpression, String expectedValue)
    {
        public TestDataType(String name, String type, String queryExpression, String expectedValue)
        {
            this(name, type, Map.of(), queryExpression, expectedValue);
        }

        String columnSchema()
        {
            String propertiesSchema = properties.entrySet().stream()
                    .map(entry -> "\"%s\" = %s".formatted(
                            entry.getKey(),
                            entry.getKey().equals(ALLOWED_VALUES_PROPERTY) ? entry.getValue() : "'%s'".formatted(entry.getValue())))
                    .collect(joining(", "));
            return "%s %s NOT NULL%s".formatted(name, type, propertiesSchema.isEmpty() ? "" : " WITH (%s)".formatted(propertiesSchema));
        }
    }

    private record DistributionTestCase(String type, String min, String max, String toDouble)
    {
        DistributionTestCase(String type, String min, String max)
        {
            this(type, min, max, "CAST(%s AS double)");
        }

        String columnSchema(String distribution)
        {
            return "x %s NOT NULL WITH (min = '%s', max = '%s', distribution = '%s')".formatted(type, min, max, distribution);
        }

        String minValue()
        {
            return "CAST('%s' AS %s)".formatted(min, type);
        }

        String maxValue()
        {
            return "CAST('%s' AS %s)".formatted(max, type);
        }

        String position()
        {
            // halve the values, so the span of the widest ranges doesn't overflow
            return "(%2$s / 2 - %1$s / 2) / (%3$s / 2 - %1$s / 2)".formatted(
                    toDouble.formatted(minValue()),
                    toDouble.formatted("x"),
                    toDouble.formatted(maxValue()));
        }
    }

    @Test
    void testReferences()
    {
        try (TestTable parent = newTrinoTable("references_parent",
                """
                (
                    id UUID NOT NULL,
                    name VARCHAR NOT NULL WITH (generator = '#{Name.first_name}'),
                    number INTEGER NOT NULL WITH (min = '1', step = '1')
                )
                WITH (default_limit = 100)
                """);
                TestTable child = newTrinoTable("references_child",
                        """
                        (
                            parent_id UUID NOT NULL WITH (references = '%1$s.id'),
                            parent_name VARCHAR NOT NULL WITH (references = 'default.%1$s.name'),
                            parent_number INTEGER NOT NULL WITH (references = '%1$s.number')
                        )
                        """.formatted(parent.getName()))) {
            assertNoOrphans(child.getName(), "parent_id", parent.getName(), "id");
            assertNoOrphans(child.getName(), "parent_name", parent.getName(), "name");
            assertNoOrphans(child.getName(), "parent_number", parent.getName(), "number");
            // 1000 rows choosing from 100 parent rows reference almost all of them
            assertThat((long) computeScalar("SELECT count(DISTINCT parent_id) FROM " + child.getName())).isGreaterThan(90);
        }
    }

    @Test
    void testReferencesSameTable()
    {
        assertUpdate(
                """
                CREATE TABLE references_employees (
                    id BIGINT NOT NULL WITH (min = '1', step = '1'),
                    manager_id BIGINT WITH (references = 'references_employees.id', null_probability = 0.1)
                )
                """);
        assertNoOrphans("references_employees", "manager_id", "references_employees", "id");
        assertUpdate("DROP TABLE references_employees");
    }

    @Test
    void testReferencesChain()
    {
        try (TestTable first = newTrinoTable("references_chain_first", "(id UUID NOT NULL)");
                TestTable second = newTrinoTable("references_chain_second", "(first_id UUID NOT NULL WITH (references = '%s.id')) WITH (default_limit = 10)".formatted(first.getName()));
                TestTable third = newTrinoTable("references_chain_third", "(first_id UUID NOT NULL WITH (references = '%s.first_id'))".formatted(second.getName()))) {
            assertNoOrphans(second.getName(), "first_id", first.getName(), "id");
            assertNoOrphans(third.getName(), "first_id", second.getName(), "first_id");
        }
    }

    @Test
    void testReferencesNullableColumn()
    {
        try (TestTable parent = newTrinoTable("references_nullable_parent", "(id UUID WITH (null_probability = 0.5))");
                TestTable child = newTrinoTable("references_nullable_child", "(parent_id UUID NOT NULL WITH (references = '%s.id'))".formatted(parent.getName()))) {
            assertNoOrphans(child.getName(), "parent_id", parent.getName(), "id");
            assertQuery("SELECT count(*) FROM %s WHERE parent_id IS NULL".formatted(child.getName()), "VALUES 0");
        }
    }

    @Test
    void testReferencesOnlyNullRows()
    {
        // with this seed, both rows of the referenced table are null
        try (TestTable parent = newTrinoTable("references_null_rows_parent", "(id UUID WITH (null_probability = 0.999999)) WITH (default_limit = 2, seed = 0)");
                TestTable child = newTrinoTable("references_null_rows_child", "(parent_id UUID NOT NULL WITH (references = '%s.id'))".formatted(parent.getName()))) {
            assertQueryFails(
                    "SELECT * FROM " + child.getName(),
                    "Column 'id' referenced by column 'parent_id' only has null values");
        }
    }

    @Test
    void testReferencingColumnMetadata()
    {
        try (TestTable parent = newTrinoTable("references_metadata_parent", "(id UUID NOT NULL)");
                TestTable child = newTrinoTable("references_metadata_child", "(parent_id UUID NOT NULL WITH (references = '%s.id'))".formatted(parent.getName()))) {
            assertUpdate("COMMENT ON COLUMN %s.parent_id IS 'comment text'".formatted(child.getName()));
            assertQuery("SHOW COLUMNS FROM " + child.getName(), "VALUES ('parent_id', 'uuid', '', 'comment text')");
            assertQuery(
                    "SHOW STATS FOR " + child.getName(),
                    "VALUES ('parent_id', NULL, NULL, 0.0, NULL, NULL, NULL), (NULL, NULL, NULL, NULL, 1000.0, NULL, NULL)");
        }
    }

    @Test
    void testReferencesValidation()
    {
        try (TestTable parent = newTrinoTable("references_invalid_parent", "(id UUID NOT NULL, number BIGINT NOT NULL, nothing UUID WITH (null_probability = 1), digest HyperLogLog)");
                TestTable emptyParent = newTrinoTable("references_invalid_empty_parent", "AS SELECT uuid() AS id WITH NO DATA")) {
            String parentName = parent.getName();
            assertThat(query("CREATE TABLE references_invalid (x UUID WITH (references = 'id'))"))
                    .failure()
                    .hasErrorCode(INVALID_COLUMN_PROPERTY)
                    .hasMessage("The `references` property must be in the 'table.column' or 'schema.table.column' format");
            assertThat(query("CREATE TABLE references_invalid (x UUID WITH (references = 'missing.id'))"))
                    .failure()
                    .hasErrorCode(INVALID_COLUMN_PROPERTY)
                    .hasMessage("Table 'default.missing' referenced by column 'default.references_invalid.x' does not exist");
            assertThat(query("CREATE TABLE references_invalid (x UUID WITH (references = '%s.missing'))".formatted(parentName)))
                    .failure()
                    .hasErrorCode(INVALID_COLUMN_PROPERTY)
                    .hasMessage("Column 'default.%s.missing' referenced by column 'default.references_invalid.x' does not exist".formatted(parentName));
            assertThat(query("CREATE TABLE references_invalid (x VARCHAR WITH (references = '%s.id'))".formatted(parentName)))
                    .failure()
                    .hasErrorCode(INVALID_COLUMN_PROPERTY)
                    .hasMessage("Column 'default.references_invalid.x' of type varchar cannot reference column 'default.%s.id' of type uuid".formatted(parentName));
            for (String property : ImmutableList.of("min = '1'", "max = '1'", "step = '1'", "allowed_values = ARRAY['1']")) {
                assertThat(query("CREATE TABLE references_invalid (x BIGINT WITH (references = '%s.number', %s))".formatted(parentName, property)))
                        .failure()
                        .hasErrorCode(INVALID_COLUMN_PROPERTY)
                        .hasMessage("The `references` property cannot be set together with `min`, `max`, `step`, `allowed_values`, and `generator` properties");
            }
            assertThat(query("CREATE TABLE references_invalid (x UUID WITH (references = 'references_invalid.y'), y UUID WITH (references = 'references_invalid.x'))"))
                    .failure()
                    .hasErrorCode(INVALID_COLUMN_PROPERTY)
                    .hasMessage("Column references form a cycle: default.references_invalid.x -> default.references_invalid.y -> default.references_invalid.x");
            assertThat(query("CREATE TABLE references_invalid (x UUID WITH (references = '%s.nothing'))".formatted(parentName)))
                    .failure()
                    .hasErrorCode(INVALID_COLUMN_PROPERTY)
                    .hasMessage("Column 'default.references_invalid.x' cannot reference column 'default.%s.nothing', which only has null values".formatted(parentName));
            assertThat(query("CREATE TABLE references_invalid (x HyperLogLog WITH (references = '%s.digest'))".formatted(parentName)))
                    .failure()
                    .hasErrorCode(INVALID_COLUMN_PROPERTY)
                    .hasMessage("Column 'default.references_invalid.x' cannot reference column 'default.%s.digest', which only has null values".formatted(parentName));
            assertThat(query("CREATE TABLE references_invalid (x UUID WITH (references = '%s.id'))".formatted(emptyParent.getName())))
                    .failure()
                    .hasErrorCode(INVALID_COLUMN_PROPERTY)
                    .hasMessage("Table 'default.%s' referenced by column 'default.references_invalid.x' has no rows".formatted(emptyParent.getName()));
            assertThat(getQueryRunner().tableExists(getSession(), "references_invalid")).isFalse();
        }
    }

    @Test
    void testReferencedTableChanged()
    {
        assertUpdate("CREATE TABLE references_changed_parent (id UUID NOT NULL)");
        assertUpdate("CREATE TABLE references_changed_child (parent_id UUID NOT NULL WITH (references = 'references_changed_parent.id'))");

        assertUpdate("ALTER TABLE references_changed_parent SET PROPERTIES default_limit = 10, seed = 1");
        assertNoOrphans("references_changed_child", "parent_id", "references_changed_parent", "id");
        assertQuery("SELECT count(DISTINCT parent_id) FROM references_changed_child", "VALUES 10");

        assertUpdate("DROP TABLE references_changed_parent");
        assertThat(query("SELECT * FROM references_changed_child"))
                .failure()
                .hasErrorCode(INVALID_COLUMN_PROPERTY)
                .hasMessage("Table 'default.references_changed_parent' referenced by column 'default.references_changed_child.parent_id' does not exist");

        assertUpdate("CREATE TABLE references_changed_parent (id VARCHAR NOT NULL)");
        assertThat(query("SELECT * FROM references_changed_child"))
                .failure()
                .hasErrorCode(INVALID_COLUMN_PROPERTY)
                .hasMessage("Column 'default.references_changed_child.parent_id' of type uuid cannot reference column 'default.references_changed_parent.id' of type varchar");

        assertUpdate("DROP TABLE references_changed_parent");
        assertUpdate("DROP TABLE references_changed_child");
    }

    private void assertNoOrphans(String table, String column, String referencedTable, String referencedColumn)
    {
        assertQuery(
                """
                SELECT count(r.%3$s) > 0, count_if(t.%4$s IS NULL)
                FROM %1$s r
                LEFT JOIN %2$s t ON r.%3$s = t.%4$s
                WHERE r.%3$s IS NOT NULL
                """.formatted(table, referencedTable, column, referencedColumn),
                "VALUES (true, 0)");
    }

    @Test
    void testSetTableProperties()
    {
        try (TestTable table = newTrinoTable("set_table_properties", "(id INTEGER, name VARCHAR)")) {
            assertUpdate("ALTER TABLE " + table.getName() + " SET PROPERTIES default_limit = 100");
            assertThat((String) computeScalar("SHOW CREATE TABLE " + table.getName()))
                    .contains("default_limit = 100");
        }
    }

    @Test
    void testRenameTable()
    {
        assertUpdate("CREATE TABLE original_table(id INTEGER, name VARCHAR)");
        assertUpdate("ALTER TABLE original_table RENAME TO renamed_table");
        // original_table should not exist anymore after renaming.
        assertQueryFails("DESC original_table", "line 1:1: Table 'faker.default.original_table' does not exist");
        // should not allow renaming to an already existing table.
        assertQueryFails("ALTER TABLE renamed_table RENAME TO renamed_table", "line 1:1: Target table 'faker.default.renamed_table' already exists");
        assertUpdate("DROP TABLE renamed_table");
    }

    @Test
    void testRenameTableAcrossSchema()
    {
        assertUpdate("CREATE SCHEMA new_schema");
        assertUpdate("CREATE TABLE original_table_schema(id INTEGER, name VARCHAR)");
        assertUpdate("ALTER TABLE original_table_schema RENAME TO new_schema.renamed_table");
        assertQueryFails("DESC original_table_schema", "line 1:1: Table 'faker.default.original_table_schema' does not exist");
        assertUpdate("DROP TABLE new_schema.renamed_table");
        assertUpdate("DROP SCHEMA new_schema");
    }

    @Test
    void testCreateTableAsSelect()
    {
        assertUpdate("CREATE TABLE faker.default.limited_range WITH (null_probability = 0, default_limit = 50, dictionary_detection_enabled = false) AS " +
                "SELECT * FROM (VALUES -1, 3, 5) t(id)", 3);

        assertQuery(
                "SELECT count(id) FROM (SELECT id FROM limited_range) a",
                "VALUES (50)");

        assertQueryFails("INSERT INTO faker.default.limited_range(id) VALUES (10)", "This connector does not support inserts");

        assertUpdate("DROP TABLE faker.default.limited_range");

        List<TestDataType> testCases = ImmutableList.<TestDataType>builder()
                .add(new TestDataType("rnd_bigint", "bigint", Map.of("min", "0", "max", "1"), "count(distinct rnd_bigint)", "2"))
                .add(new TestDataType("rnd_integer", "integer", Map.of("min", "0", "max", "1"), "count(distinct rnd_integer)", "2"))
                .add(new TestDataType("rnd_smallint", "smallint", Map.of("min", "0", "max", "1"), "count(distinct rnd_smallint)", "2"))
                .add(new TestDataType("rnd_tinyint", "tinyint", Map.of("min", "0", "max", "1"), "count(distinct rnd_tinyint)", "2"))
                .add(new TestDataType("rnd_date", "date", Map.of("min", "2022-03-01", "max", "2022-03-02"), "count(distinct rnd_date)", "2"))
                .add(new TestDataType("rnd_decimal1", "decimal", Map.of("min", "0", "max", "1"), "count(distinct rnd_decimal1)", "2"))
                .add(new TestDataType("rnd_decimal2", "decimal(18,5)", Map.of("min", "0.00000", "max", "0.00001"), "count(distinct rnd_decimal2)", "2"))
                .add(new TestDataType("rnd_decimal3", "decimal(38,0)", Map.of("min", "0", "max", "1"), "count(distinct rnd_decimal3)", "2"))
                .add(new TestDataType("rnd_decimal4", "decimal(38,38)", Map.of("min", "0.00000000000000000000000000000000000000", "max", "0.00000000000000000000000000000000000001"), "count(distinct rnd_decimal4)", "2"))
                .add(new TestDataType("rnd_decimal5", "decimal(5,2)", Map.of("min", "0.00", "max", "0.01"), "count(distinct rnd_decimal5)", "2"))
                .add(new TestDataType("rnd_real", "real", Map.of("min", "0.0", "max", "1.4E-45"), "count(distinct rnd_real)", "2"))
                .add(new TestDataType("rnd_double", "double", Map.of("min", "0.0", "max", "4.9E-324"), "count(distinct rnd_double)", "2"))
                .add(new TestDataType("rnd_interval1", "interval day(2) to second(3)", Map.of("min", "0.000", "max", "0.001"), "count(distinct rnd_interval1)", "2"))
                .add(new TestDataType("rnd_interval2", "interval year(9) to month", Map.of("min", "0", "max", "1"), "count(distinct rnd_interval2)", "2"))
                .add(new TestDataType("rnd_timestamp", "timestamp", Map.of("min", "2022-03-21 00:00:00.000", "max", "2022-03-21 00:00:00.001"), "count(distinct rnd_timestamp)", "2"))
                .add(new TestDataType("rnd_timestamp0", "timestamp(0)", Map.of("min", "2022-03-21 00:00:00", "max", "2022-03-21 00:00:01"), "count(distinct rnd_timestamp0)", "2"))
                .add(new TestDataType("rnd_timestamp6", "timestamp(6)", Map.of("min", "2022-03-21 00:00:00.000000", "max", "2022-03-21 00:00:00.000001"), "count(distinct rnd_timestamp6)", "2"))
                .add(new TestDataType("rnd_timestamp9", "timestamp(9)", Map.of("min", "2022-03-21 00:00:00.000000000", "max", "2022-03-21 00:00:00.000000001"), "count(distinct rnd_timestamp9)", "2"))
                .add(new TestDataType("rnd_timestamptz", "timestamp with time zone", Map.of("min", "2022-03-21 00:00:00.000 +01:00", "max", "2022-03-21 00:00:00.001 +01:00"), "count(distinct rnd_timestamptz)", "2"))
                .add(new TestDataType("rnd_timestamptz0", "timestamp(0) with time zone", Map.of("min", "2022-03-21 00:00:00 +01:00", "max", "2022-03-21 00:00:01 +01:00"), "count(distinct rnd_timestamptz0)", "2"))
                .add(new TestDataType("rnd_timestamptz6", "timestamp(6) with time zone", Map.of("min", "2022-03-21 00:00:00.000000 +01:00", "max", "2022-03-21 00:00:00.000001 +01:00"), "count(distinct rnd_timestamptz6)", "2"))
                .add(new TestDataType("rnd_timestamptz9", "timestamp(9) with time zone", Map.of("min", "2022-03-21 00:00:00.000000000 +01:00", "max", "2022-03-21 00:00:00.000000001 +01:00"), "count(distinct rnd_timestamptz9)", "2"))
                .add(new TestDataType("rnd_time", "time", Map.of("min", "01:02:03.456", "max", "01:02:03.457"), "count(distinct rnd_time)", "2"))
                .add(new TestDataType("rnd_time0", "time(0)", Map.of("min", "01:02:03", "max", "01:02:04"), "count(distinct rnd_time0)", "2"))
                .add(new TestDataType("rnd_time6", "time(6)", Map.of("min", "01:02:03.000456", "max", "01:02:03.000457"), "count(distinct rnd_time6)", "2"))
                .add(new TestDataType("rnd_time9", "time(9)", Map.of("min", "01:02:03.000000456", "max", "01:02:03.000000457"), "count(distinct rnd_time9)", "2"))
                .add(new TestDataType("rnd_timetz", "time with time zone", Map.of("min", "01:02:03.456 +01:00", "max", "01:02:03.457 +01:00"), "count(distinct rnd_timetz)", "2"))
                .add(new TestDataType("rnd_timetz0", "time(0) with time zone", Map.of("min", "01:02:03 +01:00", "max", "01:02:04 +01:00"), "count(distinct rnd_timetz0)", "2"))
                .add(new TestDataType("rnd_timetz6", "time(6) with time zone", Map.of("min", "01:02:03.000456 +01:00", "max", "01:02:03.000457 +01:00"), "count(distinct rnd_timetz6)", "2"))
                .add(new TestDataType("rnd_timetz9", "time(9) with time zone", Map.of("min", "01:02:03.000000456 +01:00", "max", "01:02:03.000000457 +01:00"), "count(distinct rnd_timetz9)", "2"))
                .add(new TestDataType("rnd_timetz12", "time(12) with time zone", Map.of("min", "01:02:03.000000000456 +01:00", "max", "01:02:03.000000000457 +01:00"), "count(distinct rnd_timetz12)", "2"))
                .build();

        for (TestDataType testCase : testCases) {
            try (TestTable sourceTable = new TestTable(getQueryRunner()::execute, "ctas_src_" + testCase.name(), "(%s) WITH (null_probability = 0, default_limit = 1000)".formatted(testCase.columnSchema()));
                    TestTable table = new TestTable(getQueryRunner()::execute, "ctas_" + testCase.name(), "WITH (null_probability = 0, default_limit = 1000, dictionary_detection_enabled = false, sequence_detection_enabled = false) AS SELECT %s FROM %s".formatted(testCase.name(), sourceTable.getName()))) {
                assertQuery("SELECT %s FROM %s".formatted(testCase.queryExpression(), table.getName()), "VALUES (%s)".formatted(testCase.expectedValue()));
            }
        }

        for (TestDataType testCase : testCases) {
            try (TestTable sourceTable = new TestTable(getQueryRunner()::execute, "ctas_src_" + testCase.name(), "(%s %s) WITH (null_probability = 0, default_limit = 2)".formatted(testCase.name(), testCase.type()));
                    TestTable table = new TestTable(getQueryRunner()::execute, "ctas_" + testCase.name(), "WITH (null_probability = 0, default_limit = 1000, sequence_detection_enabled = false) AS SELECT %s FROM %s".formatted(testCase.name(), sourceTable.getName()))) {
                assertQuery("SELECT %s FROM %s".formatted(testCase.queryExpression(), table.getName()), "VALUES (%s)".formatted(testCase.expectedValue()));
            }
        }
    }

    @Test
    void testCreateTableAsSelectSequence()
    {
        String source =
                """
                SELECT
                  cast(greatest(least(sequential_number, 0x7f), -0x80) AS TINYINT) AS seq_tinyint,
                  cast(sequential_number AS SMALLINT) AS seq_smallint,
                  cast(sequential_number AS INTEGER) AS seq_integer,
                  cast(sequential_number AS BIGINT) AS seq_bigint
                FROM TABLE(sequence(start => -500, stop => 500, step => 1))
                """;
        try (TestTable sourceTable = new TestTable(getQueryRunner()::execute, "seq_src", "WITH (null_probability = 0, default_limit = 1000, dictionary_detection_enabled = false) AS " + source);
                TestTable table = new TestTable(getQueryRunner()::execute, "seq", "WITH (null_probability = 0, default_limit = 1000, dictionary_detection_enabled = false) AS SELECT * FROM %s".formatted(sourceTable.getName()))) {
            String createTable = (String) computeScalar("SHOW CREATE TABLE " + table.getName());
            assertThat(createTable).containsPattern("seq_tinyint tinyint WITH \\(max = '\\d+', min = '-\\d+'\\)");
            assertThat(createTable).containsPattern("seq_smallint smallint WITH \\(max = '\\d+', min = '-\\d+', step = '1'\\)");
            assertThat(createTable).containsPattern("seq_integer integer WITH \\(max = '\\d+', min = '-\\d+', step = '1'\\)");
            assertThat(createTable).containsPattern("seq_bigint bigint WITH \\(max = '\\d+', min = '-\\d+', step = '1'\\)");
        }
    }

    @Test
    void testCreateTableAsSelectNulls()
    {
        String source =
                """
                SELECT
                  cast(NULL AS INTEGER) AS nullable
                FROM TABLE(sequence(start => 0, stop => 1000, step => 1))
                """;
        try (TestTable table = new TestTable(getQueryRunner()::execute, "only_nulls", "WITH (dictionary_detection_enabled = false) AS " + source)) {
            String createTable = (String) computeScalar("SHOW CREATE TABLE " + table.getName());
            assertThat(createTable).containsPattern("nullable integer WITH \\(null_probability = 1E0\\)");
        }

        // the table property overrides the detected null probability, so the column generates values without having any in the source
        try (TestTable table = new TestTable(getQueryRunner()::execute, "only_nulls", "WITH (null_probability = 0.5) AS " + source)) {
            assertThat((Long) computeScalar("SELECT count(nullable) FROM " + table.getName())).isPositive();
            String createTable = (String) computeScalar("SHOW CREATE TABLE " + table.getName());
            assertThat(createTable).doesNotContain(ALLOWED_VALUES_PROPERTY);
        }
    }

    @Test
    void testSelectAllowedValuesNulls()
    {
        try (TestTable table = newTrinoTable("allowed_values_nulls", "(x varchar WITH (allowed_values = ARRAY['a', 'b']), y bigint WITH (allowed_values = ARRAY['1', '2'], distribution = 'zipf')) WITH (null_probability = 0.5)")) {
            assertQuery(
                    "SELECT count_if(x IS NULL) BETWEEN 400 AND 600, count_if(y IS NULL) BETWEEN 400 AND 600, count(DISTINCT x), count(DISTINCT y) FROM " + table.getName(),
                    "VALUES (true, true, 2, 2)");
        }

        // dictionaries detected in a source with nulls keep generating them
        String source =
                """
                SELECT
                  IF(sequential_number % 2 = 0, NULL, CAST(sequential_number % 3 AS varchar)) AS dictionary
                FROM TABLE(sequence(start => 1, stop => 1000, step => 1))
                """;
        try (TestTable table = new TestTable(getQueryRunner()::execute, "dictionary_nulls", "AS " + source)) {
            assertThat((String) computeScalar("SHOW CREATE TABLE " + table.getName())).contains(ALLOWED_VALUES_PROPERTY);
            assertQuery("SELECT count_if(dictionary IS NULL) BETWEEN 400 AND 600, count(DISTINCT dictionary) FROM " + table.getName(), "VALUES (true, 3)");
        }
    }

    @Test
    void testCreateTableAsSelectVarchar()
    {
        String source =
                """
                SELECT * FROM tpch.tiny.orders
                """;
        try (TestTable table = new TestTable(getQueryRunner()::execute, "varchars", "AS " + source)) {
            String createTable = (String) computeScalar("SHOW CREATE TABLE " + table.getName());
            assertThat(createTable).containsPattern("orderkey bigint WITH \\(max = '60000', min = '1', null_probability = 0E0, step = '1'\\)");
            assertThat(createTable).containsPattern("custkey bigint WITH \\(allowed_values = ARRAY\\['.*'], null_probability = 0E0\\)");
            assertThat(createTable).containsPattern("orderstatus varchar\\(1\\) WITH \\(allowed_values = ARRAY\\['.*'], null_probability = 0E0\\)");
            assertThat(createTable).containsPattern("totalprice double WITH \\(max = '.*', min = '.*', null_probability = 0E0\\)");
            assertThat(createTable).containsPattern("orderdate date WITH \\(max = '1998-08-02', min = '1992-01-01', null_probability = 0E0\\)");
            assertThat(createTable).containsPattern("orderpriority varchar\\(15\\) WITH \\(allowed_values = ARRAY\\['.*'], null_probability = 0E0\\)");
            assertThat(createTable).containsPattern("clerk varchar\\(15\\) WITH \\(allowed_values = ARRAY\\['.*'], null_probability = 0E0\\)");
            assertThat(createTable).containsPattern("shippriority integer WITH \\(allowed_values = ARRAY\\['0'], null_probability = 0E0\\)");
            assertThat(createTable).containsPattern("comment varchar\\(79\\) WITH \\(null_probability = 0E0\\)");
        }
    }

    @Test
    void testCreateTableAsSelectCharacterDictionary()
    {
        String source =
                """
                SELECT
                  orderstatus,
                  orderpriority,
                  CAST(orderpriority AS varchar) AS unbounded_orderpriority,
                  CAST(orderpriority AS char(20)) AS char_orderpriority,
                  rpad(CAST(custkey % 100 AS varchar), 2000, '.') AS long_text,
                  comment
                FROM tpch.tiny.orders
                """;
        try (TestTable table = new TestTable(getQueryRunner()::execute, "character_dictionary", "AS " + source)) {
            String createTable = (String) computeScalar("SHOW CREATE TABLE " + table.getName());
            assertThat(createTable).containsPattern("orderstatus varchar\\(1\\) WITH \\(allowed_values = ARRAY\\[('[^']+',){2}'[^']+'], null_probability = 0E0\\)");
            assertThat(createTable).containsPattern("orderpriority varchar\\(15\\) WITH \\(allowed_values = ARRAY\\[('[^']+',){4}'[^']+'], null_probability = 0E0\\)");
            assertThat(createTable).containsPattern("unbounded_orderpriority varchar WITH \\(allowed_values = ARRAY\\[('[^']+',){4}'[^']+'], null_probability = 0E0\\)");
            assertThat(createTable).containsPattern("char_orderpriority char\\(20\\) WITH \\(allowed_values = ARRAY\\[('[^']+',){4}'[^']+'], null_probability = 0E0\\)");
            assertThat(createTable).containsPattern("comment varchar\\(79\\) WITH \\(null_probability = 0E0\\)");
            assertQuery(
                    "SELECT count(DISTINCT orderstatus), count(DISTINCT orderpriority), count(DISTINCT unbounded_orderpriority), count(DISTINCT char_orderpriority), max(length(unbounded_orderpriority)) <= 15, max(length(long_text)) <= 1000 FROM " + table.getName(),
                    "VALUES (3, 5, 5, 5, true, true)");
            assertQuery("SELECT count(*) FROM %s WHERE orderpriority IN (SELECT orderpriority FROM tpch.tiny.orders)".formatted(table.getName()), "VALUES 0");
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "character_dictionary_disabled", "WITH (dictionary_detection_enabled = false) AS " + source)) {
            String createTable = (String) computeScalar("SHOW CREATE TABLE " + table.getName());
            assertThat(createTable).containsPattern("orderstatus varchar\\(1\\) WITH \\(null_probability = 0E0\\)");
            assertThat(createTable).containsPattern("orderpriority varchar\\(15\\) WITH \\(null_probability = 0E0\\)");
            assertThat(createTable).containsPattern("char_orderpriority char\\(20\\) WITH \\(null_probability = 0E0\\)");
        }
    }

    @Test
    void testCreateTableAsSelectCharacterDictionaryLocale()
            throws Exception
    {
        try (
                QueryRunner queryRunner = FakerQueryRunner.builder().setFakerProperties(Map.of("faker.locale", "ko")).build();
                TestTable table = new TestTable(queryRunner::execute, "character_dictionary_locale", "AS SELECT orderpriority FROM tpch.tiny.orders")) {
            // unlike English, Korean words contain multi-byte characters
            Object hasMultiByteValues = queryRunner.execute("SELECT bool_or(length(to_utf8(orderpriority)) > length(orderpriority)) FROM " + table.getName()).getOnlyValue();
            assertThat(hasMultiByteValues).isEqualTo(true);
        }
    }

    @Test
    void testCreateTableAsSelectIntervalBounds()
    {
        String source = "SELECT * FROM (VALUES INTERVAL '50 00:00:00.000001' DAY(9) TO SECOND(6), INTERVAL '51 00:00:00.000002' DAY(9) TO SECOND(6)) t(x)";
        try (TestTable table = new TestTable(getQueryRunner()::execute, "interval_bounds", "WITH (dictionary_detection_enabled = false) AS " + source)) {
            assertThat((String) computeScalar("SHOW CREATE TABLE " + table.getName()))
                    .contains("min = '4320000.000001'", "max = '4406400.000002'");
            assertQuery("SELECT min(x) >= INTERVAL '50 00:00:00.000001' DAY(9) TO SECOND(6) AND max(x) <= INTERVAL '51 00:00:00.000002' DAY(9) TO SECOND(6) FROM " + table.getName(), "VALUES true");
        }
    }

    @Test
    void testCreateTableAsSelectBoolean()
    {
        String source =
                """
                SELECT
                  sequential_number % 2 = 0 AS boolean,
                  ARRAY[true, false, sequential_number % 2 = 0] AS boolean_array
                FROM TABLE(sequence(start => 0, stop => 1000, step => 1))
                """;
        try (TestTable table = new TestTable(getQueryRunner()::execute, "booleans", "AS " + source)) {
            String createTable = (String) computeScalar("SHOW CREATE TABLE " + table.getName());
            assertThat(createTable).containsPattern("boolean boolean WITH \\(null_probability = 0E0\\)");
            assertThat(createTable).containsPattern("boolean_array array\\(boolean\\) WITH \\(null_probability = 0E0\\)");
        }
    }

    @Test
    void testCreateTableAsSelectNonOrderable()
    {
        String source =
                """
                SELECT
                  JSON '1' AS json,
                  MAP(ARRAY['a'], ARRAY[1]) AS map,
                  ARRAY[MAP(ARRAY['a'], ARRAY[1])] AS map_array,
                  ROW(MAP(ARRAY['a'], ARRAY[1])) AS map_row,
                  empty_approx_set() AS hll
                FROM TABLE(sequence(start => 0, stop => 1000, step => 1))
                """;
        try (TestTable table = new TestTable(getQueryRunner()::execute, "non_orderable", "AS " + source)) {
            String createTable = (String) computeScalar("SHOW CREATE TABLE " + table.getName());
            assertThat(createTable).containsPattern("json json WITH \\(null_probability = 0E0\\)");
            assertThat(createTable).containsPattern("map map\\(varchar\\(1\\), integer\\) WITH \\(null_probability = 0E0\\)");
            assertThat(createTable).containsPattern("map_array array\\(map\\(varchar\\(1\\), integer\\)\\) WITH \\(null_probability = 0E0\\)");
            assertThat(createTable).containsPattern("map_row ROW\\(map\\(varchar\\(1\\), integer\\)\\) WITH \\(null_probability = 0E0\\)");
            assertThat(createTable).containsPattern("hll HyperLogLog WITH \\(null_probability = 1E0\\)");
            assertThat(createTable).contains("default_limit = 1001");
            assertQuery(
                    "SELECT count(*), max(cardinality(map)), max(cardinality(map_row[1])) FROM " + table.getName(),
                    "VALUES (1001, 5, 5)");
            assertQuery(
                    "SELECT count(*) > 0, max(cardinality(m)) FROM %s CROSS JOIN UNNEST(map_array) u(m)".formatted(table.getName()),
                    "VALUES (true, 5)");
        }
    }

    @Test
    void testGeneratorDetection()
    {
        assertThat(showCreateTable("generator_detection", "(email VARCHAR, first_name VARCHAR(30), country_code CHAR(2), city VARCHAR(10), notes VARCHAR) WITH (generator_detection_enabled = true)"))
                .contains(
                        "email varchar WITH (generator = '#{Internet.emailAddress}')",
                        "first_name varchar(30) WITH (generator = '#{Name.first_name}')",
                        "country_code char(2) WITH (generator = '#{Country.countryCode2}')",
                        "city varchar(10),",
                        "notes varchar\n");
        assertThat(showCreateTable("no_generator_detection", "(email VARCHAR)"))
                .doesNotContain("generator =");
    }

    @Test
    void testGeneratorDetectionLike()
    {
        try (TestTable source = newTrinoTable("like_source", "(email VARCHAR, city VARCHAR)")) {
            assertThat(showCreateTable("like_generator_detection", "(LIKE %s) WITH (generator_detection_enabled = true)".formatted(source.getName())))
                    .contains(
                            "email varchar WITH (generator = '#{Internet.emailAddress}')",
                            "city varchar WITH (generator = '#{Address.city}')");
            assertThat(showCreateTable("like_no_generator_detection", "(LIKE %s)".formatted(source.getName())))
                    .doesNotContain("generator =");
        }
    }

    @Test
    void testSelectStructuralTypes()
    {
        try (TestTable table = newTrinoTable("structural",
                """
                (
                  tinyints array(tinyint) NOT NULL,
                  nested array(array(integer)) NOT NULL,
                  rows array(row(x integer, y varchar)) NOT NULL,
                  colors array(color) NOT NULL,
                  tinyint_keys map(tinyint, varchar) NOT NULL,
                  boolean_keys map(boolean, integer) NOT NULL,
                  boolean_row_keys map(row(b boolean), integer) NOT NULL,
                  map_row row(m map(varchar, array(integer))) NOT NULL,
                  color_keys map(color, integer) NOT NULL,
                  row_keys map(row(c color, i integer), integer) NOT NULL,
                  array_keys map(array(color), integer) NOT NULL)
                """)) {
            assertQuery(
                    "SELECT count(DISTINCT cardinality(tinyints)), min(cardinality(tinyints)), max(cardinality(tinyints)) FROM " + table.getName(),
                    "VALUES (6, 0, 5)");
            assertQuery(
                    "SELECT count(*) = count(e), count(DISTINCT e) > 250 FROM %s CROSS JOIN UNNEST(tinyints) u(e)".formatted(table.getName()),
                    "VALUES (true, true)");
            assertQuery(
                    "SELECT count(DISTINCT cardinality(e)) FROM %s CROSS JOIN UNNEST(nested) u(e)".formatted(table.getName()),
                    "VALUES (6)");
            assertQuery(
                    "SELECT count(*) = count(y), count(DISTINCT x) > 1 FROM %s CROSS JOIN UNNEST(rows) u(x, y)".formatted(table.getName()),
                    "VALUES (true, true)");
            // color has no generator
            assertQuery(
                    "SELECT count(*) > 0, count(e) FROM %s CROSS JOIN UNNEST(colors) u(e)".formatted(table.getName()),
                    "VALUES (true, 0)");
            assertQuery(
                    "SELECT count(DISTINCT cardinality(tinyint_keys)), count_if(cardinality(array_distinct(map_keys(tinyint_keys))) <> cardinality(tinyint_keys)) FROM " + table.getName(),
                    "VALUES (6, 0)");
            assertQuery(
                    "SELECT count(DISTINCT cardinality(boolean_keys)), max(cardinality(boolean_keys)), max(cardinality(boolean_row_keys)) FROM " + table.getName(),
                    "VALUES (3, 2, 2)");
            assertQuery(
                    "SELECT count(*) = count(v), count(DISTINCT v) > 250 FROM %s CROSS JOIN UNNEST(tinyint_keys) u(k, v)".formatted(table.getName()),
                    "VALUES (true, true)");
            assertQuery(
                    "SELECT count(*) = count(v), count(DISTINCT v) > 250 FROM %s CROSS JOIN UNNEST(boolean_keys) u(k, v)".formatted(table.getName()),
                    "VALUES (true, true)");
            assertQuery(
                    "SELECT count(DISTINCT cardinality(v)) FROM %s CROSS JOIN UNNEST(map_row.m) u(k, v)".formatted(table.getName()),
                    "VALUES (6)");
            // color has no generator, and null map keys are not allowed
            assertQuery(
                    "SELECT count(color_keys), max(cardinality(color_keys)) FROM " + table.getName(),
                    "VALUES (1000, 0)");
            // keys containing nulls are not allowed either, but empty arrays are
            assertQuery(
                    "SELECT max(cardinality(row_keys)), max(cardinality(array_keys)) FROM " + table.getName(),
                    "VALUES (0, 1)");
            assertQuery(
                    "SELECT max(cardinality(k)) FROM %s CROSS JOIN UNNEST(array_keys) u(k, v)".formatted(table.getName()),
                    "VALUES (0)");
        }
    }

    @Test
    void testSelectTypesWithoutGenerator()
    {
        try (TestTable table = newTrinoTable("without_generator", "(hll HyperLogLog NOT NULL, p4hll P4HyperLogLog, qdigest qdigest(bigint), tdigest tdigest)")) {
            assertQuery(
                    "SELECT count(*), count(hll), count(p4hll), count(qdigest), count(tdigest) FROM " + table.getName(),
                    "VALUES (1000, 0, 0, 0, 0)");
            assertQuery(
                    "SHOW STATS FOR " + table.getName(),
                    """
                    VALUES
                      ('hll', 0.0, 0.0, 1.0, NULL, NULL, NULL),
                      ('p4hll', 0.0, 0.0, 1.0, NULL, NULL, NULL),
                      ('qdigest', 0.0, 0.0, 1.0, NULL, NULL, NULL),
                      ('tdigest', 0.0, 0.0, 1.0, NULL, NULL, NULL),
                      (NULL, NULL, NULL, NULL, 1000, NULL, NULL)
                    """);
        }
    }

    @Test
    void testGeneratorDetectionCreateTableAsSelect()
    {
        String source = "SELECT CAST(email AS VARCHAR) AS email, CAST(city AS VARCHAR) AS city FROM (VALUES ('jane@example.com', 'Warsaw')) t(email, city)";
        assertThat(showCreateTable("ctas_generator_detection", "WITH (generator_detection_enabled = true) AS " + source))
                .contains(
                        "email varchar WITH (generator = '#{Internet.emailAddress}', null_probability = 0E0)",
                        "city varchar WITH (generator = '#{Address.city}', null_probability = 0E0)");
        assertThat(showCreateTable("ctas_no_generator_detection", "AS " + source))
                .doesNotContain("generator =");
    }

    @Test
    void testGeneratorDetectionProperties()
    {
        getQueryRunner().createCatalog("faker_generator_detection", "faker", Map.of("faker.generator-detection-enabled", "true"));
        assertUpdate("CREATE SCHEMA faker_generator_detection.disabled WITH (generator_detection_enabled = false)");
        assertUpdate("CREATE SCHEMA faker.generator_detection_enabled WITH (generator_detection_enabled = true)");

        assertThat(showCreateTable("faker_generator_detection.default.catalog_enabled", "(email VARCHAR)"))
                .contains("generator =");
        assertThat(showCreateTable("faker_generator_detection.default.table_disabled", "(email VARCHAR) WITH (generator_detection_enabled = false)"))
                .doesNotContain("generator =");
        assertThat(showCreateTable("faker_generator_detection.disabled.schema_disabled", "(email VARCHAR)"))
                .doesNotContain("generator =");
        assertThat(showCreateTable("faker_generator_detection.disabled.table_enabled", "(email VARCHAR) WITH (generator_detection_enabled = true)"))
                .contains("generator =");
        assertThat(showCreateTable("faker.generator_detection_enabled.schema_enabled", "(email VARCHAR)"))
                .contains("generator =");
        assertThat(showCreateTable("faker.generator_detection_enabled.table_disabled", "(email VARCHAR) WITH (generator_detection_enabled = false)"))
                .doesNotContain("generator =");

        assertUpdate("DROP SCHEMA faker_generator_detection.disabled");
        assertUpdate("DROP SCHEMA faker.generator_detection_enabled");
    }

    @Test
    void testGeneratorDetectionKeepsColumnProperties()
    {
        assertThat(showCreateTable("generator_detection_explicit", "(email VARCHAR WITH (generator = '#{Name.first_name}'), city VARCHAR WITH (allowed_values = ARRAY['Warsaw'])) WITH (generator_detection_enabled = true)"))
                .contains(
                        "email varchar WITH (generator = '#{Name.first_name}')",
                        "city varchar WITH (allowed_values = ARRAY['Warsaw'])");
        try (TestTable source = newTrinoTable("referenced_emails", "(email VARCHAR)")) {
            assertThat(showCreateTable("generator_detection_references", "(email VARCHAR WITH (references = '%s.email')) WITH (generator_detection_enabled = true)".formatted(source.getName())))
                    .contains("email varchar WITH (references = '%s.email')".formatted(source.getName()));
        }
    }

    @Test
    void testSelectDetectedGenerators()
    {
        try (TestTable table = newTrinoTable("detected_generators", "(email VARCHAR NOT NULL, country_code CHAR(2) NOT NULL, currency VARCHAR NOT NULL) WITH (generator_detection_enabled = true)")) {
            assertQuery(
                    "SELECT bool_and(strpos(email, '@') > 0), bool_and(regexp_like(country_code, '^[a-z]{2}$')), bool_and(regexp_like(currency, '^[A-Z]{3}$')) FROM " + table.getName(),
                    "VALUES (true, true, true)");
        }
    }

    private String showCreateTable(String tableName, String tableDefinition)
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, tableName, tableDefinition)) {
            return (String) computeScalar("SHOW CREATE TABLE " + table.getName());
        }
    }
}
