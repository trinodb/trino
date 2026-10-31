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
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

final class TestFakerQueries
        extends AbstractTestQueryFramework
{
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
                .add(new TestDataType("rnd_date", "date", "count(distinct rnd_date)", "1000"))
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
                .add(new TestDataType("rnd_array_int", "array(integer)", "count(distinct rnd_array_int)", "1"))
                .add(new TestDataType("rnd_array_varchar", "array(varchar)", "count(distinct rnd_array_varchar)", "1"))
                .add(new TestDataType("rnd_map_int", "map(integer, integer)", "count(distinct rnd_map_int)", "1"))
                .add(new TestDataType("rnd_map_varchar", "map(varchar, varchar)", "count(distinct rnd_map_varchar)", "1"))
                .add(new TestDataType("rnd_row", "row(integer, varchar)", "count(distinct rnd_row)", "1000"))
                .add(new TestDataType("rnd_json", "json", "count(distinct rnd_json)", "1"))
                .build();

        for (TestDataType testCase : testCases) {
            try (TestTable table = newTrinoTable("types_" + testCase.name(), "(%s)".formatted(testCase.columnSchema()))) {
                assertQuery("SELECT %s FROM %s".formatted(testCase.queryExpression(), table.getName()), "VALUES (%s)".formatted(testCase.expectedValue()));
            }
        }
    }

    @Test
    void testSelectLimit()
    {
        try (TestTable table = newTrinoTable("single_column", "(rnd_bigint bigint NOT NULL)")) {
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
            assertQuery(testQuery, "VALUES ('1FB3289AC3A44EEA')");
            assertQuery(testQuery, "VALUES ('1FB3289AC3A44EEA')");
            assertQuery(testQuery, "VALUES ('1FB3289AC3A44EEA')");

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
                """)) {
            assertQuery("SELECT name FROM " + table.getName() + " LIMIT 1", "VALUES ('Bev Runolfsson')");
        }
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
                        """)) {
            QueryAssertions.assertQuery(
                    queryRunner,
                    getSession(),
                    "SELECT name FROM " + table.getName() + " LIMIT 1",
                    h2QueryRunner,
                    "VALUES ('Klementyna Szczepanik')",
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
                .add(new TestDataType("rnd_timestamp12", "timestamp(12)", Map.of("min", "2022-03-21 00:00:00.000000000001", "max", "2022-03-21 00:00:00.000000000002"), "count(distinct rnd_timestamp12)", "2"))
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
    void testSelectStepProperties()
    {
        // small step in small ranges that produce only 10 unique values for 1000 rows
        List<TestDataType> testCases = ImmutableList.<TestDataType>builder()
                .add(new TestDataType("rnd_bigint", "bigint", Map.of("min", "0", "max", "9", "step", "1"), "count(distinct rnd_bigint)", "10"))
                .add(new TestDataType("rnd_integer", "integer", Map.of("min", "0", "max", "9", "step", "1"), "count(distinct rnd_integer)", "10"))
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
                .add(new TestDataType("rnd_bigint", "bigint", Map.of("min", "9223372036854775798", "max", "9223372036854775807", "step", "1"), "count(distinct rnd_bigint)", "10"))
                .add(new TestDataType("rnd_integer", "integer", Map.of("min", "2147483638", "max", "2147483647", "step", "1"), "count(distinct rnd_integer)", "10"))
                .add(new TestDataType("rnd_date", "date", Map.of("min", "5881580-07-02", "max", "5881580-07-11", "step", "1d"), "count(distinct rnd_date)", "10"))
                .add(new TestDataType("rnd_timestamp6", "timestamp(6)", Map.of("min", "294247-01-10 04:00:54.775798", "max", "294247-01-10 04:00:54.775807", "step", "1us"), "count(distinct rnd_timestamp6)", "10"))
                .add(new TestDataType("rnd_timestamp9", "timestamp(9)", Map.of("min", "294247-01-10 04:00:54.775798000", "max", "294247-01-10 04:00:54.775807999", "step", "1us"), "count(distinct rnd_timestamp9)", "10"))
                .build();

        for (TestDataType testCase : testCases) {
            try (TestTable table = newTrinoTable("step_top_" + testCase.name(), "(%s)".formatted(testCase.columnSchema()))) {
                assertQuery("SELECT %s FROM %s".formatted(testCase.queryExpression(), table.getName()), "VALUES (%s)".formatted(testCase.expectedValue()));
            }
        }

        // long sequences with steps large enough to overflow when multiplied by the row number
        testCases = ImmutableList.<TestDataType>builder()
                .add(new TestDataType("rnd_date", "date", Map.of("min", "2000-01-01", "step", "1d"), "count(distinct rnd_date) = 200000 AND min(rnd_date) = DATE '2000-01-01' AND max(rnd_date) = DATE '2000-01-01' + INTERVAL '199999' DAY", "true"))
                .add(new TestDataType("rnd_timestamp", "timestamp", Map.of("min", "2000-01-01 00:00:00.000", "step", "1d"), "count(distinct rnd_timestamp) = 200000 AND min(rnd_timestamp) = TIMESTAMP '2000-01-01 00:00:00.000'", "true"))
                .add(new TestDataType("rnd_bigint", "bigint", Map.of("min", "0", "step", "100000000000000"), "count(distinct rnd_bigint) = 92235 AND min(rnd_bigint) = 0", "true"))
                .add(new TestDataType("rnd_bigint_wide", "bigint", Map.of("min", "-9223372036854775808", "max", "9223372036854775807", "step", "9223372036854775807"), "count(distinct rnd_bigint_wide) = 4 AND max(rnd_bigint_wide) = 9223372036854775807", "true"))
                .build();

        for (TestDataType testCase : testCases) {
            try (TestTable table = newTrinoTable("step_long_" + testCase.name(), "(%s) WITH (default_limit = 200000)".formatted(testCase.columnSchema()))) {
                assertQuery("SELECT %s FROM %s".formatted(testCase.queryExpression(), table.getName()), "VALUES (%s)".formatted(testCase.expectedValue()));
            }
        }

        // without a min low bound, sequences start at the min value of the type
        testCases = ImmutableList.<TestDataType>builder()
                .add(new TestDataType("rnd_bigint", "bigint", Map.of("max", "0", "step", "1"), "min(rnd_bigint) = -9223372036854775808 AND max(rnd_bigint) < 0", "true"))
                .add(new TestDataType("rnd_timestamp", "timestamp", Map.of("max", "2022-03-21 00:00:00.000", "step", "1s"), "year(min(rnd_timestamp)) < -290000 AND max(rnd_timestamp) < TIMESTAMP '2022-03-21 00:00:00.000'", "true"))
                .add(new TestDataType("rnd_timestamp0", "timestamp(0)", Map.of("max", "2022-03-21 00:00:00", "step", "1s"), "year(min(rnd_timestamp0)) < -290000 AND max(rnd_timestamp0) < TIMESTAMP '2022-03-21 00:00:00'", "true"))
                .build();

        for (TestDataType testCase : testCases) {
            try (TestTable table = newTrinoTable("step_bottom_" + testCase.name(), "(%s)".formatted(testCase.columnSchema()))) {
                assertQuery("SELECT %s FROM %s".formatted(testCase.queryExpression(), table.getName()), "VALUES (%s)".formatted(testCase.expectedValue()));
            }
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
                            count_if(position BETWEEN 0.45 AND 0.55) > 4 * count_if(position < 0.1 OR position > 0.9)
                        FROM (SELECT x, %3$s AS position FROM %4$s)
                        """.formatted(testCase.minValue(), testCase.maxValue(), testCase.position(), table.getName()),
                        "VALUES (true, true)");
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
                            CAST(max(frequency) FILTER (WHERE rank = 1) AS double) / max(frequency) FILTER (WHERE rank = 2) BETWEEN 1.7 AND 2.3
                        FROM (
                            SELECT x, frequency, row_number() OVER (ORDER BY x) AS rank
                            FROM (SELECT x, count(*) AS frequency FROM %3$s GROUP BY x))
                        """.formatted(testCase.minValue(), testCase.maxValue(), table.getName()),
                        "VALUES (true, true, true)");
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
                            max_by(x, frequency) = %1$s
                        FROM (SELECT x, count(*) AS frequency FROM %3$s GROUP BY x)
                        """.formatted(testCase.minValue(), testCase.maxValue(), table.getName()),
                        "VALUES (true, true)");
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
                new TestCase("bigint", "ARRAY['3', '1', '2', '5', '4']", "ARRAY[3, 1]"),
                new TestCase("double", "ARRAY['0.5', 'NaN', '-1.0', '1.0', '0.0']", "ARRAY[0.5, nan()]"),
                new TestCase("varchar", "ARRAY['gold', 'silver', 'bronze', 'iron', 'wood']", "ARRAY['gold', 'silver']"),
                new TestCase("char(6)", "ARRAY['gold', 'silver', 'bronze', 'iron', 'wood']", "ARRAY[CAST('gold' AS char(6)), CAST('silver' AS char(6))]"),
                new TestCase("boolean", "ARRAY['true', 'false']", "ARRAY[true, false]"),
                new TestCase("uuid", "ARRAY['3ee49ede-0026-45e4-ba06-08404f794557', '1fc74d96-0216-449b-a145-455578a9eaa5']", "ARRAY[UUID '3ee49ede-0026-45e4-ba06-08404f794557', UUID '1fc74d96-0216-449b-a145-455578a9eaa5']"));

        for (TestCase testCase : testCases) {
            String columnSchema = "x %s NOT NULL WITH (allowed_values = %s, distribution = 'zipf')".formatted(testCase.type(), testCase.allowedValues());
            try (TestTable table = newTrinoTable("zipf_allowed_values", "(%s) WITH (default_limit = 10000)".formatted(columnSchema))) {
                // the first value is the most frequent, and twice as frequent as the second one
                assertQuery(
                        """
                        SELECT
                            array_agg(x ORDER BY frequency DESC)[1] IS NOT DISTINCT FROM (%1$s)[1],
                            array_agg(x ORDER BY frequency DESC)[2] IS NOT DISTINCT FROM (%1$s)[2],
                            CAST(max(frequency) AS double) / max_by(frequency, x IS NOT DISTINCT FROM (%1$s)[2]) BETWEEN 1.7 AND 2.3
                        FROM (SELECT x, count(*) AS frequency FROM %2$s GROUP BY x)
                        """.formatted(testCase.mostFrequentValues(), table.getName()),
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
            assertThat(createTable).containsPattern("orderstatus varchar\\(1\\)");
            assertThat(createTable).containsPattern("totalprice double WITH \\(max = '.*', min = '.*', null_probability = 0E0\\)");
            assertThat(createTable).containsPattern("orderdate date WITH \\(max = '1998-08-02', min = '1992-01-01', null_probability = 0E0\\)");
            assertThat(createTable).containsPattern("orderpriority varchar\\(15\\)");
            assertThat(createTable).containsPattern("clerk varchar\\(15\\)");
            assertThat(createTable).containsPattern("shippriority integer WITH \\(allowed_values = ARRAY\\['0'], null_probability = 0E0\\)");
            assertThat(createTable).containsPattern("comment varchar\\(79\\)");
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
}
