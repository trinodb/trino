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
    void testShowTables()
    {
        assertQuery("SHOW SCHEMAS FROM faker", "VALUES 'default', 'information_schema'");
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
                .add(new TestDataType("rnd_interval1", "interval day to second", "count(distinct rnd_interval1)", "1000"))
                .add(new TestDataType("rnd_interval2", "interval year to month", "count(distinct rnd_interval2)", "1000"))
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

            assertQuery("""
                        SELECT count(rnd_bigint)
                        FROM (SELECT rnd_bigint FROM %s LIMIT %d) a""".formatted(table.getName(), 2 * MAX_ROWS_PER_SPLIT),
                    "VALUES (%d)".formatted(2 * MAX_ROWS_PER_SPLIT));

            assertQuery("SELECT count(distinct rnd_bigint) FROM %s LIMIT 5".formatted(table.getName()),
                    "VALUES (1000)");

            assertQuery("""
                        SELECT count(rnd_bigint)
                        FROM (SELECT rnd_bigint FROM %s LIMIT %d) a""".formatted(table.getName(), MAX_ROWS_PER_SPLIT),
                    "VALUES (%d)".formatted(MAX_ROWS_PER_SPLIT));

            // generating data should be deterministic
            String testQuery = """
                               SELECT to_hex(checksum(rnd_bigint))
                               FROM (SELECT rnd_bigint FROM %s LIMIT %d) a""".formatted(table.getName(), 3 * MAX_ROWS_PER_SPLIT);
            assertQuery(testQuery, "VALUES ('1FB3289AC3A44EEA')");
            assertQuery(testQuery, "VALUES ('1FB3289AC3A44EEA')");
            assertQuery(testQuery, "VALUES ('1FB3289AC3A44EEA')");

            // there should be no overlap between data generated from different splits
            assertQuery("""
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
                    "VALUES ('Eugeniusz Szczepanik')",
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
                .add(new TestDataType("rnd_interval1", "interval day to second", Map.of("min", "0.000", "max", "0.001"), "count(distinct rnd_interval1)", "2"))
                .add(new TestDataType("rnd_interval2", "interval year to month", Map.of("min", "0", "max", "1"), "count(distinct rnd_interval2)", "2"))
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
            try (TestTable table = newTrinoTable("range_small_" + testCase.name(), "(%s)".formatted(testCase.columnSchema()))) {
                assertQuery("SELECT %s FROM %s".formatted(testCase.queryExpression(), table.getName()), "VALUES (%s)".formatted(testCase.expectedValue()));
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
                .add(new TestDataType("rnd_interval1", "interval day to second", Map.of("allowed_values", "ARRAY['0.000', '0.001']"), "count(distinct rnd_interval1)", "2"))
                .add(new TestDataType("rnd_interval2", "interval year to month", Map.of("allowed_values", "ARRAY['0', '1']"), "count(distinct rnd_interval2)", "2"))
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
                .add(new TestDataType("rnd_interval1", "interval day to second", Map.of("min", "0.000", "max", "0.009", "step", "0.001"), "count(distinct rnd_interval1)", "10"))
                .add(new TestDataType("rnd_interval2", "interval year to month", Map.of("min", "0", "max", "9", "step", "1"), "count(distinct rnd_interval2)", "10"))
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

        assertQuery("SELECT count(id) FROM (SELECT id FROM limited_range) a",
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
                .add(new TestDataType("rnd_interval1", "interval day to second", Map.of("min", "0.000", "max", "0.001"), "count(distinct rnd_interval1)", "2"))
                .add(new TestDataType("rnd_interval2", "interval year to month", Map.of("min", "0", "max", "1"), "count(distinct rnd_interval2)", "2"))
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
        String source = """
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
        String source = """
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
        String source = """
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
    void testCreateTableAsSelectBoolean()
    {
        String source = """
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
