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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.H2QueryRunner;
import io.trino.testing.QueryAssertions;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.plugin.faker.FakerSplitManager.MAX_ROWS_PER_SPLIT;
import static io.trino.spi.StandardErrorCode.INVALID_COLUMN_REFERENCE;
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
    void testColumnComment()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "comment", "(id INTEGER, name VARCHAR)")) {
            assertUpdate("COMMENT ON COLUMN %s.name IS 'comment text'".formatted(table.getName()));
            assertQuery("SHOW COLUMNS FROM " + table.getName(), "VALUES ('id', 'integer', '', ''), ('name', 'varchar', '', 'comment text')");
        }
    }

    @Test
    void testCannotCommentRowId()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "cannot_comment", "(id INTEGER, name VARCHAR)")) {
            assertThat(query("COMMENT ON COLUMN \"%s\".\"$row_id\" IS 'comment text'".formatted(table.getName())))
                    .failure()
                    .hasErrorCode(INVALID_COLUMN_REFERENCE)
                    .hasMessageContaining("Cannot set comment for $row_id column");
        }
    }

    @Test
    void testSelectFromTable()
    {
        @Language("SQL")
        String tableQuery =
                """
                CREATE TABLE faker.default.all_types (
                rnd_bigint bigint NOT NULL,
                rnd_integer integer NOT NULL,
                rnd_smallint smallint NOT NULL,
                rnd_tinyint tinyint NOT NULL,
                rnd_boolean boolean NOT NULL,
                rnd_date date NOT NULL,
                rnd_decimal1 decimal NOT NULL,
                rnd_decimal2 decimal(18,5) NOT NULL,
                rnd_decimal3 decimal(38,0) NOT NULL,
                rnd_decimal4 decimal(38,38) NOT NULL,
                rnd_decimal5 decimal(5,2) NOT NULL,
                rnd_real real NOT NULL,
                rnd_double double NOT NULL,
                rnd_interval_day_time interval day to second NOT NULL,
                rnd_interval_year interval year to month NOT NULL,
                rnd_timestamp timestamp NOT NULL,
                rnd_timestamp0 timestamp(0) NOT NULL,
                rnd_timestamp6 timestamp(6) NOT NULL,
                rnd_timestamp9 timestamp(9) NOT NULL,
                rnd_timestamptz timestamp with time zone NOT NULL,
                rnd_timestamptz0 timestamp(0) with time zone NOT NULL,
                rnd_timestamptz6 timestamp(6) with time zone NOT NULL,
                rnd_timestamptz9 timestamp(9) with time zone NOT NULL,
                rnd_time time NOT NULL,
                rnd_time0 time(0) NOT NULL,
                rnd_time6 time(6) NOT NULL,
                rnd_time9 time(9) NOT NULL,
                rnd_timetz time with time zone NOT NULL,
                rnd_timetz0 time(0) with time zone NOT NULL,
                rnd_timetz6 time(6) with time zone NOT NULL,
                rnd_timetz9 time(9) with time zone NOT NULL,
                rnd_timetz12 time(12) with time zone NOT NULL,
                rnd_varbinary varbinary NOT NULL,
                rnd_varchar varchar NOT NULL,
                rnd_nvarchar varchar(1000) NOT NULL,
                rnd_char char NOT NULL,
                rnd_nchar char(1000) NOT NULL,
                rnd_ipaddress ipaddress NOT NULL,
                rnd_uuid uuid NOT NULL)""";
        assertUpdate(tableQuery);

        @Language("SQL")
        String testQuery =
                """
                SELECT
                count(distinct rnd_bigint),
                count(distinct rnd_integer),
                count(rnd_smallint),
                count(rnd_tinyint),
                count(distinct rnd_boolean),
                count(distinct rnd_date),
                count(distinct rnd_decimal1),
                count(distinct rnd_decimal2),
                count(distinct rnd_decimal3),
                count(distinct rnd_decimal4),
                count(rnd_decimal5),
                count(rnd_real),
                count(distinct rnd_double),
                count(distinct rnd_interval_day_time),
                count(distinct rnd_interval_year),
                count(distinct rnd_timestamp),
                count(distinct rnd_timestamp0),
                count(distinct rnd_timestamp6),
                count(distinct rnd_timestamp9),
                count(distinct rnd_timestamptz),
                count(distinct rnd_timestamptz0),
                count(distinct rnd_timestamptz6),
                count(distinct rnd_timestamptz9),
                count(rnd_time),
                count(rnd_time0),
                count(distinct rnd_time6),
                count(distinct rnd_time9),
                count(rnd_timetz),
                count(rnd_timetz0),
                count(distinct rnd_timetz6),
                count(distinct rnd_timetz9),
                count(distinct rnd_varbinary),
                count(rnd_varchar),
                count(rnd_nvarchar),
                count(distinct rnd_char),
                count(distinct rnd_nchar),
                count(distinct rnd_ipaddress),
                count(distinct rnd_uuid)
                FROM all_types""";
        assertQuery(testQuery,
                """
                VALUES (
                1000,
                1000,
                1000,
                1000,
                -- boolean, date
                2,
                1000,
                -- decimal
                1000,
                1000,
                1000,
                1000,
                1000,
                -- real, double
                1000,
                1000,
                -- intervals
                1000,
                1000,
                -- timestamps
                1000,
                1000,
                1000,
                1000,
                -- timestamps with time zone
                1000,
                1000,
                1000,
                1000,
                -- time
                1000,
                1000,
                1000,
                1000,
                -- time with time zone
                1000,
                1000,
                1000,
                1000,
                -- varbinary, varchar, nvarchar, char, nchar
                1000,
                1000,
                1000,
                19,
                1000,
                -- ip address, uuid
                1000,
                1000)""");
        assertUpdate("DROP TABLE faker.default.all_types");
    }

    @Test
    void testSelectLimit()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "single_column", "(rnd_bigint bigint NOT NULL)")) {
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
        try (TestTable table = new TestTable(getQueryRunner()::execute, "default_table_limit", "(rnd_bigint bigint NOT NULL) WITH (default_limit = 100)")) {
            assertQuery("SELECT count(distinct rnd_bigint) FROM " + table.getName(), "VALUES (100)");
        }
    }

    @Test
    public void selectOnlyNulls()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "only_nulls", "(rnd_bigint bigint) WITH (null_probability = 1.0)")) {
            assertQuery("SELECT count(distinct rnd_bigint) FROM " + table.getName(), "VALUES (0)");
        }
    }

    @Test
    void testSelectGenerator()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "generators",
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
        @Language("SQL")
        String tableQuery;
        @Language("SQL")
        String testQuery;

        // inclusive ranges that produce only 2 values
        // high boundary float value obtained using `Math.nextUp((float) 0.0)`
        tableQuery =
                """
                CREATE TABLE faker.default.all_types_range_prop (
                rnd_bigint bigint NOT NULL WITH (min = '0', max = '1'),
                rnd_integer integer NOT NULL WITH (min = '0', max = '1'),
                rnd_smallint smallint NOT NULL WITH (min = '0', max = '1'),
                rnd_tinyint tinyint NOT NULL WITH (min = '0', max = '1'),
                rnd_boolean boolean NOT NULL,
                rnd_date date NOT NULL WITH (min = '2022-03-01', max = '2022-03-02'),
                rnd_decimal1 decimal NOT NULL WITH (min = '0', max = '1'),
                rnd_decimal2 decimal(18,5) NOT NULL WITH (min = '0.00000', max = '0.00001'),
                rnd_decimal3 decimal(38,0) NOT NULL WITH (min = '0', max = '1'),
                rnd_decimal4 decimal(38,38) NOT NULL WITH (min = '0.00000000000000000000000000000000000000', max = '0.00000000000000000000000000000000000001'),
                rnd_decimal5 decimal(5,2) NOT NULL WITH (min = '0.00', max = '0.01'),
                rnd_real real NOT NULL WITH (min = '0.0', max = '1.4E-45'),
                rnd_double double NOT NULL WITH (min = '0.0', max = '4.9E-324'),
                rnd_interval_day_time interval day to second NOT NULL WITH (min = '0.000', max = '0.001'),
                rnd_interval_year interval year to month NOT NULL WITH (min = '0', max = '1'),
                rnd_timestamp timestamp NOT NULL WITH (min = '2022-03-21 00:00:00.000', max = '2022-03-21 00:00:00.001'),
                rnd_timestamp0 timestamp(0) NOT NULL WITH (min = '2022-03-21 00:00:00', max = '2022-03-21 00:00:01'),
                rnd_timestamp6 timestamp(6) NOT NULL WITH (min = '2022-03-21 00:00:00.000000', max = '2022-03-21 00:00:00.000001'),
                rnd_timestamp9 timestamp(9) NOT NULL WITH (min = '2022-03-21 00:00:00.000000000', max = '2022-03-21 00:00:00.000000001'),
                rnd_timestamptz timestamp with time zone NOT NULL WITH (min = '2022-03-21 00:00:00.000 +01:00', max = '2022-03-21 00:00:00.001 +01:00'),
                rnd_timestamptz0 timestamp(0) with time zone NOT NULL WITH (min = '2022-03-21 00:00:00 +01:00', max = '2022-03-21 00:00:01 +01:00'),
                rnd_timestamptz6 timestamp(6) with time zone NOT NULL WITH (min = '2022-03-21 00:00:00.000000 +01:00', max = '2022-03-21 00:00:00.000001 +01:00'),
                rnd_timestamptz9 timestamp(9) with time zone NOT NULL WITH (min = '2022-03-21 00:00:00.000000000 +01:00', max = '2022-03-21 00:00:00.000000001 +01:00'),
                rnd_time time NOT NULL WITH (min = '01:02:03.456', max = '01:02:03.457'),
                rnd_time0 time(0) NOT NULL WITH (min = '01:02:03', max = '01:02:04'),
                rnd_time6 time(6) NOT NULL WITH (min = '01:02:03.000456', max = '01:02:03.000457'),
                rnd_time9 time(9) NOT NULL WITH (min = '01:02:03.000000456', max = '01:02:03.000000457'),
                rnd_timetz time with time zone NOT NULL WITH (min = '01:02:03.456 +01:00', max = '01:02:03.457 +01:00'),
                rnd_timetz0 time(0) with time zone NOT NULL WITH (min = '01:02:03 +01:00', max = '01:02:04 +01:00'),
                rnd_timetz6 time(6) with time zone NOT NULL WITH (min = '01:02:03.000456 +01:00', max = '01:02:03.000457 +01:00'),
                rnd_timetz9 time(9) with time zone NOT NULL WITH (min = '01:02:03.000000456 +01:00', max = '01:02:03.000000457 +01:00'),
                rnd_timetz12 time(12) with time zone NOT NULL,
                rnd_varbinary varbinary NOT NULL,
                rnd_varchar varchar NOT NULL,
                rnd_nvarchar varchar(1000) NOT NULL,
                rnd_char char NOT NULL,
                rnd_nchar char(1000) NOT NULL,
                rnd_ipaddress ipaddress NOT NULL,
                rnd_uuid uuid NOT NULL)""";
        assertUpdate(tableQuery);

        testQuery =
                """
                SELECT
                count(distinct rnd_bigint),
                count(distinct rnd_integer),
                count(distinct rnd_smallint),
                count(distinct rnd_tinyint),
                count(distinct rnd_date),
                count(distinct rnd_decimal1),
                count(distinct rnd_decimal2),
                count(distinct rnd_decimal3),
                count(distinct rnd_decimal4),
                count(distinct rnd_decimal5),
                count(distinct rnd_real),
                count(distinct rnd_double),
                count(distinct rnd_interval_day_time),
                count(distinct rnd_interval_year),
                count(distinct rnd_timestamp),
                count(distinct rnd_timestamp0),
                count(distinct rnd_timestamp6),
                count(distinct rnd_timestamp9),
                count(distinct rnd_timestamptz),
                count(distinct rnd_timestamptz0),
                count(distinct rnd_timestamptz6),
                count(distinct rnd_timestamptz9),
                count(distinct rnd_time),
                count(distinct rnd_time0),
                count(distinct rnd_time6),
                count(distinct rnd_time9),
                count(distinct rnd_timetz),
                count(distinct rnd_timetz0),
                count(distinct rnd_timetz6),
                count(distinct rnd_timetz9)
                FROM all_types_range_prop
                """;
        assertQuery(testQuery,
                """
                VALUES (2,
                2,
                2,
                2,
                -- date
                2,
                -- decimal
                2,
                2,
                2,
                2,
                2,
                -- real, double
                2,
                2,
                -- intervals
                2,
                2,
                -- timestamps
                2,
                2,
                2,
                2,
                -- timestamps with time zone
                2,
                2,
                2,
                2,
                -- time
                2,
                2,
                2,
                2,
                -- time with time zone
                2,
                2,
                2,
                2)
                """);

        // inclusive range to get the min low bound
        tableQuery =
                """
                CREATE TABLE faker.default.all_types_max_prop (
                rnd_bigint bigint NOT NULL WITH (max = '-9223372036854775808'),
                rnd_integer integer NOT NULL WITH (max = '-2147483648'),
                rnd_smallint smallint NOT NULL WITH (max = '-32768'),
                rnd_tinyint tinyint NOT NULL WITH (max = '-128'),
                rnd_boolean boolean NOT NULL,
                rnd_date date NOT NULL WITH (max = '-5877641-06-23'),
                rnd_decimal1 decimal NOT NULL WITH (max = '-99999999999999999999999999999999999999'),
                rnd_decimal2 decimal(18,5) NOT NULL WITH (max = '-9999999999999.99999'),
                rnd_decimal3 decimal(38,0) NOT NULL WITH (max = '-99999999999999999999999999999999999999'),
                rnd_decimal4 decimal(38,38) NOT NULL WITH (max = '-0.99999999999999999999999999999999999999'),
                -- TODO it actually retdurns '-999.98'
                rnd_decimal5 decimal(5,2) NOT NULL WITH (max = '-999.99'),
                rnd_real real NOT NULL WITH (max = '1.4E-45'),
                rnd_double double NOT NULL WITH (max = '4.9E-324'),
                -- interval literals can't represent smallest possible values allowed by the engine
                rnd_interval_day_time interval day to second NOT NULL,
                rnd_interval_year interval year to month NOT NULL,
                -- can't test timestamps because their extreme values cannot be expressed as literals
                rnd_timestamp timestamp NOT NULL,
                rnd_timestamp0 timestamp(0) NOT NULL,
                rnd_timestamp6 timestamp(6) NOT NULL,
                rnd_timestamp9 timestamp(9) NOT NULL,
                rnd_timestamptz timestamp with time zone NOT NULL,
                rnd_timestamptz0 timestamp(0) with time zone NOT NULL,
                rnd_timestamptz6 timestamp(6) with time zone NOT NULL,
                rnd_timestamptz9 timestamp(9) with time zone NOT NULL,
                rnd_time time NOT NULL WITH (max = '00:00:00.000'),
                rnd_time0 time(0) NOT NULL WITH (max = '00:00:00'),
                rnd_time6 time(6) NOT NULL WITH (max = '00:00:00.000000'),
                rnd_time9 time(9) NOT NULL WITH (max = '00:00:00.000000000'),
                rnd_timetz time with time zone NOT NULL WITH (max = '00:00:00.000 +01:00'),
                rnd_timetz0 time(0) with time zone NOT NULL WITH (max = '00:00:00 +01:00'),
                rnd_timetz6 time(6) with time zone NOT NULL WITH (max = '00:00:00.000000 +01:00'),
                rnd_timetz9 time(9) with time zone NOT NULL WITH (max = '00:00:00.000000000 +01:00'),
                rnd_timetz12 time(12) with time zone NOT NULL,
                rnd_varbinary varbinary NOT NULL,
                rnd_varchar varchar NOT NULL,
                rnd_nvarchar varchar(1000) NOT NULL,
                rnd_char char NOT NULL,
                rnd_nchar char(1000) NOT NULL,
                rnd_ipaddress ipaddress NOT NULL,
                rnd_uuid uuid NOT NULL)""";
        assertUpdate(tableQuery);

        testQuery =
                """
                SELECT
                count(distinct rnd_bigint),
                count(distinct rnd_integer),
                count(distinct rnd_smallint),
                count(distinct rnd_tinyint),
                count(distinct rnd_date),
                count(distinct rnd_decimal1),
                count(distinct rnd_decimal2),
                count(distinct rnd_decimal3),
                count(distinct rnd_decimal4),
                count(distinct rnd_decimal5),
                count(distinct rnd_real),
                count(distinct rnd_double),
                -- interval literals can't represent smallest possible values allowed by the engine
                -- can't count timestamps because their extreme values cannot be expressed as literals
                count(distinct rnd_time),
                count(distinct rnd_time0),
                count(distinct rnd_time6),
                count(distinct rnd_time9),
                count(distinct rnd_timetz),
                count(distinct rnd_timetz0),
                count(distinct rnd_timetz6),
                count(distinct rnd_timetz9)
                FROM all_types_max_prop
                """;
        assertQuery(testQuery,
                """
                VALUES (1,
                1,
                1,
                1,
                -- date
                1,
                -- decimal
                1,
                1,
                1,
                1,
                1,
                -- real, double
                1,
                1,
                -- time
                1,
                1,
                1,
                1,
                -- time with time zone
                1,
                1,
                1,
                1)
                """);

        // exclusive range to get the max high bound
        tableQuery =
                """
                CREATE TABLE faker.default.all_types_min_prop (
                rnd_bigint bigint NOT NULL WITH (min = '9223372036854775807'),
                rnd_integer integer NOT NULL WITH (min = '2147483647'),
                rnd_smallint smallint NOT NULL WITH (min = '32767'),
                rnd_tinyint tinyint NOT NULL WITH (min = '127'),
                rnd_boolean boolean NOT NULL,
                rnd_date date NOT NULL WITH (min = '5881580-07-11'),
                rnd_decimal1 decimal NOT NULL WITH (min = '99999999999999999999999999999999999999'),
                rnd_decimal2 decimal(18,5) NOT NULL WITH (min = '9999999999999.99999'),
                rnd_decimal3 decimal(38,0) NOT NULL WITH (min = '99999999999999999999999999999999999999'),
                rnd_decimal4 decimal(38,38) NOT NULL WITH (min = '0.99999999999999999999999999999999999999'),
                rnd_decimal5 decimal(5,2) NOT NULL WITH (min = '999.99'),
                rnd_real real NOT NULL WITH (min = '1.4E45'),
                rnd_double double NOT NULL WITH (min = '4.9E324'),
                -- interval literals can't represent smallest possible values allowed by the engine
                rnd_interval_day_time interval day to second NOT NULL,
                rnd_interval_year interval year to month NOT NULL,
                -- can't test timestamps because their extreme values cannot be expressed as literals
                rnd_timestamp timestamp NOT NULL,
                rnd_timestamp0 timestamp(0) NOT NULL,
                rnd_timestamp6 timestamp(6) NOT NULL,
                rnd_timestamp9 timestamp(9) NOT NULL,
                rnd_timestamptz timestamp with time zone NOT NULL,
                rnd_timestamptz0 timestamp(0) with time zone NOT NULL,
                rnd_timestamptz6 timestamp(6) with time zone NOT NULL,
                rnd_timestamptz9 timestamp(9) with time zone NOT NULL,
                rnd_time time NOT NULL WITH (min = '23:59:59.999'),
                rnd_time0 time(0) NOT NULL WITH (min = '23:59:59'),
                rnd_time6 time(6) NOT NULL WITH (min = '23:59:59.999999'),
                rnd_time9 time(9) NOT NULL WITH (min = '23:59:59.999999999'),
                rnd_timetz time with time zone NOT NULL WITH (min = '23:59:59.999 +01:00'),
                rnd_timetz0 time(0) with time zone NOT NULL WITH (min = '23:59:59 +01:00'),
                rnd_timetz6 time(6) with time zone NOT NULL WITH (min = '23:59:59.999999 +01:00'),
                rnd_timetz9 time(9) with time zone NOT NULL WITH (min = '23:59:59.999999999 +01:00'),
                rnd_timetz12 time(12) with time zone NOT NULL,
                rnd_varbinary varbinary NOT NULL,
                rnd_varchar varchar NOT NULL,
                rnd_nvarchar varchar(1000) NOT NULL,
                rnd_char char NOT NULL,
                rnd_nchar char(1000) NOT NULL,
                rnd_ipaddress ipaddress NOT NULL,
                rnd_uuid uuid NOT NULL)""";
        assertUpdate(tableQuery);

        testQuery =
                """
                SELECT
                count(distinct rnd_bigint),
                count(distinct rnd_integer),
                count(distinct rnd_smallint),
                count(distinct rnd_tinyint),
                count(distinct rnd_date),
                count(distinct rnd_decimal1),
                count(distinct rnd_decimal2),
                count(distinct rnd_decimal3),
                count(distinct rnd_decimal4),
                count(distinct rnd_decimal5),
                count(distinct rnd_real),
                count(distinct rnd_double),
                -- interval literals can't represent smallest possible values allowed by the engine
                -- can't count timestamps because their extreme values cannot be expressed as literals
                count(distinct rnd_time),
                count(distinct rnd_time0),
                count(distinct rnd_time6),
                count(distinct rnd_time9),
                count(distinct rnd_timetz),
                count(distinct rnd_timetz0),
                count(distinct rnd_timetz6),
                count(distinct rnd_timetz9)
                FROM all_types_min_prop
                """;
        assertQuery(testQuery,
                """
                VALUES (1,
                1,
                1,
                1,
                -- date
                1,
                -- decimal
                1,
                1,
                1,
                1,
                1,
                -- real, double
                1,
                1,
                -- time
                1,
                1,
                1,
                1,
                -- time with time zone
                1,
                1,
                1,
                1)
                """);

        assertUpdate("DROP TABLE faker.default.all_types_range_prop");
        assertUpdate("DROP TABLE faker.default.all_types_max_prop");
        assertUpdate("DROP TABLE faker.default.all_types_min_prop");
    }

    @Test
    void testSelectValuesProperty()
    {
        @Language("SQL")
        String tableQuery =
                """
                CREATE TABLE faker.default.all_types_values (
                rnd_bigint bigint NOT NULL WITH ("allowed_values" = ARRAY['0', '1']),
                rnd_integer integer NOT NULL WITH ("allowed_values" = ARRAY['0', '1']),
                rnd_smallint smallint NOT NULL WITH ("allowed_values" = ARRAY['0', '1']),
                rnd_tinyint tinyint NOT NULL WITH ("allowed_values" = ARRAY['0', '1']),
                rnd_boolean boolean NOT NULL WITH ("allowed_values" = ARRAY['true', 'false']),
                rnd_date date NOT NULL WITH ("allowed_values" = ARRAY['2022-03-01', '2022-03-02']),
                rnd_decimal1 decimal NOT NULL WITH ("allowed_values" = ARRAY['0', '1']),
                rnd_decimal2 decimal(18,5) NOT NULL WITH ("allowed_values" = ARRAY['0.00000', '0.00001']),
                rnd_decimal3 decimal(38,0) NOT NULL WITH ("allowed_values" = ARRAY['0', '1']),
                rnd_decimal4 decimal(38,38) NOT NULL WITH ("allowed_values" = ARRAY['0.00000000000000000000000000000000000000', '0.00000000000000000000000000000000000001']),
                rnd_decimal5 decimal(5,2) NOT NULL WITH ("allowed_values" = ARRAY['0.00', '0.01']),
                rnd_real real NOT NULL WITH ("allowed_values" = ARRAY['0.0', '1.4E-45']),
                rnd_double double NOT NULL WITH ("allowed_values" = ARRAY['0.0', '4.9E-324']),
                rnd_interval_day_time interval day to second NOT NULL WITH ("allowed_values" = ARRAY['0.000', '0.001']),
                rnd_interval_year interval year to month NOT NULL WITH ("allowed_values" = ARRAY['0', '1']),
                rnd_timestamp timestamp NOT NULL WITH ("allowed_values" = ARRAY['2022-03-21 00:00:00.000', '2022-03-21 00:00:00.001']),
                rnd_timestamp0 timestamp(0) NOT NULL WITH ("allowed_values" = ARRAY['2022-03-21 00:00:00', '2022-03-21 00:00:01']),
                rnd_timestamp6 timestamp(6) NOT NULL WITH ("allowed_values" = ARRAY['2022-03-21 00:00:00.000000', '2022-03-21 00:00:00.000001']),
                rnd_timestamp9 timestamp(9) NOT NULL WITH ("allowed_values" = ARRAY['2022-03-21 00:00:00.000000000', '2022-03-21 00:00:00.000000001']),
                rnd_timestamptz timestamp with time zone NOT NULL WITH ("allowed_values" = ARRAY['2022-03-21 00:00:00.000 +01:00', '2022-03-21 00:00:00.001 +01:00']),
                rnd_timestamptz0 timestamp(0) with time zone NOT NULL WITH ("allowed_values" = ARRAY['2022-03-21 00:00:00 +01:00', '2022-03-21 00:00:01 +01:00']),
                rnd_timestamptz6 timestamp(6) with time zone NOT NULL WITH ("allowed_values" = ARRAY['2022-03-21 00:00:00.000000 +01:00', '2022-03-21 00:00:00.000001 +01:00']),
                rnd_timestamptz9 timestamp(9) with time zone NOT NULL WITH ("allowed_values" = ARRAY['2022-03-21 00:00:00.000000000 +01:00', '2022-03-21 00:00:00.000000001 +01:00']),
                rnd_time time NOT NULL WITH ("allowed_values" = ARRAY['01:02:03.456', '01:02:03.457']),
                rnd_time0 time(0) NOT NULL WITH ("allowed_values" = ARRAY['01:02:03', '01:02:04']),
                rnd_time6 time(6) NOT NULL WITH ("allowed_values" = ARRAY['01:02:03.000456', '01:02:03.000457']),
                rnd_time9 time(9) NOT NULL WITH ("allowed_values" = ARRAY['01:02:03.000000456', '01:02:03.000000457']),
                rnd_timetz time with time zone NOT NULL WITH ("allowed_values" = ARRAY['01:02:03.456 +01:00', '01:02:03.457 +01:00']),
                rnd_timetz0 time(0) with time zone NOT NULL WITH ("allowed_values" = ARRAY['01:02:03 +01:00', '01:02:04 +01:00']),
                rnd_timetz6 time(6) with time zone NOT NULL WITH ("allowed_values" = ARRAY['01:02:03.000456 +01:00', '01:02:03.000457 +01:00']),
                rnd_timetz9 time(9) with time zone NOT NULL WITH ("allowed_values" = ARRAY['01:02:03.000000456 +01:00', '01:02:03.000000457 +01:00']),
                rnd_timetz12 time(12) with time zone NOT NULL,
                rnd_varbinary varbinary NOT NULL WITH ("allowed_values" = ARRAY['ff', '00']),
                rnd_varchar varchar NOT NULL WITH ("allowed_values" = ARRAY['aa', 'bb']),
                rnd_nvarchar varchar(1000) NOT NULL WITH ("allowed_values" = ARRAY['aa', 'bb']),
                rnd_ipaddress ipaddress NOT NULL WITH ("allowed_values" = ARRAY['0.0.0.0', '1.2.3.4']),
                rnd_uuid uuid NOT NULL WITH ("allowed_values" = ARRAY['1fc74d96-0216-449b-a145-455578a9eaa5', '3ee49ede-0026-45e4-ba06-08404f794557']))
                """;
        assertUpdate(tableQuery);

        @Language("SQL")
        String testQuery;

        // inclusive ranges (BETWEEN) that produce only 2 values
        // obtained using `Math.nextUp((float) 0.0)`
        testQuery =
                """
                SELECT
                count(distinct rnd_bigint),
                count(distinct rnd_integer),
                count(distinct rnd_smallint),
                count(distinct rnd_tinyint),
                count(distinct rnd_date),
                count(distinct rnd_decimal1),
                count(distinct rnd_decimal2),
                count(distinct rnd_decimal3),
                count(distinct rnd_decimal4),
                count(distinct rnd_decimal5),
                count(distinct rnd_real),
                count(distinct rnd_double),
                count(distinct rnd_interval_day_time),
                count(distinct rnd_interval_year),
                count(distinct rnd_timestamp),
                count(distinct rnd_timestamp0),
                count(distinct rnd_timestamp6),
                count(distinct rnd_timestamp9),
                count(distinct rnd_timestamptz),
                count(distinct rnd_timestamptz0),
                count(distinct rnd_timestamptz6),
                count(distinct rnd_timestamptz9),
                count(distinct rnd_time),
                count(distinct rnd_time0),
                count(distinct rnd_time6),
                count(distinct rnd_time9),
                count(distinct rnd_timetz),
                count(distinct rnd_timetz0),
                count(distinct rnd_timetz6),
                count(distinct rnd_timetz9),
                count(distinct rnd_varbinary),
                count(distinct rnd_varchar),
                count(distinct rnd_nvarchar),
                count(distinct rnd_ipaddress),
                count(distinct rnd_uuid)
                FROM all_types_values
                """;
        assertQuery(testQuery,
                """
                VALUES (2,
                2,
                2,
                2,
                -- date
                2,
                -- decimal
                2,
                2,
                2,
                2,
                2,
                -- real, double
                2,
                2,
                -- intervals
                2,
                2,
                -- timestamps
                2,
                2,
                2,
                2,
                -- timestamps with time zone
                2,
                2,
                2,
                2,
                -- time
                2,
                2,
                2,
                2,
                -- time with time zone
                2,
                2,
                2,
                2,
                -- character types
                2,
                2,
                2,
                -- ip, uuid
                2,
                2)
                """);

        assertUpdate("DROP TABLE faker.default.all_types_values");
    }
}
