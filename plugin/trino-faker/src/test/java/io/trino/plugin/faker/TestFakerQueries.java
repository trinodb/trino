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
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

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
        try (TestTable table = new TestTable(getQueryRunner()::execute, "generators", """
            (
                name VARCHAR NOT NULL WITH (generator = '#{Name.first_name} #{Name.last_name}'),
                age_years INTEGER NOT NULL
            )
            """)) {
            assertQuery("SELECT count(name) FILTER (WHERE LENGTH(name) - LENGTH(REPLACE(name, ' ', '')) = 1) FROM " + table.getName(), "VALUES (1000)");
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
    void testSelectRange()
    {
        @Language("SQL")
        String tableQuery =
                """
                CREATE TABLE faker.default.all_types_range (
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
                count(distinct rnd_timetz9)
                FROM all_types_range
                WHERE 1=1
                AND rnd_bigint BETWEEN 0 AND 1
                AND rnd_integer BETWEEN 0 AND 1
                AND rnd_smallint BETWEEN 0 AND 1
                AND rnd_tinyint BETWEEN 0 AND 1
                AND rnd_date BETWEEN DATE '2022-03-01' AND DATE '2022-03-02'
                AND rnd_decimal1 BETWEEN 0 AND 1
                AND rnd_decimal2 BETWEEN 0.00000 AND 0.00001
                AND rnd_decimal3 BETWEEN 0 AND 1
                AND rnd_decimal4 BETWEEN DECIMAL '0.00000000000000000000000000000000000000' AND  DECIMAL '0.00000000000000000000000000000000000001'
                AND rnd_decimal5 BETWEEN 0.00 AND 0.01
                AND rnd_real BETWEEN REAL '0.0' AND REAL '1.4E-45'
                AND rnd_double BETWEEN DOUBLE '0.0' AND DOUBLE '4.9E-324'
                AND rnd_interval_day_time BETWEEN INTERVAL '0.000' SECOND AND INTERVAL '0.001' SECOND
                AND rnd_interval_year BETWEEN INTERVAL '0' MONTH AND INTERVAL '1' MONTH
                AND rnd_timestamp BETWEEN TIMESTAMP '2022-03-21 00:00:00.000' AND  TIMESTAMP '2022-03-21 00:00:00.001'
                AND rnd_timestamp0 BETWEEN TIMESTAMP '2022-03-21 00:00:00' AND  TIMESTAMP '2022-03-21 00:00:01'
                AND rnd_timestamp6 BETWEEN TIMESTAMP '2022-03-21 00:00:00.000000' AND  TIMESTAMP '2022-03-21 00:00:00.000001'
                AND rnd_timestamp9 BETWEEN TIMESTAMP '2022-03-21 00:00:00.000000000' AND  TIMESTAMP '2022-03-21 00:00:00.000000001'
                AND rnd_timestamptz BETWEEN TIMESTAMP '2022-03-21 00:00:00.000 +01:00' AND  TIMESTAMP '2022-03-21 00:00:00.001 +01:00'
                AND rnd_timestamptz0 BETWEEN TIMESTAMP '2022-03-21 00:00:00 +01:00' AND  TIMESTAMP '2022-03-21 00:00:01 +01:00'
                AND rnd_timestamptz6 BETWEEN TIMESTAMP '2022-03-21 00:00:00.000000 +01:00' AND  TIMESTAMP '2022-03-21 00:00:00.000001 +01:00'
                AND rnd_timestamptz9 BETWEEN TIMESTAMP '2022-03-21 00:00:00.000000000 +01:00' AND  TIMESTAMP '2022-03-21 00:00:00.000000001 +01:00'
                AND rnd_time BETWEEN TIME '01:02:03.456' AND  TIME '01:02:03.457'
                AND rnd_time0 BETWEEN TIME '01:02:03' AND  TIME '01:02:04'
                AND rnd_time6 BETWEEN TIME '01:02:03.000456' AND  TIME '01:02:03.000457'
                AND rnd_time9 BETWEEN TIME '01:02:03.000000456' AND  TIME '01:02:03.000000457'
                AND rnd_timetz BETWEEN TIME '01:02:03.456 +01:00' AND  TIME '01:02:03.457 +01:00'
                AND rnd_timetz0 BETWEEN TIME '01:02:03 +01:00' AND  TIME '01:02:04 +01:00'
                AND rnd_timetz6 BETWEEN TIME '01:02:03.000456 +01:00' AND  TIME '01:02:03.000457 +01:00'
                AND rnd_timetz9 BETWEEN TIME '01:02:03.000000456 +01:00' AND  TIME '01:02:03.000000457 +01:00'\s""";
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

        // exclusive ranges that produce only 1 value
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
                count(distinct rnd_timetz9)
                FROM all_types_range
                WHERE 1=1
                AND rnd_bigint > 0 AND rnd_bigint  < 2
                AND rnd_integer > 0 AND rnd_integer < 2
                AND rnd_smallint > 0 AND rnd_smallint < 2
                AND rnd_tinyint > 0 AND rnd_tinyint < 2
                AND rnd_date > DATE '2022-03-01' AND rnd_date < DATE '2022-03-03'
                AND rnd_decimal1 > 0 AND rnd_decimal1 < 2
                AND rnd_decimal2 > 0.00000 AND rnd_decimal2 < 0.00002
                AND rnd_decimal3 > 0 AND rnd_decimal3 < 2
                AND rnd_decimal4 > DECIMAL '0.00000000000000000000000000000000000000' AND rnd_decimal4 <  DECIMAL '0.00000000000000000000000000000000000002'
                AND rnd_decimal5 > 0.00 AND rnd_decimal5 < 0.02
                AND rnd_real > REAL '0.0' AND rnd_real < REAL '2.8E-45'
                AND rnd_double > DOUBLE '0.0' AND rnd_double < DOUBLE '1.0E-323'
                AND rnd_interval_day_time > INTERVAL '0.000' SECOND AND rnd_interval_day_time < INTERVAL '0.002' SECOND
                AND rnd_interval_year > INTERVAL '0' MONTH AND rnd_interval_year < INTERVAL '2' MONTH
                AND rnd_timestamp > TIMESTAMP '2022-03-21 00:00:00.000' AND rnd_timestamp < TIMESTAMP '2022-03-21 00:00:00.002'
                AND rnd_timestamp0 > TIMESTAMP '2022-03-21 00:00:00' AND rnd_timestamp0 < TIMESTAMP '2022-03-21 00:00:02'
                AND rnd_timestamp6 > TIMESTAMP '2022-03-21 00:00:00.000000' AND rnd_timestamp6 < TIMESTAMP '2022-03-21 00:00:00.000002'
                AND rnd_timestamp9 > TIMESTAMP '2022-03-21 00:00:00.000000000' AND rnd_timestamp9 < TIMESTAMP '2022-03-21 00:00:00.000000002'
                AND rnd_timestamptz > TIMESTAMP '2022-03-21 00:00:00.000 +01:00' AND rnd_timestamptz < TIMESTAMP '2022-03-21 00:00:00.002 +01:00'
                AND rnd_timestamptz0 > TIMESTAMP '2022-03-21 00:00:00 +01:00' AND rnd_timestamptz0 < TIMESTAMP '2022-03-21 00:00:02 +01:00'
                AND rnd_timestamptz6 > TIMESTAMP '2022-03-21 00:00:00.000000 +01:00' AND rnd_timestamptz6 < TIMESTAMP '2022-03-21 00:00:00.000002 +01:00'
                AND rnd_timestamptz9 > TIMESTAMP '2022-03-21 00:00:00.000000000 +01:00' AND rnd_timestamptz9 < TIMESTAMP '2022-03-21 00:00:00.000000002 +01:00'
                AND rnd_time > TIME '01:02:03.456' AND rnd_time < TIME '01:02:03.458'
                AND rnd_time0 > TIME '01:02:03' AND rnd_time0 < TIME '01:02:05'
                AND rnd_time6 > TIME '01:02:03.000456' AND rnd_time6 < TIME '01:02:03.000458'
                AND rnd_time9 > TIME '01:02:03.000000456' AND rnd_time9 < TIME '01:02:03.000000458'
                AND rnd_timetz > TIME '01:02:03.456 +01:00' AND rnd_timetz < TIME '01:02:03.458 +01:00'
                AND rnd_timetz0 > TIME '01:02:03 +01:00' AND rnd_timetz0 < TIME '01:02:05 +01:00'
                AND rnd_timetz6 > TIME '01:02:03.000456 +01:00' AND rnd_timetz6 < TIME '01:02:03.000458 +01:00'
                AND rnd_timetz9 > TIME '01:02:03.000000456 +01:00' AND rnd_timetz9 < TIME '01:02:03.000000458 +01:00'\s""";
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
                -- intervals
                1,
                1,
                -- timestamps
                1,
                1,
                1,
                1,
                -- timestamps with time zone
                1,
                1,
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

        // inclusive range to get the min low bound
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
                --count(distinct rnd_interval_day_time),
                --count(distinct rnd_interval_year),
                -- can't count timestamps because their extreme values cannot be expressed as literals
                count(distinct rnd_time),
                count(distinct rnd_time0),
                count(distinct rnd_time6),
                count(distinct rnd_time9),
                count(distinct rnd_timetz),
                count(distinct rnd_timetz0),
                count(distinct rnd_timetz6),
                count(distinct rnd_timetz9)
                FROM all_types_range
                WHERE 1=1
                AND rnd_bigint <= -9223372036854775808
                AND rnd_integer <= -2147483648
                AND rnd_smallint <= -32768
                AND rnd_tinyint <= -128
                -- TODO it actually returns -5877641-06-23 - there's definitely some overflow happening in the engine
                AND rnd_date <= DATE '-5877641-06-23'
                AND rnd_decimal1 <= DECIMAL '-99999999999999999999999999999999999999'
                AND rnd_decimal2 <= DECIMAL '-9999999999999.99999'
                AND rnd_decimal3 <= DECIMAL '-99999999999999999999999999999999999999'
                AND rnd_decimal4 <= DECIMAL '-0.99999999999999999999999999999999999999'
                -- TODO it actually retdurns '-999.98'
                AND rnd_decimal5 <= DECIMAL '-999.99'
                AND rnd_real <= REAL '1.4E-45'
                AND rnd_double <= DOUBLE '4.9E-324'
                -- interval literals can't represent smallest possible values allowed by the engine
                --AND rnd_interval_day_time <= INTERVAL '-2147483647' SECOND
                --AND rnd_interval_year <= INTERVAL '-2147483647' MONTH
                AND rnd_time <= TIME '00:00:00.000'
                AND rnd_time0 <= TIME '00:00:00'
                AND rnd_time6 <= TIME '00:00:00.000000'
                AND rnd_time9 <= TIME '00:00:00.000000000'
                AND rnd_timetz <= TIME '00:00:00.000 +01:00'
                AND rnd_timetz0 <= TIME '00:00:00 +01:00'
                AND rnd_timetz6 <= TIME '00:00:00.000000 +01:00'
                AND rnd_timetz9 <= TIME '00:00:00.000000000 +01:00'
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
                -- intervals
                --1,
                --1,
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

        assertUpdate("DROP TABLE faker.default.all_types_range");
    }

    @Test
    void testSelectIn()
    {
        @Language("SQL")
        String tableQuery =
                """
                CREATE TABLE faker.default.all_types_in (
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
                rnd_ipaddress ipaddress NOT NULL,
                rnd_uuid uuid NOT NULL)""";
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
                FROM all_types_in
                WHERE 1=1
                AND rnd_bigint IN (0, 1)
                AND rnd_integer IN (0, 1)
                AND rnd_smallint IN (0, 1)
                AND rnd_tinyint IN (0, 1)
                AND rnd_date IN (DATE '2022-03-01', DATE '2022-03-02')
                AND rnd_decimal1 IN (0, 1)
                AND rnd_decimal2 IN (0.00000, 0.00001)
                AND rnd_decimal3 IN (0, 1)
                AND rnd_decimal4 IN (DECIMAL '0.00000000000000000000000000000000000000',  DECIMAL '0.00000000000000000000000000000000000001')
                AND rnd_decimal5 IN (0.00, 0.01)
                AND rnd_real IN (REAL '0.0', REAL '1.4E-45')
                AND rnd_double IN (DOUBLE '0.0', DOUBLE '4.9E-324')
                AND rnd_interval_day_time IN (INTERVAL '0.000' SECOND, INTERVAL '0.001' SECOND)
                AND rnd_interval_year IN (INTERVAL '0' MONTH, INTERVAL '1' MONTH)
                AND rnd_timestamp IN (TIMESTAMP '2022-03-21 00:00:00.000',  TIMESTAMP '2022-03-21 00:00:00.001')
                AND rnd_timestamp0 IN (TIMESTAMP '2022-03-21 00:00:00',  TIMESTAMP '2022-03-21 00:00:01')
                AND rnd_timestamp6 IN (TIMESTAMP '2022-03-21 00:00:00.000000',  TIMESTAMP '2022-03-21 00:00:00.000001')
                AND rnd_timestamp9 IN (TIMESTAMP '2022-03-21 00:00:00.000000000',  TIMESTAMP '2022-03-21 00:00:00.000000001')
                AND rnd_timestamptz IN (TIMESTAMP '2022-03-21 00:00:00.000 +01:00',  TIMESTAMP '2022-03-21 00:00:00.001 +01:00')
                AND rnd_timestamptz0 IN (TIMESTAMP '2022-03-21 00:00:00 +01:00',  TIMESTAMP '2022-03-21 00:00:01 +01:00')
                AND rnd_timestamptz6 IN (TIMESTAMP '2022-03-21 00:00:00.000000 +01:00',  TIMESTAMP '2022-03-21 00:00:00.000001 +01:00')
                AND rnd_timestamptz9 IN (TIMESTAMP '2022-03-21 00:00:00.000000000 +01:00',  TIMESTAMP '2022-03-21 00:00:00.000000001 +01:00')
                AND rnd_time IN (TIME '01:02:03.456',  TIME '01:02:03.457')
                AND rnd_time0 IN (TIME '01:02:03',  TIME '01:02:04')
                AND rnd_time6 IN (TIME '01:02:03.000456',  TIME '01:02:03.000457')
                AND rnd_time9 IN (TIME '01:02:03.000000456',  TIME '01:02:03.000000457')
                AND rnd_timetz IN (TIME '01:02:03.456 +01:00',  TIME '01:02:03.457 +01:00')
                AND rnd_timetz0 IN (TIME '01:02:03 +01:00',  TIME '01:02:04 +01:00')
                AND rnd_timetz6 IN (TIME '01:02:03.000456 +01:00',  TIME '01:02:03.000457 +01:00')
                AND rnd_timetz9 IN (TIME '01:02:03.000000456 +01:00',  TIME '01:02:03.000000457 +01:00')
                AND rnd_varbinary IN (x'ff', x'00')
                AND rnd_varchar IN ('aa', 'bb')
                AND rnd_nvarchar IN ('aa', 'bb')
                AND rnd_ipaddress IN (IPADDRESS '0.0.0.0', IPADDRESS '1.2.3.4')
                AND rnd_uuid IN (UUID '1fc74d96-0216-449b-a145-455578a9eaa5', UUID '3ee49ede-0026-45e4-ba06-08404f794557')
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

        assertUpdate("DROP TABLE faker.default.all_types_in");
    }
}
