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
package io.trino.tests.product.kafka;

import io.trino.tempto.ProductTest;
import org.testng.annotations.Test;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tempto.query.QueryExecutor.query;
import static io.trino.tests.product.TestGroups.KAFKA;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static java.lang.String.format;

public class TestKafkaWritesSmokeTest
        extends ProductTest
{
    private static final String KAFKA_CATALOG = "kafka";
    private static final String SCHEMA_NAME = "product_tests";

    private static final String SIMPLE_KEY_AND_VALUE_TABLE_NAME = "write_simple_key_and_value";

    @Test(groups = {KAFKA, PROFILE_SPECIFIC_TESTS})
    public void testInsertSimpleKeyAndValue()
    {
        assertThat(query(format(
                "INSERT INTO %s.%s.%s VALUES " +
                        "('jasio', 1, 'ania', 2), " +
                        "('piotr', 3, 'kasia', 4)",
                KAFKA_CATALOG,
                SCHEMA_NAME,
                SIMPLE_KEY_AND_VALUE_TABLE_NAME)))
                .updatedRowsCountIsEqualTo(2);

        assertThat(query(format(
                "SELECT * FROM %s.%s.%s",
                KAFKA_CATALOG,
                SCHEMA_NAME,
                SIMPLE_KEY_AND_VALUE_TABLE_NAME)))
                .containsOnly(
                        row("jasio", 1, "ania", 2),
                        row("piotr", 3, "kasia", 4));
    }

    private static final String ALL_DATATYPES_RAW_TABLE_NAME = "write_all_datatypes_raw";

    @Test(groups = {KAFKA, PROFILE_SPECIFIC_TESTS})
    public void testInsertRawTable()
    {
        // TODO RawRowEncoder doesn't take mapping length into considertion while writing so a
        //  BIGINT with dataFormat = BYTE takes up 8 bytes during write (as opposed to 1 byte
        //  during read) and hence a buffer overflow is possible before we are able to reach
        //  the end of row.
        assertThat(query(format(
                "INSERT INTO %s.%s.%s VALUES " +
                        "('jasio', 9223372036854775807, 2147483647, 32767, 127, 1234567890.123456789, true), " +
                        "('piotr', -9223372036854775808, -2147483648, -32768, -128, -1234567890.123456789, false), " +
                        "('hasan', 9223372036854775807, 2147483647, 32767, 127, 1234567890.123456789, true), " +
                        "('kasia', -9223372036854775808, -2147483648, -32768, -128, -1234567890.123456789, false)",
                KAFKA_CATALOG,
                SCHEMA_NAME,
                ALL_DATATYPES_RAW_TABLE_NAME)))
                .updatedRowsCountIsEqualTo(4);

        assertThat(query(format(
                "SELECT * FROM %s.%s.%s",
                KAFKA_CATALOG,
                SCHEMA_NAME,
                ALL_DATATYPES_RAW_TABLE_NAME)))
                .containsOnly(
                        row("jasio", 9223372036854775807L, 2147483647, 32767, 127, 1234567890.123456789, true),
                        row("piotr", -9223372036854775808L, -2147483648, -32768, -128, -1234567890.123456789, false),
                        row("hasan", 9223372036854775807L, 2147483647, 32767, 127, 1234567890.123456789, true),
                        row("kasia", -9223372036854775808L, -2147483648, -32768, -128, -1234567890.123456789, false));
    }

    private static final String ALL_DATATYPES_CSV_TABLE_NAME = "write_all_datatypes_csv";

    @Test(groups = {KAFKA, PROFILE_SPECIFIC_TESTS})
    public void testInsertCsvTable()
    {
        assertThat(query(format(
                "INSERT INTO %s.%s.%s VALUES " +
                        "('jasio', 9223372036854775807, 2147483647, 32767, 127, 1234567890.123456789, true), " +
                        "('stasio', -9223372036854775808, -2147483648, -32768, -128, -1234567890.123456789, false), " +
                        "(null, null, null, null, null, null, null), " +
                        "('krzysio', 9223372036854775807, 2147483647, 32767, 127, 1234567890.123456789, false), " +
                        "('kasia', 9223372036854775807, 2147483647, 32767, null, null, null)",
                KAFKA_CATALOG,
                SCHEMA_NAME,
                ALL_DATATYPES_CSV_TABLE_NAME)))
                .updatedRowsCountIsEqualTo(5);

        assertThat(query(format(
                "SELECT * FROM %s.%s.%s",
                KAFKA_CATALOG,
                SCHEMA_NAME,
                ALL_DATATYPES_CSV_TABLE_NAME)))
                .containsOnly(
                        row("jasio", 9223372036854775807L, 2147483647, 32767, 127, 1234567890.123456789, true),
                        row("stasio", -9223372036854775808L, -2147483648, -32768, -128, -1234567890.123456789, false),
                        row(null, null, null, null, null, null, null),
                        row("krzysio", 9223372036854775807L, 2147483647, 32767, 127, 1234567890.123456789, false),
                        row("kasia", 9223372036854775807L, 2147483647, 32767, null, null, null));
    }

    private static final String ALL_DATATYPES_JSON_TABLE_NAME = "write_all_datatypes_json";

    @Test(groups = {KAFKA, PROFILE_SPECIFIC_TESTS})
    public void testInsertJsonTable()
    {
        assertThat(query(format(
                "INSERT INTO %s.%s.%s VALUES (" +
                        "'ala ma kota'," +
                        "9223372036854775807," +
                        "2147483647," +
                        "32767," +
                        "127," +
                        "1234567890.123456789," +
                        "true," +
                        "TIMESTAMP '2018-02-09 13:15:16'," +
                        "TIMESTAMP '2018-02-09 13:15:17'," +
                        "TIMESTAMP '2018-02-09 13:15:18'," +
                        "TIMESTAMP '2018-02-09 13:15:19'," +
                        "TIMESTAMP '2018-02-09 13:15:20'," +
                        "DATE '2018-02-11'," +
                        "DATE '2018-02-13'," +
                        "TIME '13:15:16'," +
                        "TIME '13:15:17'," +
                        "TIME '13:15:18'," +
                        "TIME '13:15:20'," +
                        "TIMESTAMP '2018-02-09 13:15:18 Pacific/Apia'," +
                        "TIMESTAMP '2018-02-09 13:15:19 Pacific/Apia'," +
                        "TIMESTAMP '2018-02-09 13:15:20 Pacific/Apia'," +
                        "TIMESTAMP '2018-02-09 13:15:21 Pacific/Apia'," +
                        // Pacific/Apia was UTC-11:00 on epoch day 0
                        "TIME '02:15:18 -11:00'," +
                        "TIME '02:15:20 -11:00')",
                KAFKA_CATALOG,
                SCHEMA_NAME,
                ALL_DATATYPES_JSON_TABLE_NAME)))
                .updatedRowsCountIsEqualTo(1);

        assertThat(query(format(
                "SELECT c_varchar, " +
                        "c_bigint, " +
                        "c_integer, " +
                        "c_smallint, " +
                        "c_tinyint, " +
                        "c_double, " +
                        "c_boolean, " +
                        "c_timestamp_milliseconds_since_epoch, " +
                        "c_timestamp_seconds_since_epoch, " +
                        "c_timestamp_iso8601, " +
                        "c_timestamp_rfc2822, " +
                        "c_timestamp_custom, " +
                        "c_date_iso8601, " +
                        "c_date_custom, " +
                        "c_time_milliseconds_since_epoch, " +
                        "c_time_seconds_since_epoch, " +
                        "c_time_iso8601, " +
                        "c_time_custom, " +
                        // TODO Until https://github.com/trinodb/trino/pull/307 is done, comparing string representation is more resilient against JVM timezone differences
                        "CAST(c_timestamptz_iso8601 AS VARCHAR), " +
                        "CAST(c_timestamptz_rfc2822 AS VARCHAR), " +
                        "CAST(c_timestamptz_custom AS VARCHAR), " +
                        "CAST(c_timestamptz_custom_with_zone AS VARCHAR), " +
                        "CAST(c_timetz_iso8601 AS VARCHAR), " +
                        "CAST(c_timetz_custom AS VARCHAR) " +
                        "FROM %s.%s.%s",
                KAFKA_CATALOG,
                SCHEMA_NAME,
                ALL_DATATYPES_JSON_TABLE_NAME)))
                .containsOnly(row(
                        "ala ma kota",
                        9223372036854775807L,
                        2147483647,
                        32767,
                        127,
                        1234567890.123456789,
                        true,
                        Timestamp.valueOf(LocalDateTime.of(2018, 2, 9, 13, 15, 16)),
                        Timestamp.valueOf(LocalDateTime.of(2018, 2, 9, 13, 15, 17)),
                        Timestamp.valueOf(LocalDateTime.of(2018, 2, 9, 13, 15, 18)),
                        Timestamp.valueOf(LocalDateTime.of(2018, 2, 9, 13, 15, 19)),
                        Timestamp.valueOf(LocalDateTime.of(2018, 2, 9, 13, 15, 20)),
                        Date.valueOf(LocalDate.of(2018, 2, 11)),
                        Date.valueOf(LocalDate.of(2018, 2, 13)),
                        Time.valueOf(LocalTime.of(13, 15, 16)),
                        Time.valueOf(LocalTime.of(13, 15, 17)),
                        Time.valueOf(LocalTime.of(13, 15, 18)),
                        Time.valueOf(LocalTime.of(13, 15, 20)),
                        // ISO8601 and RFC2822 formatters write the TZ as offset
                        "2018-02-09 13:15:18.000 +14:00",
                        "2018-02-09 13:15:19.000 +14:00",
                        "2018-02-09 13:15:20.000 Pacific/Apia",
                        "2018-02-09 13:15:21.000 Pacific/Apia",
                        "02:15:18.000-11:00",
                        "02:15:20.000-11:00"));
    }
}
