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

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ProductTest
@RequiresEnvironment(KafkaBasicEnvironment.class)
@TestGroup.Kafka
@TestGroup.ProfileSpecificTests
class TestKafkaWritesSmokeTest
{
    private static final String KAFKA_CATALOG = "kafka";
    private static final String SCHEMA_NAME = "product_tests";

    @Test
    void testInsertSimpleKeyAndValue(KafkaEnvironment env)
            throws Exception
    {
        String topicName = "write_simple_key_and_value";
        env.createTopic(topicName);

        try (Connection conn = env.createTrinoConnection();
                Statement stmt = conn.createStatement()) {
            int inserted = stmt.executeUpdate(
                    "INSERT INTO " + KAFKA_CATALOG + "." + SCHEMA_NAME + "." + topicName + " VALUES " +
                            "('jasio', 1, 'ania', 2), " +
                            "('piotr', 3, 'kasia', 4)");
            assertThat(inserted).isEqualTo(2);

            Thread.sleep(1000);

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT * FROM " + KAFKA_CATALOG + "." + SCHEMA_NAME + "." + topicName)) {
                List<List<Object>> rows = collectRows(rs);
                assertThat(rows).containsExactlyInAnyOrder(
                        List.of("jasio", 1L, "ania", 2L),
                        List.of("piotr", 3L, "kasia", 4L));
            }
        }
    }

    @Test
    void testInsertRawTable(KafkaEnvironment env)
            throws Exception
    {
        String topicName = "write_all_datatypes_raw";
        env.createTopic(topicName);

        try (Connection conn = env.createTrinoConnection();
                Statement stmt = conn.createStatement()) {
            // Note: The original test has a comment about RawRowEncoder buffer issues
            // Column order per JSON schema: c_varchar, c_long_bigint, c_int_integer, c_short_smallint, c_byte_tinyint, c_double_double, c_byte_boolean
            int inserted = stmt.executeUpdate(
                    "INSERT INTO " + KAFKA_CATALOG + "." + SCHEMA_NAME + "." + topicName + " VALUES " +
                            "('jasio', 9223372036854775807, 2147483647, 32767, 127, 1234567890.123456789, true), " +
                            "('piotr', -9223372036854775808, -2147483648, -32768, -128, -1234567890.123456789, false), " +
                            "('hasan', 9223372036854775807, 2147483647, 32767, 127, 1234567890.123456789, true), " +
                            "('kasia', -9223372036854775808, -2147483648, -32768, -128, -1234567890.123456789, false)");
            assertThat(inserted).isEqualTo(4);

            Thread.sleep(1000);

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT * FROM " + KAFKA_CATALOG + "." + SCHEMA_NAME + "." + topicName)) {
                List<List<Object>> rows = collectRows(rs);
                assertThat(rows).containsExactlyInAnyOrder(
                        List.of("jasio", 9223372036854775807L, 2147483647, (short) 32767, (byte) 127, 1234567890.123456789, true),
                        List.of("piotr", -9223372036854775808L, -2147483648, (short) -32768, (byte) -128, -1234567890.123456789, false),
                        List.of("hasan", 9223372036854775807L, 2147483647, (short) 32767, (byte) 127, 1234567890.123456789, true),
                        List.of("kasia", -9223372036854775808L, -2147483648, (short) -32768, (byte) -128, -1234567890.123456789, false));
            }
        }
    }

    @Test
    void testInsertCsvTable(KafkaEnvironment env)
            throws Exception
    {
        String topicName = "write_all_datatypes_csv";
        env.createTopic(topicName);

        try (Connection conn = env.createTrinoConnection();
                Statement stmt = conn.createStatement()) {
            int inserted = stmt.executeUpdate(
                    "INSERT INTO " + KAFKA_CATALOG + "." + SCHEMA_NAME + "." + topicName + " VALUES " +
                            "('jasio', 9223372036854775807, 2147483647, 32767, 127, 1234567890.123456789, true), " +
                            "('stasio', -9223372036854775808, -2147483648, -32768, -128, -1234567890.123456789, false), " +
                            "(null, null, null, null, null, null, null), " +
                            "('krzysio', 9223372036854775807, 2147483647, 32767, 127, 1234567890.123456789, false), " +
                            "('kasia', 9223372036854775807, 2147483647, 32767, null, null, null)");
            assertThat(inserted).isEqualTo(5);

            Thread.sleep(1000);

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT * FROM " + KAFKA_CATALOG + "." + SCHEMA_NAME + "." + topicName)) {
                List<List<Object>> rows = collectRowsNullable(rs);
                assertThat(rows).containsExactlyInAnyOrder(
                        List.of("jasio", 9223372036854775807L, 2147483647, (short) 32767, (byte) 127, 1234567890.123456789, true),
                        List.of("stasio", -9223372036854775808L, -2147483648, (short) -32768, (byte) -128, -1234567890.123456789, false),
                        nullList(7),
                        List.of("krzysio", 9223372036854775807L, 2147483647, (short) 32767, (byte) 127, 1234567890.123456789, false),
                        listWithNulls("kasia", 9223372036854775807L, 2147483647, (short) 32767, null, null, null));
            }
        }
    }

    @Test
    void testInsertJsonTable(KafkaEnvironment env)
            throws Exception
    {
        String topicName = "write_all_datatypes_json";
        env.createTopic(topicName);

        try (Connection conn = env.createTrinoConnection();
                Statement stmt = conn.createStatement()) {
            int inserted = stmt.executeUpdate(
                    "INSERT INTO " + KAFKA_CATALOG + "." + SCHEMA_NAME + "." + topicName + " VALUES (" +
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
                            "TIME '02:15:20 -11:00')");
            assertThat(inserted).isEqualTo(1);

            Thread.sleep(1000);

            try (ResultSet rs = stmt.executeQuery(
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
                            // Comparing string representation is more resilient against JVM timezone differences
                            "CAST(c_timestamptz_iso8601 AS VARCHAR), " +
                            "CAST(c_timestamptz_rfc2822 AS VARCHAR), " +
                            "CAST(c_timestamptz_custom AS VARCHAR), " +
                            "CAST(c_timestamptz_custom_with_zone AS VARCHAR), " +
                            "CAST(c_timetz_iso8601 AS VARCHAR), " +
                            "CAST(c_timetz_custom AS VARCHAR) " +
                            "FROM " + KAFKA_CATALOG + "." + SCHEMA_NAME + "." + topicName)) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("c_varchar")).isEqualTo("ala ma kota");
                assertThat(rs.getLong("c_bigint")).isEqualTo(9223372036854775807L);
                assertThat(rs.getInt("c_integer")).isEqualTo(2147483647);
                assertThat(rs.getShort("c_smallint")).isEqualTo((short) 32767);
                assertThat(rs.getByte("c_tinyint")).isEqualTo((byte) 127);
                assertThat(rs.getDouble("c_double")).isEqualTo(1234567890.123456789);
                assertThat(rs.getBoolean("c_boolean")).isTrue();
                assertThat(rs.getTimestamp("c_timestamp_milliseconds_since_epoch")).isEqualTo(Timestamp.valueOf(LocalDateTime.of(2018, 2, 9, 13, 15, 16)));
                assertThat(rs.getTimestamp("c_timestamp_seconds_since_epoch")).isEqualTo(Timestamp.valueOf(LocalDateTime.of(2018, 2, 9, 13, 15, 17)));
                assertThat(rs.getTimestamp("c_timestamp_iso8601")).isEqualTo(Timestamp.valueOf(LocalDateTime.of(2018, 2, 9, 13, 15, 18)));
                assertThat(rs.getTimestamp("c_timestamp_rfc2822")).isEqualTo(Timestamp.valueOf(LocalDateTime.of(2018, 2, 9, 13, 15, 19)));
                assertThat(rs.getTimestamp("c_timestamp_custom")).isEqualTo(Timestamp.valueOf(LocalDateTime.of(2018, 2, 9, 13, 15, 20)));
                assertThat(rs.getDate("c_date_iso8601")).isEqualTo(Date.valueOf(LocalDate.of(2018, 2, 11)));
                assertThat(rs.getDate("c_date_custom")).isEqualTo(Date.valueOf(LocalDate.of(2018, 2, 13)));
                assertThat(rs.getTime("c_time_milliseconds_since_epoch")).isEqualTo(Time.valueOf(LocalTime.of(13, 15, 16)));
                assertThat(rs.getTime("c_time_seconds_since_epoch")).isEqualTo(Time.valueOf(LocalTime.of(13, 15, 17)));
                assertThat(rs.getTime("c_time_iso8601")).isEqualTo(Time.valueOf(LocalTime.of(13, 15, 18)));
                assertThat(rs.getTime("c_time_custom")).isEqualTo(Time.valueOf(LocalTime.of(13, 15, 20)));
                // ISO8601 and RFC2822 formatters write the TZ as offset
                assertThat(rs.getString(19)).isEqualTo("2018-02-09 13:15:18.000 +14:00");
                assertThat(rs.getString(20)).isEqualTo("2018-02-09 13:15:19.000 +14:00");
                assertThat(rs.getString(21)).isEqualTo("2018-02-09 13:15:20.000 Pacific/Apia");
                assertThat(rs.getString(22)).isEqualTo("2018-02-09 13:15:21.000 Pacific/Apia");
                assertThat(rs.getString(23)).isEqualTo("02:15:18.000-11:00");
                assertThat(rs.getString(24)).isEqualTo("02:15:20.000-11:00");
                assertThat(rs.next()).isFalse();
            }
        }
    }

    private static List<List<Object>> collectRows(ResultSet rs)
            throws Exception
    {
        List<List<Object>> rows = new ArrayList<>();
        int columnCount = rs.getMetaData().getColumnCount();
        while (rs.next()) {
            List<Object> row = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                row.add(rs.getObject(i));
            }
            rows.add(row);
        }
        return rows;
    }

    private static List<List<Object>> collectRowsNullable(ResultSet rs)
            throws Exception
    {
        List<List<Object>> rows = new ArrayList<>();
        int columnCount = rs.getMetaData().getColumnCount();
        while (rs.next()) {
            List<Object> row = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                Object value = rs.getObject(i);
                row.add(value);
            }
            rows.add(row);
        }
        return rows;
    }

    private static List<Object> nullList(int size)
    {
        List<Object> list = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            list.add(null);
        }
        return list;
    }

    private static List<Object> listWithNulls(Object... values)
    {
        List<Object> list = new ArrayList<>();
        for (Object value : values) {
            list.add(value);
        }
        return list;
    }
}
