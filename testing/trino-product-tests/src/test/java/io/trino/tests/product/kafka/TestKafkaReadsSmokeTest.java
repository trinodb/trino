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

import java.nio.charset.StandardCharsets;
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

import static java.lang.Double.doubleToRawLongBits;
import static java.lang.Float.floatToIntBits;
import static java.nio.ByteBuffer.allocate;
import static java.nio.ByteOrder.BIG_ENDIAN;
import static org.assertj.core.api.Assertions.assertThat;

@ProductTest
@RequiresEnvironment(KafkaBasicEnvironment.class)
@TestGroup.Kafka
@TestGroup.ProfileSpecificTests
class TestKafkaReadsSmokeTest
{
    private static final String KAFKA_CATALOG = "kafka";
    private static final String SCHEMA_NAME = "product_tests";

    @Test
    void testSelectSimpleKeyAndValue(KafkaEnvironment env)
            throws Exception
    {
        String topicName = "read_simple_key_and_value";
        env.createTopic(topicName);

        // Send CSV-formatted messages: key is "varchar_key,bigint_key", value is "varchar_value,bigint_value"
        env.sendMessages(topicName,
                "jasio,1".getBytes(StandardCharsets.UTF_8),
                "ania,2".getBytes(StandardCharsets.UTF_8));
        env.sendMessages(topicName,
                "piotr,3".getBytes(StandardCharsets.UTF_8),
                "kasia,4".getBytes(StandardCharsets.UTF_8));

        Thread.sleep(1000);

        try (Connection conn = env.createTrinoConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT varchar_key, bigint_key, varchar_value, bigint_value " +
                                "FROM " + KAFKA_CATALOG + "." + SCHEMA_NAME + "." + topicName)) {
            List<List<Object>> rows = collectRows(rs);
            assertThat(rows).containsExactlyInAnyOrder(
                    List.of("jasio", 1L, "ania", 2L),
                    List.of("piotr", 3L, "kasia", 4L));
        }
    }

    @Test
    void testSelectAllRawTable(KafkaEnvironment env)
            throws Exception
    {
        String topicName = "read_all_datatypes_raw";
        env.createTopic(topicName);

        // Raw format requires binary encoding according to the table definition
        byte[] rawMessage = allocate(58)
                .order(BIG_ENDIAN)
                .put("jasio".getBytes(StandardCharsets.UTF_8))  // c_varchar (0:5)
                .put((byte) 0x01)  // c_byte_bigint (5)
                .putShort((short) 0x0203)  // c_short_bigint (6)
                .putInt(0x04050607)  // c_int_bigint (8)
                .putLong(0x08090a0b0c0d0e0fL)  // c_long_bigint (12)
                .put((byte) 0x10)  // c_byte_integer (20)
                .putShort((short) 0x1112)  // c_short_integer (21)
                .putInt(0x13141516)  // c_int_integer (23)
                .put((byte) 0x17)  // c_byte_smallint (27)
                .putShort((short) 0x1819)  // c_short_smallint (28)
                .put((byte) 0x1a)  // c_byte_tinyint (30)
                .putInt(floatToIntBits(0.13f))  // c_float_double (31)
                .putLong(doubleToRawLongBits(0.45))  // c_double_double (35)
                .put((byte) 0x1b)  // c_byte_boolean (43)
                .putShort((short) 0x1c1d)  // c_short_boolean (44)
                .putInt(0x1e1f2021)  // c_int_boolean (46)
                .putLong(0x2223242526272829L)  // c_long_boolean (50)
                .array();

        env.sendMessages(topicName, null, rawMessage);

        Thread.sleep(1000);

        try (Connection conn = env.createTrinoConnection();
                Statement stmt = conn.createStatement()) {
            // Verify column schema
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT column_name, data_type FROM " + KAFKA_CATALOG + ".information_schema.columns " +
                            "WHERE table_schema = '" + SCHEMA_NAME + "' AND table_name = '" + topicName + "'")) {
                List<List<Object>> columns = collectRows(rs);
                assertThat(columns).containsExactlyInAnyOrder(
                        List.of("c_varchar", "varchar"),
                        List.of("c_byte_bigint", "bigint"),
                        List.of("c_short_bigint", "bigint"),
                        List.of("c_int_bigint", "bigint"),
                        List.of("c_long_bigint", "bigint"),
                        List.of("c_byte_integer", "integer"),
                        List.of("c_short_integer", "integer"),
                        List.of("c_int_integer", "integer"),
                        List.of("c_byte_smallint", "smallint"),
                        List.of("c_short_smallint", "smallint"),
                        List.of("c_byte_tinyint", "tinyint"),
                        List.of("c_float_double", "double"),
                        List.of("c_double_double", "double"),
                        List.of("c_byte_boolean", "boolean"),
                        List.of("c_short_boolean", "boolean"),
                        List.of("c_int_boolean", "boolean"),
                        List.of("c_long_boolean", "boolean"));
            }

            // Verify data
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + KAFKA_CATALOG + "." + SCHEMA_NAME + "." + topicName)) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("c_varchar")).isEqualTo("jasio");
                assertThat(rs.getLong("c_byte_bigint")).isEqualTo(0x01);
                assertThat(rs.getLong("c_short_bigint")).isEqualTo(0x0203);
                assertThat(rs.getLong("c_int_bigint")).isEqualTo(0x04050607);
                assertThat(rs.getLong("c_long_bigint")).isEqualTo(0x08090a0b0c0d0e0fL);
                assertThat(rs.getInt("c_byte_integer")).isEqualTo(0x10);
                assertThat(rs.getInt("c_short_integer")).isEqualTo(0x1112);
                assertThat(rs.getInt("c_int_integer")).isEqualTo(0x13141516);
                assertThat(rs.getShort("c_byte_smallint")).isEqualTo((short) 0x17);
                assertThat(rs.getShort("c_short_smallint")).isEqualTo((short) 0x1819);
                assertThat(rs.getByte("c_byte_tinyint")).isEqualTo((byte) 0x1a);
                assertThat(rs.getFloat("c_float_double")).isEqualTo(0.13f);
                assertThat(rs.getDouble("c_double_double")).isEqualTo(0.45);
                assertThat(rs.getBoolean("c_byte_boolean")).isTrue();
                assertThat(rs.getBoolean("c_short_boolean")).isTrue();
                assertThat(rs.getBoolean("c_int_boolean")).isTrue();
                assertThat(rs.getBoolean("c_long_boolean")).isTrue();
                assertThat(rs.next()).isFalse();
            }
        }
    }

    @Test
    void testSelectAllCsvTable(KafkaEnvironment env)
            throws Exception
    {
        String topicName = "read_all_datatypes_csv";
        env.createTopic(topicName);

        // CSV format: c_varchar,c_bigint,c_integer,c_smallint,c_tinyint,c_double,c_boolean
        env.sendMessages(topicName, null,
                "jasio,9223372036854775807,2147483647,32767,127,1234567890.123456789,true".getBytes(StandardCharsets.UTF_8));
        env.sendMessages(topicName, null,
                "stasio,-9223372036854775808,-2147483648,-32768,-128,-1234567890.123456789,blah".getBytes(StandardCharsets.UTF_8));
        env.sendMessages(topicName, null,
                ",,,,,,".getBytes(StandardCharsets.UTF_8));
        env.sendMessages(topicName, null,
                "krzysio,9223372036854775807,2147483647,32767,127,1234567890.123456789,false,extra,fields".getBytes(StandardCharsets.UTF_8));
        env.sendMessages(topicName, null,
                "kasia,9223372036854775807,2147483647,32767".getBytes(StandardCharsets.UTF_8));

        Thread.sleep(1000);

        try (Connection conn = env.createTrinoConnection();
                Statement stmt = conn.createStatement()) {
            // Verify column schema
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT column_name, data_type FROM " + KAFKA_CATALOG + ".information_schema.columns " +
                            "WHERE table_schema = '" + SCHEMA_NAME + "' AND table_name = '" + topicName + "'")) {
                List<List<Object>> columns = collectRows(rs);
                assertThat(columns).containsExactlyInAnyOrder(
                        List.of("c_varchar", "varchar"),
                        List.of("c_bigint", "bigint"),
                        List.of("c_integer", "integer"),
                        List.of("c_smallint", "smallint"),
                        List.of("c_tinyint", "tinyint"),
                        List.of("c_double", "double"),
                        List.of("c_boolean", "boolean"));
            }

            // Verify data
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + KAFKA_CATALOG + "." + SCHEMA_NAME + "." + topicName)) {
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
    void testSelectAllJsonTable(KafkaEnvironment env)
            throws Exception
    {
        String topicName = "read_all_datatypes_json";
        env.createTopic(topicName);

        String jsonMessage = """
                {
                  "j_varchar": "ala ma kota",
                  "j_bigint": "9223372036854775807",
                  "j_integer": "2147483647",
                  "j_smallint": "32767",
                  "j_tinyint": "127",
                  "j_double": "1234567890.123456789",
                  "j_boolean": "true",
                  "j_timestamp_milliseconds_since_epoch": "1518182116000",
                  "j_timestamp_seconds_since_epoch": "1518182117",
                  "j_timestamp_iso8601": "2018-02-09T13:15:18",
                  "j_timestamp_rfc2822": "Fri Feb 09 13:15:19 Z 2018",
                  "j_timestamp_custom": "02/2018/09 13:15:20",
                  "j_date_iso8601": "2018-02-11",
                  "j_date_custom": "2018/13/02",
                  "j_time_milliseconds_since_epoch": "47716000",
                  "j_time_seconds_since_epoch": "47717",
                  "j_time_iso8601": "13:15:18",
                  "j_time_custom": "15:13:20",
                  "j_timestamptz_milliseconds_since_epoch": "1518182116000",
                  "j_timestamptz_seconds_since_epoch": "1518182117",
                  "j_timestamptz_iso8601": "2018-02-09T13:15:18Z",
                  "j_timestamptz_rfc2822": "Fri Feb 09 13:15:19 Z 2018",
                  "j_timestamptz_custom": "02/2018/09 13:15:20",
                  "j_timetz_milliseconds_since_epoch": "47716000",
                  "j_timetz_seconds_since_epoch": "47717",
                  "j_timetz_iso8601": "13:15:18Z",
                  "j_timetz_custom": "15:13:20"
                }
                """;

        env.sendMessages(topicName, null, jsonMessage.getBytes(StandardCharsets.UTF_8));

        Thread.sleep(1000);

        try (Connection conn = env.createTrinoConnection();
                Statement stmt = conn.createStatement()) {
            // Verify column schema
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT column_name, data_type FROM " + KAFKA_CATALOG + ".information_schema.columns " +
                            "WHERE table_schema = '" + SCHEMA_NAME + "' AND table_name = '" + topicName + "'")) {
                List<List<Object>> columns = collectRows(rs);
                assertThat(columns).containsExactlyInAnyOrder(
                        List.of("c_varchar", "varchar"),
                        List.of("c_bigint", "bigint"),
                        List.of("c_integer", "integer"),
                        List.of("c_smallint", "smallint"),
                        List.of("c_tinyint", "tinyint"),
                        List.of("c_double", "double"),
                        List.of("c_boolean", "boolean"),
                        List.of("c_timestamp_milliseconds_since_epoch", "timestamp(3)"),
                        List.of("c_timestamp_seconds_since_epoch", "timestamp(3)"),
                        List.of("c_timestamp_iso8601", "timestamp(3)"),
                        List.of("c_timestamp_rfc2822", "timestamp(3)"),
                        List.of("c_timestamp_custom", "timestamp(3)"),
                        List.of("c_date_iso8601", "date"),
                        List.of("c_date_custom", "date"),
                        List.of("c_time_milliseconds_since_epoch", "time(3)"),
                        List.of("c_time_seconds_since_epoch", "time(3)"),
                        List.of("c_time_iso8601", "time(3)"),
                        List.of("c_time_custom", "time(3)"),
                        List.of("c_timestamptz_milliseconds_since_epoch", "timestamp(3) with time zone"),
                        List.of("c_timestamptz_seconds_since_epoch", "timestamp(3) with time zone"),
                        List.of("c_timestamptz_iso8601", "timestamp(3) with time zone"),
                        List.of("c_timestamptz_rfc2822", "timestamp(3) with time zone"),
                        List.of("c_timestamptz_custom", "timestamp(3) with time zone"),
                        List.of("c_timetz_milliseconds_since_epoch", "time(3) with time zone"),
                        List.of("c_timetz_seconds_since_epoch", "time(3) with time zone"),
                        List.of("c_timetz_iso8601", "time(3) with time zone"),
                        List.of("c_timetz_custom", "time(3) with time zone"));
            }

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + KAFKA_CATALOG + "." + SCHEMA_NAME + "." + topicName)) {
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
                assertThat(rs.getTimestamp("c_timestamptz_milliseconds_since_epoch")).isEqualTo(Timestamp.valueOf(LocalDateTime.of(2018, 2, 9, 19, 0, 16)));
                assertThat(rs.getTimestamp("c_timestamptz_seconds_since_epoch")).isEqualTo(Timestamp.valueOf(LocalDateTime.of(2018, 2, 9, 19, 0, 17)));
                assertThat(rs.getTimestamp("c_timestamptz_iso8601")).isEqualTo(Timestamp.valueOf(LocalDateTime.of(2018, 2, 9, 19, 0, 18)));
                assertThat(rs.getTimestamp("c_timestamptz_rfc2822")).isEqualTo(Timestamp.valueOf(LocalDateTime.of(2018, 2, 9, 19, 0, 19)));
                assertThat(rs.getTimestamp("c_timestamptz_custom")).isEqualTo(Timestamp.valueOf(LocalDateTime.of(2018, 2, 9, 19, 0, 20)));
                assertThat(rs.getTime("c_timetz_milliseconds_since_epoch")).isEqualTo(Time.valueOf(LocalTime.of(18, 45, 16)));
                assertThat(rs.getTime("c_timetz_seconds_since_epoch")).isEqualTo(Time.valueOf(LocalTime.of(18, 45, 17)));
                assertThat(rs.getTime("c_timetz_iso8601")).isEqualTo(Time.valueOf(LocalTime.of(18, 45, 18)));
                assertThat(rs.getTime("c_timetz_custom")).isEqualTo(Time.valueOf(LocalTime.of(18, 45, 20)));
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
