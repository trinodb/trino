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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ProductTest
@RequiresEnvironment(KafkaBasicEnvironment.class)
@TestGroup.Kafka
@TestGroup.ProfileSpecificTests
class TestKafkaProtobufWritesSmokeTest
{
    private static final String KAFKA_CATALOG = "kafka";
    private static final String KAFKA_SCHEMA = "product_tests";
    private static final String ALL_DATATYPES_PROTOBUF_TABLE_NAME = "all_datatypes_protobuf";
    private static final String STRUCTURAL_PROTOBUF_TABLE_NAME = "structural_datatype_protobuf";

    @Test
    void testInsertAllDataType(KafkaEnvironment env)
            throws Exception
    {
        String topicName = "write_all_datatypes_protobuf";
        env.createTopic(topicName);

        try (Connection conn = env.createTrinoConnection();
                Statement stmt = conn.createStatement()) {
            int inserted = stmt.executeUpdate(
                    "INSERT INTO " + KAFKA_CATALOG + "." + KAFKA_SCHEMA + "." + ALL_DATATYPES_PROTOBUF_TABLE_NAME + " VALUES " +
                            "('Chennai', 314, 9223372036854775807, 1234567890.123456789, 3.14, true, 'ZERO', TIMESTAMP '2020-12-21 15:45:00.012345')," +
                            "('TamilNadu', -314, -9223372036854775808, -1234567890.123456789, -3.14, false, 'ONE', TIMESTAMP '1970-01-01 15:45:00.012345'), " +
                            "('India', 314, 9223372036854775807, 1234567890.123456789, 3.14, false, 'TWO', TIMESTAMP '0001-01-01 00:00:00.000001')");
            assertThat(inserted).isEqualTo(3);

            Thread.sleep(1000);

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT * FROM " + KAFKA_CATALOG + "." + KAFKA_SCHEMA + "." + ALL_DATATYPES_PROTOBUF_TABLE_NAME)) {
                List<List<Object>> rows = collectRows(rs);
                assertThat(rows).containsExactlyInAnyOrder(
                        List.of("Chennai", 314, 9223372036854775807L, 1234567890.123456789, 3.14f, true, "ZERO", Timestamp.valueOf("2020-12-21 15:45:00.012345")),
                        List.of("TamilNadu", -314, -9223372036854775808L, -1234567890.123456789, -3.14f, false, "ONE", Timestamp.valueOf("1970-01-01 15:45:00.012345")),
                        List.of("India", 314, 9223372036854775807L, 1234567890.123456789, 3.14f, false, "TWO", Timestamp.valueOf("0001-01-01 00:00:00.000001")));
            }

            // Test null value insertion error
            assertThatThrownBy(() -> stmt.executeUpdate(
                    "INSERT INTO " + KAFKA_CATALOG + "." + KAFKA_SCHEMA + "." + ALL_DATATYPES_PROTOBUF_TABLE_NAME + " (h_varchar) VALUES ('Chennai')"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("Protobuf doesn't support serializing null values");
        }
    }

    @Test
    void testInsertStructuralDataType(KafkaEnvironment env)
            throws Exception
    {
        String topicName = "structural_datatype_protobuf";
        env.createTopic(topicName);

        try (Connection conn = env.createTrinoConnection();
                Statement stmt = conn.createStatement()) {
            int inserted = stmt.executeUpdate(
                    "INSERT INTO " + KAFKA_CATALOG + "." + KAFKA_SCHEMA + "." + STRUCTURAL_PROTOBUF_TABLE_NAME + " VALUES " +
                            "(ARRAY[CAST(ROW('Entry1') AS ROW(simple_string VARCHAR))], " +
                            "map_from_entries(ARRAY[('key1', CAST(ROW('value1') AS ROW(simple_string VARCHAR)))]), " +
                            "CAST(ROW(1234567890.123456789, 3.14, 'ONE') AS ROW(d_double DOUBLE, e_float REAL, g_enum VARCHAR)), " +
                            "'Chennai', " +
                            "314, " +
                            "9223372036854775807, " +
                            "CAST(ROW('Entry2') AS ROW(simple_string VARCHAR)), " +
                            "TIMESTAMP '2020-12-21 15:45:00.012345')");
            assertThat(inserted).isEqualTo(1);

            Thread.sleep(1000);

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT c_array[1].simple_string, b_map['key1'].simple_string, a_row.d_double, a_row.e_float, a_row.g_enum, " +
                            "a_string, c_integer, c_bigint, d_row.simple_string, e_timestamp " +
                            "FROM " + KAFKA_CATALOG + "." + KAFKA_SCHEMA + "." + STRUCTURAL_PROTOBUF_TABLE_NAME)) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString(1)).isEqualTo("Entry1");
                assertThat(rs.getString(2)).isEqualTo("value1");
                assertThat(rs.getDouble(3)).isEqualTo(1234567890.123456789);
                assertThat(rs.getFloat(4)).isEqualTo(3.14f);
                assertThat(rs.getString(5)).isEqualTo("ONE");
                assertThat(rs.getString(6)).isEqualTo("Chennai");
                assertThat(rs.getInt(7)).isEqualTo(314);
                assertThat(rs.getLong(8)).isEqualTo(9223372036854775807L);
                assertThat(rs.getString(9)).isEqualTo("Entry2");
                assertThat(rs.getTimestamp(10)).isEqualTo(Timestamp.valueOf("2020-12-21 15:45:00.012345"));
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
}
