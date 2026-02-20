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
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ProductTest
@RequiresEnvironment(KafkaBasicEnvironment.class)
@TestGroup.Kafka
@TestGroup.ProfileSpecificTests
class TestKafkaAvroWritesSmokeTest
{
    private static final String KAFKA_CATALOG = "kafka";
    private static final String ALL_DATATYPES_AVRO_TABLE_NAME = "product_tests.write_all_datatypes_avro";
    private static final String STRUCTURAL_AVRO_TABLE_NAME = "product_tests.write_structural_datatype_avro";

    @Test
    void testInsertPrimitiveDataType(KafkaEnvironment env)
            throws Exception
    {
        String topicName = "write_all_datatypes_avro";
        env.createTopic(topicName);

        try (Connection conn = env.createTrinoConnection();
                Statement stmt = conn.createStatement()) {
            int inserted = stmt.executeUpdate(
                    "INSERT INTO " + KAFKA_CATALOG + "." + ALL_DATATYPES_AVRO_TABLE_NAME + " VALUES " +
                            "('jasio', 9223372036854775807, 1234567890.123456789, true), " +
                            "('stasio', -9223372036854775808, -1234567890.123456789, false), " +
                            "(null, null, null, null), " +
                            "('krzysio', 9223372036854775807, 1234567890.123456789, false), " +
                            "('kasia', 9223372036854775807, null, null)");
            assertThat(inserted).isEqualTo(5);

            Thread.sleep(1000);

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT * FROM " + KAFKA_CATALOG + "." + ALL_DATATYPES_AVRO_TABLE_NAME)) {
                List<List<Object>> rows = collectRowsNullable(rs);
                assertThat(rows).containsExactlyInAnyOrder(
                        List.of("jasio", 9223372036854775807L, 1234567890.123456789, true),
                        List.of("stasio", -9223372036854775808L, -1234567890.123456789, false),
                        nullList(4),
                        List.of("krzysio", 9223372036854775807L, 1234567890.123456789, false),
                        listWithNulls("kasia", 9223372036854775807L, null, null));
            }
        }
    }

    @Test
    void testInsertStructuralDataType(KafkaEnvironment env)
            throws Exception
    {
        String topicName = "write_structural_datatype_avro";
        env.createTopic(topicName);

        try (Connection conn = env.createTrinoConnection();
                Statement stmt = conn.createStatement()) {
            assertThatThrownBy(() -> stmt.executeUpdate(
                    "INSERT INTO " + KAFKA_CATALOG + "." + STRUCTURAL_AVRO_TABLE_NAME + " VALUES " +
                            "(ARRAY[100, 102], map_from_entries(ARRAY[('key1', 'value1')]))"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("Unsupported column type 'array(bigint)' for column 'c_array'");
        }
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
