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

import com.google.common.collect.ImmutableList;
import io.trino.tempto.ProductTest;
import io.trino.tempto.fulfillment.table.TableManager;
import io.trino.tempto.fulfillment.table.kafka.KafkaTableDefinition;
import io.trino.tempto.fulfillment.table.kafka.KafkaTableManager;
import io.trino.tempto.fulfillment.table.kafka.ListKafkaDataSource;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.sql.Timestamp;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.context.ThreadLocalTestContextHolder.testContext;
import static io.trino.tempto.fulfillment.table.TableHandle.tableHandle;
import static io.trino.tests.product.TestGroups.KAFKA;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestKafkaProtobufWritesSmokeTest
        extends ProductTest
{
    private static final String KAFKA_CATALOG = "kafka";
    private static final String KAFKA_SCHEMA = "product_tests";

    private static final String ALL_DATATYPES_PROTOBUF_TABLE_NAME = "all_datatypes_protobuf";
    private static final String ALL_DATATYPES_PROTOBUF_TOPIC_NAME = "write_all_datatypes_protobuf";

    private static final String STRUCTURAL_PROTOBUF_TABLE_NAME = "structural_datatype_protobuf";
    private static final String STRUCTURAL_PROTOBUF_TOPIC_NAME = "structural_datatype_protobuf";

    private static void createProtobufTable(String tableName, String topicName)
    {
        KafkaTableDefinition tableDefinition = new KafkaTableDefinition(
                tableName,
                topicName,
                new ListKafkaDataSource(ImmutableList.of()),
                1,
                1);
        KafkaTableManager kafkaTableManager = (KafkaTableManager) testContext().getDependency(TableManager.class, "kafka");
        kafkaTableManager.createImmutable(tableDefinition, tableHandle(tableName).inSchema(KAFKA_SCHEMA));
    }

    @Test(groups = {KAFKA, PROFILE_SPECIFIC_TESTS})
    public void testInsertAllDataType()
    {
        createProtobufTable(ALL_DATATYPES_PROTOBUF_TABLE_NAME, ALL_DATATYPES_PROTOBUF_TOPIC_NAME);
        assertThat(onTrino().executeQuery(format(
                "INSERT INTO %s.%s.%s VALUES " +
                        "('Chennai', 314, 9223372036854775807, 1234567890.123456789, 3.14, true, 'ZERO', TIMESTAMP '2020-12-21 15:45:00.012345')," +
                        "('TamilNadu', -314, -9223372036854775808, -1234567890.123456789, -3.14, false, 'ONE', TIMESTAMP '1970-01-01 15:45:00.012345'), " +
                        "('India', 314, 9223372036854775807, 1234567890.123456789, 3.14, false, 'TWO', TIMESTAMP '0001-01-01 00:00:00.000001')",
                KAFKA_CATALOG,
                KAFKA_SCHEMA,
                ALL_DATATYPES_PROTOBUF_TABLE_NAME)))
                .updatedRowsCountIsEqualTo(3);

        assertThat(onTrino().executeQuery(format(
                "SELECT * FROM %s.%s.%s",
                KAFKA_CATALOG,
                KAFKA_SCHEMA,
                ALL_DATATYPES_PROTOBUF_TABLE_NAME)))
                .containsOnly(
                        row("Chennai", 314, 9223372036854775807L, 1234567890.123456789, 3.14f, true, "ZERO", Timestamp.valueOf("2020-12-21 15:45:00.012345")),
                        row("TamilNadu", -314, -9223372036854775808L, -1234567890.123456789, -3.14f, false, "ONE", Timestamp.valueOf("1970-01-01 15:45:00.012345")),
                        row("India", 314, 9223372036854775807L, 1234567890.123456789, 3.14f, false, "TWO", Timestamp.valueOf("0001-01-01 00:00:00.000001")));

        assertQueryFailure(() -> onTrino().executeQuery(format(
                "INSERT INTO %s.%s.%s (h_varchar) VALUES ('Chennai')", KAFKA_CATALOG, KAFKA_SCHEMA, ALL_DATATYPES_PROTOBUF_TABLE_NAME)))
                .isInstanceOf(SQLException.class)
                .hasMessageMatching("Query failed \\(.+\\): Protobuf doesn't support serializing null values");
    }

    @Test(groups = {KAFKA, PROFILE_SPECIFIC_TESTS})
    public void testInsertStructuralDataType()
    {
        createProtobufTable(STRUCTURAL_PROTOBUF_TABLE_NAME, STRUCTURAL_PROTOBUF_TOPIC_NAME);
        assertThat(onTrino().executeQuery(format(
                "INSERT INTO %s.%s.%s VALUES " +
                        "(ARRAY[CAST(ROW('Entry1') AS ROW(simple_string VARCHAR))], " +
                        "map_from_entries(ARRAY[('key1', CAST(ROW('value1') AS ROW(simple_string VARCHAR)))]), " +
                        "CAST(ROW(1234567890.123456789, 3.14, 'ONE') AS ROW(d_double DOUBLE, e_float REAL, g_enum VARCHAR)), " +
                        "'Chennai', " +
                        "314, " +
                        "9223372036854775807, " +
                        "CAST(ROW('Entry2') AS ROW(simple_string VARCHAR)), " +
                        "TIMESTAMP '2020-12-21 15:45:00.012345')",
                KAFKA_CATALOG,
                KAFKA_SCHEMA,
                STRUCTURAL_PROTOBUF_TABLE_NAME)))
                .updatedRowsCountIsEqualTo(1);

        assertThat(onTrino().executeQuery(format(
                "SELECT c_array[1].simple_string, b_map['key1'].simple_string, a_row.d_double, a_row.e_float, a_row.g_enum, a_string, c_integer, c_bigint, d_row.simple_string, e_timestamp FROM %s.%s.%s",
                KAFKA_CATALOG,
                KAFKA_SCHEMA,
                STRUCTURAL_PROTOBUF_TABLE_NAME)))
                .containsOnly(
                        row(
                                "Entry1",
                                "value1",
                                1234567890.1234567890,
                                3.14f,
                                "ONE",
                                "Chennai",
                                314,
                                9223372036854775807L,
                                "Entry2",
                                Timestamp.valueOf("2020-12-21 15:45:00.012345")));
    }
}
