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

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.KAFKA;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;

public class TestKafkaAvroWritesSmokeTest
        extends ProductTest
{
    private static final String KAFKA_CATALOG = "kafka";

    private static final String ALL_DATATYPES_AVRO_TABLE_NAME = "product_tests.write_all_datatypes_avro";

    private static final String STRUCTURAL_AVRO_TABLE_NAME = "product_tests.write_structural_datatype_avro";

    @Test(groups = {KAFKA, PROFILE_SPECIFIC_TESTS})
    public void testInsertPrimitiveDataType()
    {
        assertThat(onTrino().executeQuery(format(
                "INSERT INTO %s.%s VALUES " +
                        "('jasio', 9223372036854775807, 1234567890.123456789, true), " +
                        "('stasio', -9223372036854775808, -1234567890.123456789, false), " +
                        "(null, null, null, null), " +
                        "('krzysio', 9223372036854775807, 1234567890.123456789, false), " +
                        "('kasia', 9223372036854775807, null, null)",
                KAFKA_CATALOG,
                ALL_DATATYPES_AVRO_TABLE_NAME)))
                .updatedRowsCountIsEqualTo(5);

        assertThat(onTrino().executeQuery(format(
                "SELECT * FROM %s.%s",
                KAFKA_CATALOG,
                ALL_DATATYPES_AVRO_TABLE_NAME)))
                .containsOnly(
                        row("jasio", 9223372036854775807L, 1234567890.123456789, true),
                        row("stasio", -9223372036854775808L, -1234567890.123456789, false),
                        row(null, null, null, null),
                        row("krzysio", 9223372036854775807L, 1234567890.123456789, false),
                        row("kasia", 9223372036854775807L, null, null));
    }

    @Test(groups = {KAFKA, PROFILE_SPECIFIC_TESTS})
    public void testInsertStructuralDataType()
    {
        assertQueryFailure(() -> onTrino().executeQuery(format(
                "INSERT INTO %s.%s VALUES " +
                        "(ARRAY[100, 102], map_from_entries(ARRAY[('key1', 'value1')]))",
                KAFKA_CATALOG,
                STRUCTURAL_AVRO_TABLE_NAME)))
                .hasMessageMatching("Query failed \\(.+\\): Unsupported column type 'array\\(bigint\\)' for column 'c_array'");
    }
}
