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
package io.trino.tests.product.hive;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.lang.String.format;

/**
 * Tests for Avro schema literal support in Hive connector.
 * <p>
 * Ported from the Tempto-based TestAvroSchemaLiteral.
 */
@ProductTest
@RequiresEnvironment(HiveStorageFormatsEnvironment.class)
@TestGroup.StorageFormats
class TestAvroSchemaLiteral
{
    @Language("JSON")
    private static final String SCHEMA_LITERAL =
            """
            {
              "namespace": "io.trino.test",
              "name": "product_tests_avro_table",
              "type": "record",
              "fields": [
                { "name":"string_col", "type":"string"},
                { "name":"int_col", "type":"int" }
              ]
            }
            """;

    @Test
    void testHiveCreatedTable(HiveStorageFormatsEnvironment env)
    {
        env.executeHiveUpdate("DROP TABLE IF EXISTS test_avro_schema_literal_hive");
        env.executeHiveUpdate(format("" +
                        "CREATE TABLE test_avro_schema_literal_hive " +
                        "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' " +
                        "STORED AS " +
                        "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' " +
                        "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' " +
                        "TBLPROPERTIES ('avro.schema.literal'='%s')",
                SCHEMA_LITERAL));
        env.executeHiveUpdate("INSERT INTO test_avro_schema_literal_hive VALUES ('some text', 123042)");

        assertThat(env.executeHive("SELECT * FROM test_avro_schema_literal_hive")).containsExactlyInOrder(row("some text", 123042));
        assertThat(env.executeTrino("SELECT * FROM hive.default.test_avro_schema_literal_hive")).containsExactlyInOrder(row("some text", 123042));

        env.executeHiveUpdate("DROP TABLE test_avro_schema_literal_hive");
    }

    @Test
    void testTrinoCreatedTable(HiveStorageFormatsEnvironment env)
    {
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default.test_avro_schema_literal_trino");
        env.executeTrinoUpdate(format("CREATE TABLE hive.default.test_avro_schema_literal_trino (dummy_col VARCHAR) WITH (format='AVRO', avro_schema_literal='%s')", SCHEMA_LITERAL));
        env.executeTrinoUpdate("INSERT INTO hive.default.test_avro_schema_literal_trino VALUES ('some text', 123042)");

        assertThat(env.executeHive("SELECT * FROM test_avro_schema_literal_trino")).containsExactlyInOrder(row("some text", 123042));
        assertThat(env.executeTrino("SELECT * FROM hive.default.test_avro_schema_literal_trino")).containsExactlyInOrder(row("some text", 123042));

        env.executeTrinoUpdate("DROP TABLE hive.default.test_avro_schema_literal_trino");
    }
}
