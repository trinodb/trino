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
package io.trino.tests.hive;

import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.TestGroups.AVRO;
import static io.trino.tests.utils.QueryExecutors.onHive;
import static io.trino.tests.utils.QueryExecutors.onPresto;
import static java.lang.String.format;

public class TestAvroSchemaLiteral
        extends HiveProductTest
{
    @Language("JSON")
    private static final String SCHEMA_LITERAL = "{\n" +
            "  \"namespace\": \"io.trino.test\",\n" +
            "  \"name\": \"product_tests_avro_table\",\n" +
            "  \"type\": \"record\",\n" +
            "  \"fields\": [\n" +
            "    { \"name\":\"string_col\", \"type\":\"string\"},\n" +
            "    { \"name\":\"int_col\", \"type\":\"int\" }\n" +
            "]}";

    @Test(groups = AVRO)
    public void testHiveCreatedTable()
    {
        onHive().executeQuery("DROP TABLE IF EXISTS test_avro_schema_literal_hive");
        onHive().executeQuery(format("" +
                        "CREATE TABLE test_avro_schema_literal_hive " +
                        "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' " +
                        "STORED AS " +
                        "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' " +
                        "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' " +
                        "TBLPROPERTIES ('avro.schema.literal'='%s')",
                SCHEMA_LITERAL));
        onHive().executeQuery("INSERT INTO test_avro_schema_literal_hive VALUES ('some text', 123042)");

        assertThat(onHive().executeQuery("SELECT * FROM test_avro_schema_literal_hive")).containsExactlyInOrder(row("some text", 123042));
        assertThat(onPresto().executeQuery("SELECT * FROM test_avro_schema_literal_hive")).containsExactlyInOrder(row("some text", 123042));

        onHive().executeQuery("DROP TABLE test_avro_schema_literal_hive");
    }

    @Test(groups = AVRO)
    public void testPrestoCreatedTable()
    {
        onPresto().executeQuery("DROP TABLE IF EXISTS test_avro_schema_literal_presto");
        onPresto().executeQuery(format("CREATE TABLE test_avro_schema_literal_presto (dummy_col VARCHAR) WITH (format='AVRO', avro_schema_literal='%s')", SCHEMA_LITERAL));
        onPresto().executeQuery("INSERT INTO test_avro_schema_literal_presto VALUES ('some text', 123042)");

        assertThat(onHive().executeQuery("SELECT * FROM test_avro_schema_literal_presto")).containsExactlyInOrder(row("some text", 123042));
        assertThat(onPresto().executeQuery("SELECT * FROM test_avro_schema_literal_presto")).containsExactlyInOrder(row("some text", 123042));

        onPresto().executeQuery("DROP TABLE test_avro_schema_literal_presto");
    }
}
