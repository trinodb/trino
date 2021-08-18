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

import io.trino.tempto.ProductTest;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;

public class TestAvroFileHiveTable
        extends ProductTest
{
    @Test
    public void testWritingAndReadingAvroTableWithTimestampColumn()
    {
        onHive().executeQuery("DROP TABLE IF EXISTS test_avro_table");
        onTrino().executeQuery("CREATE TABLE test_avro_table (id BIGINT, ts TIMESTAMP) WITH (format = 'AVRO')");
        onHive().executeQuery("INSERT INTO test_avro_table VALUES(1, '2020-01-01 12:34:56.123')");
        assertThat(onHive().executeQuery("SELECT CAST(ts as VARCHAR(100)) FROM test_avro_table")).containsOnly(row("2020-01-01 12:34:56.123"));
        assertThat(onTrino().executeQuery("SELECT CAST(ts as VARCHAR(100)) FROM test_avro_table")).containsOnly(row("2020-01-01 12:34:56.123"));
    }
}
