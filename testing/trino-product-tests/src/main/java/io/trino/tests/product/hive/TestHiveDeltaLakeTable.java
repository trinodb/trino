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

import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;

public class TestHiveDeltaLakeTable
        extends HiveProductTest
{
    @Test
    public void testReadDeltaLakeTable()
    {
        onHive().executeQuery("DROP TABLE IF EXISTS test_delta_lake_table");

        onHive().executeQuery("" +
                "CREATE TABLE test_delta_lake_table (ignored int) " +
                "TBLPROPERTIES ('spark.sql.sources.provider'='DELTA')");

        assertQueryFailure(() -> onTrino().executeQuery("SELECT * FROM test_delta_lake_table"))
                .hasMessageContaining("Cannot query Delta Lake table 'default.test_delta_lake_table'");

        onHive().executeQuery("DROP TABLE test_delta_lake_table");
    }
}
