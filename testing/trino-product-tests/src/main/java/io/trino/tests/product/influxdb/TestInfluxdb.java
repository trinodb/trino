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
package io.trino.tests.product.influxdb;

import io.trino.tempto.ProductTest;
import io.trino.tempto.query.QueryResult;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.INFLUXDB;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;

public class TestInfluxdb
        extends ProductTest
{
    @Test(groups = {INFLUXDB, PROFILE_SPECIFIC_TESTS})
    public void testShowSchemas()
    {
        QueryResult result = onTrino().executeQuery("SHOW SCHEMAS FROM influxdb");
        assertThat(result).contains(row("product_test"));
    }

    @Test(groups = {INFLUXDB, PROFILE_SPECIFIC_TESTS})
    public void testShowTable()
    {
        QueryResult result = onTrino().executeQuery("SHOW TABLES FROM influxdb.product_test");
        assertThat(result).contains(row("cpu_load_1"), row("cpu_load_5"), row("cpu_load_15"));
    }

    @Test(groups = {INFLUXDB, PROFILE_SPECIFIC_TESTS})
    public void testSelect()
    {
        assertThat(onTrino().executeQuery("SELECT * FROM influxdb.product_test.cpu_load_1 LIMIT 5"))
                .hasRowsCount(5)
                .hasColumnsCount(3);

        assertThat(onTrino().executeQuery("SELECT * FROM influxdb.product_test.cpu_load_1 WHERE host='none'"))
                .hasNoRows();

        assertThat(onTrino().executeQuery("SELECT host, value FROM influxdb.product_test.cpu_load_5 LIMIT 5"))
                .hasRowsCount(5)
                .hasColumnsCount(2);

        assertThat(onTrino().executeQuery("SELECT host, value FROM influxdb.product_test.cpu_load_5 where value < -1"))
                .hasNoRows();

        assertThat(onTrino().executeQuery("SELECT time, value FROM influxdb.product_test.cpu_load_15 WHERE host='server1' LIMIT 10"))
                .hasRowsCount(10)
                .hasColumnsCount(2);

        assertThat(onTrino().executeQuery("SELECT time, value FROM influxdb.product_test.cpu_load_15 where value < -1"))
                .hasNoRows();
    }
}
