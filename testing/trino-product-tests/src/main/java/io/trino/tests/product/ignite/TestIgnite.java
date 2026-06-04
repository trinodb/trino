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
package io.trino.tests.product.ignite;

import io.trino.tempto.ProductTest;
import io.trino.tempto.query.QueryResult;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.IGNITE;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIgnite
        extends ProductTest
{
    @Test(groups = {IGNITE, PROFILE_SPECIFIC_TESTS})
    public void testCreateTableAsSelect()
    {
        QueryResult result = onTrino().executeQuery("CREATE TABLE nation AS SELECT * FROM tpch.tiny.nation");
        try {
            assertThat(result).updatedRowsCountIsEqualTo(25);
            assertThat(onTrino().executeQuery("SELECT COUNT(*) FROM nation"))
                    .containsOnly(row(25));
        }
        finally {
            onTrino().executeQuery("DROP TABLE nation");
        }
    }

    @Test(groups = {IGNITE, PROFILE_SPECIFIC_TESTS})
    public void testHighPrecisionDecimalMapsToNumber()
    {
        String tableName = "test_decimal_number_" + randomNameSuffix();
        onTrino().executeQuery(format("CALL system.execute('CREATE TABLE %s (id int primary key, d40 decimal(40, 5), d50 decimal(50, 0))')", tableName));
        try {
            onTrino().executeQuery(format(
                    "CALL system.execute('INSERT INTO %s VALUES (1, CAST(''12345678901234567890123456789012345.12345'' AS decimal(40, 5)), CAST(''12345678901234567890123456789012345678901234567890'' AS decimal(50, 0)))')",
                    tableName));

            assertThat(onTrino().executeQuery(format(
                    "SELECT typeof(d40), d40 = NUMBER '12345678901234567890123456789012345.12345', typeof(d50), d50 = NUMBER '12345678901234567890123456789012345678901234567890' FROM %s",
                    tableName)))
                    .containsOnly(row(
                            "number",
                            true,
                            "number",
                            true));
        }
        finally {
            onTrino().executeQuery("DROP TABLE " + tableName);
        }
    }
}
