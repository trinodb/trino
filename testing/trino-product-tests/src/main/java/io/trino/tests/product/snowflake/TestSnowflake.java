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
package io.trino.tests.product.snowflake;

import io.trino.tempto.ProductTest;
import io.trino.tempto.query.QueryResult;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.TestGroups.SNOWFLAKE;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSnowflake
        extends ProductTest
{
    @Test(groups = {SNOWFLAKE, PROFILE_SPECIFIC_TESTS})
    public void testCreateTableAsSelect()
    {
        String tableName = "snowflake.tpch.nation_" + randomNameSuffix();

        onTrino().executeQuery("DROP TABLE IF EXISTS " + tableName);
        QueryResult result = onTrino().executeQuery("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.tiny.nation");
        try {
            assertThat(result).updatedRowsCountIsEqualTo(25);
            assertThat(onTrino().executeQuery("SELECT COUNT(*) FROM " + tableName))
                    .containsOnly(row(25));
        }
        finally {
            onTrino().executeQuery("DROP TABLE " + tableName);
        }
    }
}
