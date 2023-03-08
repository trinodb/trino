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
package io.trino.tests.product.exasol;

import io.trino.tempto.ProductTest;
import org.testng.annotations.Test;

import java.math.BigDecimal;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.EXASOL;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onExasol;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;

public class TestExasol
        extends ProductTest
{
    @Test(groups = {EXASOL, PROFILE_SPECIFIC_TESTS})
    public void testSelect()
    {
        String schemaName = "test_" + randomNameSuffix();
        String tableName = schemaName + ".tab";
        onExasol().executeQuery("CREATE SCHEMA " + schemaName);
        try {
            onExasol().executeQuery("CREATE TABLE " + tableName + " (id integer, name varchar(5))");
            onExasol().executeQuery("INSERT INTO " + tableName + " VALUES (1, 'a')");
            assertThat(onTrino().executeQuery("SELECT * FROM " + tableName))
                    .containsOnly(row(BigDecimal.valueOf(1), "a"));
        }
        finally {
            onExasol().executeQuery("DROP TABLE IF EXISTS " + tableName);
            onExasol().executeQuery("DROP SCHEMA " + schemaName);
        }
    }
}
