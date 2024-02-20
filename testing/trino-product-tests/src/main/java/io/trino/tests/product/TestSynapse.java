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
package io.trino.tests.product;

import io.trino.tempto.ProductTest;
import io.trino.tempto.query.QueryResult;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.TestGroups.SYNAPSE;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;

public class TestSynapse
        extends ProductTest
{
    @Test(groups = {SYNAPSE, PROFILE_SPECIFIC_TESTS})
    public void testCreateTableAsSelect()
    {
        String tableName = "nation_" + randomNameSuffix();

        QueryResult queryResult = onTrino().executeQuery(
                format("""
                        CREATE TABLE synapse.dbo.%s
                        AS SELECT *
                        FROM tpch.tiny.nation
                        """, tableName));
        try {
            assertThat(queryResult).containsOnly(row(25));

            assertThat(onTrino().executeQuery("SELECT * FROM synapse.dbo." + tableName))
                    .hasRowsCount(25);

            assertThat(onTrino().executeQuery(
                    format("""
                            ALTER TABLE synapse.dbo.%s
                            ADD COLUMN extra_column VARCHAR
                            """, tableName)))
                    .containsOnly(row(0));

            assertThat(onTrino().executeQuery(
                    format("""
                            INSERT INTO synapse.dbo.%s (nationkey, name, extra_column)
                            VALUES (100, 'invalid', 'extra value')
                            """, tableName)))
                    .containsOnly(row(1));

            assertThat(onTrino().executeQuery(
                    format("""
                            SELECT nationkey, name, extra_column, regionkey, comment
                            FROM synapse.dbo.%s
                            WHERE extra_column = 'extra value'
                            """, tableName)))
                    .containsOnly(row(100, "invalid", "extra value", null, null));
        }
        finally {
            assertThat(onTrino().executeQuery("DROP TABLE synapse.dbo." + tableName))
                    .containsOnly(row(0));
        }
    }
}
