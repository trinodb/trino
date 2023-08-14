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
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.TestGroups.STARBURST_SNOWFLAKE;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.util.UUID.randomUUID;

/**
 * Adapted from SEP's {@code TestSnowflake}, but doesn't test the distributed
 * connector.
 */
public class TestStarburstSnowflake
        extends ProductTest
{
    @Test(groups = {STARBURST_SNOWFLAKE, PROFILE_SPECIFIC_TESTS})
    public void testCreateTableAsSelect()
    {
        String catalog = "snowflake_jdbc";
        final String suffix = randomUUID().toString().replace("-", "_");

        onTrino().executeQuery(format("DROP TABLE IF EXISTS %s.test_schema.nation_%s", catalog, suffix));

        assertThat(onTrino().executeQuery(
                format("""
                        CREATE TABLE %s.test_schema.nation_%s
                        AS SELECT * FROM tpch.tiny.nation
                        """, catalog, suffix)))
                .containsOnly(row(25));

        try {
            assertThat(onTrino().executeQuery(
                    format("SELECT * FROM %s.test_schema.nation_%s", catalog, suffix)))
                    .hasRowsCount(25);
        }
        finally {
            onTrino().executeQuery(format("DROP TABLE %s.test_schema.nation_%s", catalog, suffix));
        }
    }
}
