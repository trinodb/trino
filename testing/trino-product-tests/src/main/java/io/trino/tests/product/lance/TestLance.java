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
package io.trino.tests.product.lance;

import io.trino.tempto.ProductTest;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tests.product.TestGroups.LANCE;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onSpark;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLance
        extends ProductTest
{
    @Test(groups = {LANCE, PROFILE_SPECIFIC_TESTS})
    public void testShowSchemas()
    {
        onSpark().executeQuery("""
                CREATE TABLE example
                (
                  key bigint,
                  value string
                )
                """);
        onSpark().executeQuery("""
                INSERT INTO example
                VALUES
                    (1, 'A'),
                    (2, 'B'),
                    (3, 'C'),
                    (4, 'D'),
                    (5, 'E');
                """);

        assertThat(onTrino().executeQuery("SHOW SCHEMAS FROM lance")).containsOnly(row("default"), row("information_schema"));
        assertThat(onTrino().executeQuery("SELECT count(*) FROM lance.default.example")).containsExactlyInOrder(row(5));
    }
}
