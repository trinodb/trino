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

import com.google.common.collect.ImmutableList;
import io.trino.tempto.ProductTest;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTableWithLargeDataset
        extends ProductTest
{
    @Test
    public void testGroupBy()
    {
        onTrino().executeQuery("DROP TABLE IF EXISTS large_table");
        onTrino().executeQuery(format(
                """
                CREATE TABLE large_table AS
                SELECT date_add('day', a.sequential_number, date'2022-01-01') col1,
                       random(727637576,1152921504252452608) col2
                FROM TABLE(sequence(1, 630)) a
                CROSS JOIN TABLE(sequence(1, 2000000)) b
                CROSS JOIN TABLE(sequence(1, 2)) c
                WHERE c.sequential_number <= random(1,3)
                """));
        assertThat(onTrino().executeQuery(
                """
                SELECT count(*) BETWEEN 630.0 * 2000000 AND 630.0 * 2000000 * 2
                FROM (
                    SELECT col1, col2, count(*)
                    FROM large_table
                    GROUP BY 1,2
                )
                """)).containsOnly(ImmutableList.of(row(true)));
    }
}
