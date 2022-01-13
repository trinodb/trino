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
package io.trino.tests.product.functions;

import io.trino.tempto.ProductTest;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.JSON_FUNCTIONS;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;

public class TestFunctions
        extends ProductTest
{
    @Test(groups = JSON_FUNCTIONS)
    public void testScalarFunction()
    {
        assertThat(onTrino().executeQuery("SELECT upper('value')")).containsExactlyInOrder(row("VALUE"));
    }

    @Test(groups = JSON_FUNCTIONS)
    public void testAggregate()
    {
        assertThat(onTrino().executeQuery("SELECT min(x) FROM (VALUES 1,2,3,4) t(x)")).containsExactlyInOrder(row(1));
    }
}
