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
import static io.trino.tests.product.TestGroups.FUNCTIONS;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static org.testng.Assert.assertEquals;

public class TestFunctions
        extends ProductTest
{
    @Test(groups = FUNCTIONS)
    public void testSubstring()
    {
        assertThat(onTrino().executeQuery("SELECT SUBSTRING('ala ma kota' from 2 for 4)")).contains(row("la m"));
        assertThat(onTrino().executeQuery("SELECT SUBSTR('ala ma kota', 2, 4)")).contains(row("la m"));
    }

    @Test(groups = FUNCTIONS)
    public void testPosition()
    {
        assertThat(onTrino().executeQuery("SELECT POSITION('ma' IN 'ala ma kota')")).contains(row(5));
    }

    @Test(groups = FUNCTIONS)
    public void testVersion()
    {
        assertEquals(
                onTrino().executeQuery("SELECT version()").getOnlyValue(),
                onTrino().executeQuery("SELECT node_version FROM system.runtime.nodes WHERE coordinator = TRUE").getOnlyValue());
    }
}
