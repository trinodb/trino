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

import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;

public class TestDisablePlugin
        extends ProductTest
{
    @Test
    public void testPluginEnabled()
    {
        // functions are global, so they are not specific to the specific connector
        assertThat(onTrino().executeQuery("SELECT ST_Point(1, 2)")).hasRowsCount(1);
    }

    @Test(groups = {PROFILE_SPECIFIC_TESTS})
    public void testPluginDisabled()
    {
        // functions are global, so they are not specific to the specific connector
        assertQueryFailure(() -> onTrino().executeQuery("SELECT ST_Point(1, 2)")).hasMessageContaining("Function 'st_point' not registered");
    }
}
