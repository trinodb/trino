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

package io.trino.plugin.memsql;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static io.trino.testing.assertions.Assert.assertEquals;

public class TestMemSqlClientModule
{
    @Test(dataProvider = "connectionUrls")
    public void testMariaDbUrlCompatability(String[] mariaDbUrls)
    {
        assertEquals(MemSqlClientModule.ensureUrlBackwardCompatibility(mariaDbUrls[0]), mariaDbUrls[1]);
    }

    @DataProvider
    public Object[][] connectionUrls()
    {
        return new Object[][] {
                // input / expected result
                {"jdbc:mariadb://test_mariadb", "jdbc:singlestore://test_mariadb"},
                {"JDBC:MARIADB://test_mariadb", "jdbc:singlestore://test_mariadb"},
                {"jdbc:mariadb://TEST_MARIADB", "jdbc:singlestore://TEST_MARIADB"},
                {"jdbc:mariadb:loadbalance://test_mariadb", "jdbc:singlestore:loadbalance://test_mariadb"},
                {"jdbc:MARIADB:sequential://test_mariadb", "jdbc:singlestore:sequential://test_mariadb"},
        };
    }
}
