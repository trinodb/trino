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
package io.trino.plugin.elasticsearch.client;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.plugin.elasticsearch.client.ElasticsearchClient.extractAddress;
import static org.testng.Assert.assertEquals;

public class TestExtractAddress
{
    @Test(dataProvider = "extractAddressValues")
    public void test(String nodeAddress, String address)
    {
        assertEquals(extractAddress(nodeAddress), Optional.ofNullable(address));
    }

    @DataProvider
    public static Object[][] extractAddressValues()
    {
        return new Object[][] {
                {"node/1.2.3.4:9200", "node:9200"},
                {"1.2.3.4:9200", "1.2.3.4:9200"},
                {"node/1.2.3.4:9200", "node:9200"},
                {"node/[fe80::1]:9200", "node:9200"},
                {"[fe80::1]:9200", "[fe80::1]:9200"},
                {"", null},
                {"node/1.2.3.4", null},
                {"node/1.2.3.4:xxxx", null},
                {"1.2.3.4:xxxx", null}
        };
    }
}
