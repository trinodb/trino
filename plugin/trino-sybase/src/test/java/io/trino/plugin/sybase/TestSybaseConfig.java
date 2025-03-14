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
package io.trino.plugin.sybase;

public class TestSybaseConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(SybaseConfig.class)
                .setVarcharMaxLength(32672)
                .setApiKey(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        int testVarcharLength = 30000;
        String testApiKey = "xyz";

        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("sybase.varchar-max-length", String.valueOf(testVarcharLength))
                .put("sybase.iam-api-key", testApiKey)
                .build();

        SybaseConfig expected = new SybaseConfig()
                .setVarcharMaxLength(testVarcharLength)
                .setApiKey(testApiKey);

        assertFullMapping(properties, expected);
    }
}
