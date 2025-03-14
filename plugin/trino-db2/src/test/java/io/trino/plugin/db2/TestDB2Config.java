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
package io.trino.plugin.db2;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestDB2Config
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(DB2Config.class)
                .setVarcharMaxLength(32672)
                .setApiKey(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        int testVarcharLength = 30000;
        String testApiKey = "xyz";

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("db2.varchar-max-length", String.valueOf(testVarcharLength))
                .put("db2.iam-api-key", testApiKey)
                .buildOrThrow();

        DB2Config expected = new DB2Config()
                .setVarcharMaxLength(testVarcharLength)
                .setApiKey(testApiKey);

        assertFullMapping(properties, expected);
    }
}
