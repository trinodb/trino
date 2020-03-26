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
package com.starburstdata.presto.plugin.oracle;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import org.testng.annotations.Test;

import java.math.RoundingMode;
import java.util.Map;

public class TestOracleConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(OracleConfig.class)
                .setImpersonationEnabled(false)
                .setSynonymsEnabled(false)
                .setConnectionPoolingEnabled(true)
                .setNumberRoundingMode(RoundingMode.UNNECESSARY)
                .setDefaultNumberScale(null)
                .setAuthenticationType(OracleAuthenticationType.USER_PASSWORD));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("oracle.impersonation.enabled", "true")
                .put("oracle.synonyms.enabled", "true")
                .put("oracle.connection-pool.enabled", "false")
                .put("oracle.number.rounding-mode", "HALF_EVEN")
                .put("oracle.number.default-scale", "0")
                .put("oracle.authentication.type", "KERBEROS")
                .build();

        OracleConfig expected = new OracleConfig()
                .setImpersonationEnabled(true)
                .setSynonymsEnabled(true)
                .setConnectionPoolingEnabled(false)
                .setNumberRoundingMode(RoundingMode.HALF_EVEN)
                .setDefaultNumberScale(0)
                .setAuthenticationType(OracleAuthenticationType.KERBEROS);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
