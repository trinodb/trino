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
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import javax.validation.constraints.AssertTrue;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TestOracleConnectionPoolingConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(OracleConnectionPoolingConfig.class)
                .setMaxPoolSize(30)
                .setMinPoolSize(1)
                .setInactiveConnectionTimeout(new Duration(20, MINUTES)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("oracle.connection-pool.max-size", "10")
                .put("oracle.connection-pool.min-size", "5")
                .put("oracle.connection-pool.inactive-timeout", "10m")
                .build();

        OracleConnectionPoolingConfig expected = new OracleConnectionPoolingConfig()
                .setMaxPoolSize(10)
                .setMinPoolSize(5)
                .setInactiveConnectionTimeout(new Duration(10, MINUTES));

        assertFullMapping(properties, expected);
    }

    @Test
    public void testInvalidPoolingConfiguration()
    {
        assertFailsValidation(
                new OracleConnectionPoolingConfig()
                        .setMaxPoolSize(2)
                        .setMinPoolSize(3),
                "poolSizedProperly",
                "Max pool size must be greater or equal than min size",
                AssertTrue.class);
    }
}
