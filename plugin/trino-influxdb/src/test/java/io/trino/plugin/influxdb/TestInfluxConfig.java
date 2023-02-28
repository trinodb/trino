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

package io.trino.plugin.influxdb;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import javax.validation.constraints.AssertFalse;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestInfluxConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(InfluxConfig.class)
                .setEndpoint(null)
                .setUsername(null)
                .setPassword(null)
                .setConnectTimeOut(new Duration(10, SECONDS))
                .setReadTimeOut(new Duration(60, SECONDS)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("influx.endpoint", "http://localhost:8086")
                .put("influx.username", "testuser")
                .put("influx.password", "testpass")
                .put("influx.connect-timeout", "5s")
                .put("influx.read-timeout", "30s")
                .buildOrThrow();

        InfluxConfig expected = new InfluxConfig()
                .setEndpoint("http://localhost:8086")
                .setUsername("testuser")
                .setPassword("testpass")
                .setConnectTimeOut(new Duration(5, SECONDS))
                .setReadTimeOut(new Duration(30, SECONDS));

        assertFullMapping(properties, expected);
    }

    @Test
    public void testValidation()
    {
        assertFailsPasswordValidation(new InfluxConfig().setUsername("testuser"));
        assertFailsPasswordValidation(new InfluxConfig().setPassword("testpass"));
    }

    private static void assertFailsPasswordValidation(InfluxConfig config)
    {
        assertFailsValidation(
                config,
                "validPasswordConfig",
                "'influx.username' and 'influx.password' must be both empty or both non-empty.",
                AssertFalse.class);
    }
}
