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
package io.trino.server.security.oauth2;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestOidcDiscoveryConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(OidcDiscoveryConfig.class)
                .setDiscoveryTimeout(new Duration(30, SECONDS))
                .setUserinfoEndpointEnabled(true));
    }

    @Test
    public void testExplicitPropertyMapping()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("http-server.authentication.oauth2.oidc.discovery.timeout", "1m")
                .put("http-server.authentication.oauth2.oidc.use-userinfo-endpoint", "false")
                .buildOrThrow();

        OidcDiscoveryConfig expected = new OidcDiscoveryConfig()
                .setDiscoveryTimeout(new Duration(1, MINUTES))
                .setUserinfoEndpointEnabled(false);

        assertFullMapping(properties, expected);
    }
}
