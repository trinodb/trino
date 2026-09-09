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
package io.trino.plugin.httpcredentials;

import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

class TestHttpExtraCredentialsConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(HttpExtraCredentialsConfig.class)
                .setUri(null)
                .setSharedSecret(null)
                .setCacheTtl(new Duration(30, SECONDS)));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = Map.of(
                "http-extra-credentials.uri", "http://example.com:8080/credentials",
                "http-extra-credentials.shared-secret", "secret",
                "http-extra-credentials.cache-ttl", "5m");

        HttpExtraCredentialsConfig expected = new HttpExtraCredentialsConfig()
                .setUri(URI.create("http://example.com:8080/credentials"))
                .setSharedSecret("secret")
                .setCacheTtl(new Duration(5, MINUTES));

        assertFullMapping(properties, expected);
    }
}
