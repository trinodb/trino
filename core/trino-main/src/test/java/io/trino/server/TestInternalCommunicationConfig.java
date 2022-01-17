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
package io.trino.server;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestInternalCommunicationConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(InternalCommunicationConfig.class)
                .setSharedSecret(null)
                .setHttp2Enabled(false)
                .setHttpsRequired(false)
                .setKeyStorePath(null)
                .setKeyStorePassword(null)
                .setTrustStorePath(null)
                .setTrustStorePassword(null)
                .setHttpServerHttpsEnabled(false));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Path keystoreFile = Files.createTempFile(null, null);
        Path truststoreFile = Files.createTempFile(null, null);

        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("internal-communication.shared-secret", "secret")
                .put("internal-communication.http2.enabled", "true")
                .put("internal-communication.https.required", "true")
                .put("internal-communication.https.keystore.path", keystoreFile.toString())
                .put("internal-communication.https.keystore.key", "key-key")
                .put("internal-communication.https.truststore.path", truststoreFile.toString())
                .put("internal-communication.https.truststore.key", "trust-key")
                .put("http-server.https.enabled", "true")
                .buildOrThrow();

        InternalCommunicationConfig expected = new InternalCommunicationConfig()
                .setSharedSecret("secret")
                .setHttp2Enabled(true)
                .setHttpsRequired(true)
                .setKeyStorePath(keystoreFile.toString())
                .setKeyStorePassword("key-key")
                .setTrustStorePath(truststoreFile.toString())
                .setTrustStorePassword("trust-key")
                .setHttpServerHttpsEnabled(true);

        assertFullMapping(properties, expected);
    }
}
