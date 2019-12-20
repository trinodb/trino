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
package io.prestosql.server;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

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
                .setHttpsRequired(false)
                .setKeyStorePath(null)
                .setKeyStorePassword(null)
                .setTrustStorePath(null)
                .setTrustStorePassword(null)
                .setKerberosEnabled(false)
                .setKerberosUseCanonicalHostname(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("internal-communication.shared-secret", "secret")
                .put("internal-communication.https.required", "true")
                .put("internal-communication.https.keystore.path", "key-path")
                .put("internal-communication.https.keystore.key", "key-key")
                .put("internal-communication.https.truststore.path", "trust-path")
                .put("internal-communication.https.truststore.key", "trust-key")
                .put("internal-communication.kerberos.enabled", "true")
                .put("internal-communication.kerberos.use-canonical-hostname", "false")
                .build();

        InternalCommunicationConfig expected = new InternalCommunicationConfig()
                .setSharedSecret("secret")
                .setHttpsRequired(true)
                .setKeyStorePath("key-path")
                .setKeyStorePassword("key-key")
                .setTrustStorePath("trust-path")
                .setTrustStorePassword("trust-key")
                .setKerberosEnabled(true)
                .setKerberosUseCanonicalHostname(false);

        assertFullMapping(properties, expected);
    }
}
