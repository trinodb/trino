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
package io.trino.plugin.kudu;

import com.google.common.collect.ImmutableMap;
import com.google.inject.ConfigurationException;
import io.airlift.configuration.ConfigurationFactory;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.nio.file.Files.createTempFile;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class TestKuduKerberosConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(KuduKerberosConfig.class)
                .setClientPrincipal(null)
                .setClientKeytab(null)
                .setConfig(null)
                .setKuduPrincipalPrimary(null));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Path keytab = createTempFile(null, null);
        Path config = createTempFile(null, null);

        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("kudu.authentication.client.principal", "principal")
                .put("kudu.authentication.client.keytab", keytab.toString())
                .put("kudu.authentication.config", config.toString())
                .put("kudu.authentication.server.principal.primary", "principal-primary")
                .buildOrThrow();

        KuduKerberosConfig expected = new KuduKerberosConfig()
                .setClientPrincipal("principal")
                .setClientKeytab(keytab.toFile())
                .setConfig(config.toFile())
                .setKuduPrincipalPrimary("principal-primary");

        assertFullMapping(properties, expected);
    }

    @Test
    public void testExplicitPropertyMappingsWithNonExistentPathsThrowsErrors()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("kudu.authentication.client.principal", "principal")
                .put("kudu.authentication.client.keytab", "/path/does/not/exist")
                .put("kudu.authentication.config", "/path/does/not/exist")
                .put("kudu.authentication.server.principal.primary", "principal-primary")
                .buildOrThrow();

        ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
        assertThatThrownBy(() -> configurationFactory.build(KuduKerberosConfig.class))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContainingAll(
                        "Invalid configuration property kudu.authentication.config: file does not exist",
                        "Invalid configuration property kudu.authentication.client.keytab: file does not exist");
    }
}
