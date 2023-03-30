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
package io.trino.plugin.jdbc.credential.file;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestConfigFileBasedCredentialProviderConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ConfigFileBasedCredentialProviderConfig.class)
                .setCredentialFile(null));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Path credentialFile = Files.createTempFile(null, null);

        Map<String, String> properties = ImmutableMap.of("connection-credential-file", credentialFile.toString());

        ConfigFileBasedCredentialProviderConfig expected = new ConfigFileBasedCredentialProviderConfig()
                .setCredentialFile(credentialFile.toString());

        assertFullMapping(properties, expected);
    }
}
