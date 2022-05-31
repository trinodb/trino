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
package io.trino.server.security;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestPasswordAuthenticatorConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(PasswordAuthenticatorConfig.class)
                .setUserMappingPattern(null)
                .setUserMappingFile(null)
                .setPasswordAuthenticatorFiles("etc/password-authenticator.properties"));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Path userMappingFile = Files.createTempFile(null, null);
        Path config1 = Files.createTempFile(null, null);
        Path config2 = Files.createTempFile(null, null);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("http-server.authentication.password.user-mapping.pattern", "(.*)@something")
                .put("http-server.authentication.password.user-mapping.file", userMappingFile.toString())
                .put("password-authenticator.config-files", config1.toString() + "," + config2.toString())
                .buildOrThrow();

        PasswordAuthenticatorConfig expected = new PasswordAuthenticatorConfig()
                .setUserMappingPattern("(.*)@something")
                .setUserMappingFile(userMappingFile.toFile())
                .setPasswordAuthenticatorFiles(config1 + "," + config2);

        assertFullMapping(properties, expected);
    }
}
