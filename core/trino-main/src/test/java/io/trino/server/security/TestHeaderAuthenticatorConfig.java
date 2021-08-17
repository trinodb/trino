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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestHeaderAuthenticatorConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(HeaderAuthenticatorConfig.class)
                .setHeaderAuthenticatorFiles(ImmutableList.of("etc/header-authenticator.properties"))
                .setUserMappingPattern(null)
                .setUserMappingFile(null));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Path userMappingFile = Files.createTempFile(null, null);
        Path config1 = Files.createTempFile(null, null);
        Path config2 = Files.createTempFile(null, null);

        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("http-server.authentication.header.user-mapping.pattern", "(.*)@something")
                .put("http-server.authentication.header.user-mapping.file", userMappingFile.toString())
                .put("header-authenticator.config-files", config1.toString() + "," + config2.toString())
                .build();

        HeaderAuthenticatorConfig expected = new HeaderAuthenticatorConfig()
                .setHeaderAuthenticatorFiles(ImmutableList.of(config1.toAbsolutePath().toString(), config2.toAbsolutePath().toString()))
                .setUserMappingPattern("(.*)@something")
                .setUserMappingFile(userMappingFile.toFile());

        assertFullMapping(properties, expected);
    }
}
