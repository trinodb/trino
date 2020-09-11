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
package io.prestosql.server.security;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestJsonWebTokenConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(JsonWebTokenConfig.class)
                .setKeyFile(null)
                .setRequiredAudience(null)
                .setRequiredIssuer(null)
                .setUserMappingPattern(null)
                .setUserMappingFile(null));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Path jwtKeyFile = Files.createTempFile(null, null);
        Path userMappingFile = Files.createTempFile(null, null);

        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("http-server.authentication.jwt.key-file", jwtKeyFile.toString())
                .put("http-server.authentication.jwt.required-audience", "some-audience")
                .put("http-server.authentication.jwt.required-issuer", "some-issuer")
                .put("http-server.authentication.jwt.user-mapping.pattern", "(.*)@something")
                .put("http-server.authentication.jwt.user-mapping.file", userMappingFile.toString())
                .build();

        JsonWebTokenConfig expected = new JsonWebTokenConfig()
                .setKeyFile(jwtKeyFile.toString())
                .setRequiredAudience("some-audience")
                .setRequiredIssuer("some-issuer")
                .setUserMappingPattern("(.*)@something")
                .setUserMappingFile(userMappingFile.toFile());

        assertFullMapping(properties, expected);
    }
}
