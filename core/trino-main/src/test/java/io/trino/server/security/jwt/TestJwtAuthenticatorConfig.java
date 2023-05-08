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
package io.trino.server.security.jwt;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;

public class TestJwtAuthenticatorConfig
{
    //TODO: remove unrelated code after deprecated properties are removed.
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(JwtAuthenticatorConfig.class)
                .setKeyFile(null)
                .setRequiredAudience(null)
                .setRequiredIssuer(null)
                .setPrincipalField("sub")
                .setUserMappingPattern(null)
                .setUserMappingFile(null)
                .setJwtIdpConfigFiles(""));
    }

    //TODO: remove unrelated code after deprecated properties are removed.
    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Path jwtKeyFile = Files.createTempFile(null, null);
        Path userMappingFile = Files.createTempFile(null, null);
        Path idpConfig1 = Files.createTempFile("azuread_idp", ".properties");
        Path idpConfig2 = Files.createTempFile("keycloak_idp", ".properties");

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("http-server.authentication.jwt.key-file", jwtKeyFile.toString())
                .put("http-server.authentication.jwt.required-audience", "some-audience")
                .put("http-server.authentication.jwt.required-issuer", "some-issuer")
                .put("http-server.authentication.jwt.principal-field", "some-field")
                .put("http-server.authentication.jwt.user-mapping.pattern", "(.*)@something")
                .put("http-server.authentication.jwt.user-mapping.file", userMappingFile.toString())
                .put("http-server.authentication.jwt.config-files", idpConfig1.toString() + "," + idpConfig2.toString())
                .buildOrThrow();

        JwtAuthenticatorConfig expected = new JwtAuthenticatorConfig()
                .setKeyFile(jwtKeyFile.toString())
                .setRequiredAudience("some-audience")
                .setRequiredIssuer("some-issuer")
                .setPrincipalField("some-field")
                .setUserMappingPattern("(.*)@something")
                .setUserMappingFile(userMappingFile.toFile())
                .setUserMappingFile(userMappingFile.toFile())
                .setJwtIdpConfigFiles(ImmutableList.of(idpConfig1.toFile(), idpConfig2.toFile()));

        assertFullMapping(properties, expected);
    }
}
