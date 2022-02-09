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
import static io.trino.server.security.KerberosNameType.HOSTBASED_SERVICE;
import static io.trino.server.security.KerberosNameType.USER_NAME;

public class TestKerberosConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(KerberosConfig.class)
                .setKerberosConfig(null)
                .setServiceName(null)
                .setKeytab(null)
                .setPrincipalHostname(null)
                .setNameType(HOSTBASED_SERVICE)
                .setUserMappingPattern(null)
                .setUserMappingFile(null));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Path krbConfigFile = Files.createTempFile(null, null);
        Path keytabFile = Files.createTempFile(null, null);
        Path userMappingFile = Files.createTempFile(null, null);

        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("http.authentication.krb5.config", krbConfigFile.toString())
                .put("http-server.authentication.krb5.service-name", "airlift")
                .put("http-server.authentication.krb5.keytab", keytabFile.toString())
                .put("http-server.authentication.krb5.principal-hostname", "test.example.com")
                .put("http-server.authentication.krb5.name-type", "USER_NAME")
                .put("http-server.authentication.krb5.user-mapping.pattern", "(.*)@something")
                .put("http-server.authentication.krb5.user-mapping.file", userMappingFile.toString())
                .buildOrThrow();

        KerberosConfig expected = new KerberosConfig()
                .setKerberosConfig(krbConfigFile.toFile())
                .setServiceName("airlift")
                .setKeytab(keytabFile.toFile())
                .setPrincipalHostname("test.example.com")
                .setNameType(USER_NAME)
                .setUserMappingPattern("(.*)@something")
                .setUserMappingFile(userMappingFile.toFile());

        assertFullMapping(properties, expected);
    }
}
