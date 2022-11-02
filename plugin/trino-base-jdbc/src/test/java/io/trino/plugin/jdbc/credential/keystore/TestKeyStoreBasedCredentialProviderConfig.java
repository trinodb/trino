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
package io.trino.plugin.jdbc.credential.keystore;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestKeyStoreBasedCredentialProviderConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(KeyStoreBasedCredentialProviderConfig.class)
                .setKeyStoreFilePath(null)
                .setKeyStoreType(null)
                .setKeyStorePassword(null)
                .setUserCredentialName(null)
                .setPasswordForUserCredentialName(null)
                .setPasswordCredentialName(null)
                .setPasswordForPasswordCredentialName(null));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Path keystoreFile = Files.createTempFile(null, null);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("keystore-file-path", keystoreFile.toString())
                .put("keystore-type", "JCEKS")
                .put("keystore-password", "keystore_password")
                .put("keystore-user-credential-name", "userName")
                .put("keystore-user-credential-password", "keystore_password_for_user_name")
                .put("keystore-password-credential-name", "password")
                .put("keystore-password-credential-password", "keystore_password_for_password")
                .buildOrThrow();

        KeyStoreBasedCredentialProviderConfig expected = new KeyStoreBasedCredentialProviderConfig()
                .setKeyStoreFilePath(keystoreFile.toString())
                .setKeyStoreType("JCEKS")
                .setKeyStorePassword("keystore_password")
                .setUserCredentialName("userName")
                .setPasswordForUserCredentialName("keystore_password_for_user_name")
                .setPasswordCredentialName("password")
                .setPasswordForPasswordCredentialName("keystore_password_for_password");

        assertFullMapping(properties, expected);
    }
}
