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
package io.trino.plugin.jdbc.credential;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.bootstrap.Bootstrap;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Optional;

import static com.google.common.io.Resources.getResource;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCredentialProvider
{
    @Test
    public void testInlineCredentialProvider()
    {
        Map<String, String> properties = ImmutableMap.of(
                "connection-url", "jdbc:h2:mem:config",
                "connection-user", "user_from_inline",
                "connection-password", "password_for_user_from_inline");

        CredentialProvider credentialProvider = getCredentialProvider(properties);
        assertThat(credentialProvider.getConnectionUser(Optional.empty()).get()).isEqualTo("user_from_inline");
        assertThat(credentialProvider.getConnectionPassword(Optional.empty()).get()).isEqualTo("password_for_user_from_inline");
    }

    @Test
    public void testFileCredentialProvider()
    {
        Map<String, String> properties = ImmutableMap.of(
                "connection-url", "jdbc:h2:mem:config",
                "credential-provider.type", "FILE",
                "connection-credential-file", getResourceFilePath("credentials.properties"));

        CredentialProvider credentialProvider = getCredentialProvider(properties);
        assertThat(credentialProvider.getConnectionUser(Optional.empty()).get()).isEqualTo("user_from_file");
        assertThat(credentialProvider.getConnectionPassword(Optional.empty()).get()).isEqualTo("password_for_user_from_file");
    }

    @Test
    public void testKeyStoreBasedCredentialProvider()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("connection-url", "jdbc:h2:mem:config")
                .put("credential-provider.type", "KEYSTORE")
                .put("keystore-file-path", getResourceFilePath("credentials.jceks"))
                .put("keystore-type", "JCEKS")
                .put("keystore-password", "keystore_password")
                .put("keystore-user-credential-name", "userName")
                .put("keystore-user-credential-password", "keystore_password_for_user_name")
                .put("keystore-password-credential-name", "password")
                .put("keystore-password-credential-password", "keystore_password_for_password")
                .buildOrThrow();

        CredentialProvider credentialProvider = getCredentialProvider(properties);
        assertThat(credentialProvider.getConnectionUser(Optional.empty()).get()).isEqualTo("user_from_keystore");
        assertThat(credentialProvider.getConnectionPassword(Optional.empty()).get()).isEqualTo("password_from_keystore");
    }

    private CredentialProvider getCredentialProvider(Map<String, String> properties)
    {
        return new Bootstrap(ImmutableList.of(new CredentialProviderModule()))
                .setOptionalConfigurationProperties(properties)
                .initialize()
                .getInstance(CredentialProvider.class);
    }

    private String getResourceFilePath(String fileName)
    {
        try {
            return new File(getResource(fileName).toURI()).getPath();
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
