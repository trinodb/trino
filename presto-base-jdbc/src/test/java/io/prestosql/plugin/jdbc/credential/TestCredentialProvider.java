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
package io.prestosql.plugin.jdbc.credential;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.bootstrap.Bootstrap;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static org.testng.Assert.assertEquals;

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
        assertEquals(credentialProvider.getConnectionUser(Optional.empty()).get(), "user_from_inline");
        assertEquals(credentialProvider.getConnectionPassword(Optional.empty()).get(), "password_for_user_from_inline");
    }

    @Test
    public void testFileCredentialProvider()
    {
        Map<String, String> properties = ImmutableMap.of(
                "connection-url", "jdbc:h2:mem:config",
                "credential-provider.type", "FILE",
                "connection-credential-file", getResourceFilePath("credentials.properties"));

        CredentialProvider credentialProvider = getCredentialProvider(properties);
        assertEquals(credentialProvider.getConnectionUser(Optional.empty()).get(), "user_from_file");
        assertEquals(credentialProvider.getConnectionPassword(Optional.empty()).get(), "password_for_user_from_file");
    }

    private static CredentialProvider getCredentialProvider(Map<String, String> properties)
    {
        return new Bootstrap(ImmutableList.of(new CredentialProviderModule()))
                .setOptionalConfigurationProperties(properties)
                .initialize()
                .getInstance(CredentialProvider.class);
    }

    private String getResourceFilePath(String fileName)
    {
        return this.getClass().getClassLoader().getResource(fileName).getPath();
    }
}
