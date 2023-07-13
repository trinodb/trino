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
package io.trino.hdfs.azure;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestHiveAzureConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(HiveAzureConfig.class)
                .setWasbAccessKey(null)
                .setWasbStorageAccount(null)
                .setAbfsStorageAccount(null)
                .setAbfsAccessKey(null)
                .setAdlClientId(null)
                .setAdlCredential(null)
                .setAdlProxyHost(null)
                .setAdlRefreshUrl(null)
                .setAbfsOAuthClientEndpoint(null)
                .setAbfsOAuthClientId(null)
                .setAbfsOAuthClientSecret(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.azure.wasb-storage-account", "testwasbstorage")
                .put("hive.azure.wasb-access-key", "secret")
                .put("hive.azure.abfs-storage-account", "abfsstorage")
                .put("hive.azure.abfs-access-key", "abfssecret")
                .put("hive.azure.adl-client-id", "adlclientid")
                .put("hive.azure.adl-credential", "adlcredential")
                .put("hive.azure.adl-refresh-url", "adlrefreshurl")
                .put("hive.azure.adl-proxy-host", "proxy-host:9800")
                .put("hive.azure.abfs.oauth.endpoint", "abfsoauthendpoint")
                .put("hive.azure.abfs.oauth.client-id", "abfsoauthclientid")
                .put("hive.azure.abfs.oauth.secret", "abfsoauthsecret")
                .buildOrThrow();

        HiveAzureConfig expected = new HiveAzureConfig()
                .setWasbStorageAccount("testwasbstorage")
                .setWasbAccessKey("secret")
                .setAbfsStorageAccount("abfsstorage")
                .setAbfsAccessKey("abfssecret")
                .setAdlClientId("adlclientid")
                .setAdlCredential("adlcredential")
                .setAdlRefreshUrl("adlrefreshurl")
                .setAdlProxyHost(HostAndPort.fromParts("proxy-host", 9800))
                .setAbfsOAuthClientEndpoint("abfsoauthendpoint")
                .setAbfsOAuthClientId("abfsoauthclientid")
                .setAbfsOAuthClientSecret("abfsoauthsecret");

        assertFullMapping(properties, expected);
    }
}
