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

import com.google.common.net.HostAndPort;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;

import java.util.Optional;

public class HiveAzureConfig
{
    private String wasbStorageAccount;
    private String wasbAccessKey;
    private String abfsStorageAccount;
    private String abfsAccessKey;
    private String adlClientId;
    private String adlCredential;
    private String adlRefreshUrl;
    private HostAndPort adlProxyHost;
    private String abfsOAuthClientEndpoint;
    private String abfsOAuthClientId;
    private String abfsOAuthClientSecret;

    @Deprecated(forRemoval = true, since = "470")
    public Optional<String> getWasbStorageAccount()
    {
        return Optional.ofNullable(wasbStorageAccount);
    }

    @ConfigSecuritySensitive
    @Deprecated(forRemoval = true, since = "470")
    @Config("hive.azure.wasb-storage-account")
    public HiveAzureConfig setWasbStorageAccount(String wasbStorageAccount)
    {
        this.wasbStorageAccount = wasbStorageAccount;
        return this;
    }

    @Deprecated(forRemoval = true, since = "470")
    public Optional<String> getWasbAccessKey()
    {
        return Optional.ofNullable(wasbAccessKey);
    }

    @ConfigSecuritySensitive
    @Deprecated(forRemoval = true, since = "470")
    @Config("hive.azure.wasb-access-key")
    public HiveAzureConfig setWasbAccessKey(String wasbAccessKey)
    {
        this.wasbAccessKey = wasbAccessKey;
        return this;
    }

    @Deprecated
    public Optional<String> getAbfsStorageAccount()
    {
        return Optional.ofNullable(abfsStorageAccount);
    }

    @ConfigSecuritySensitive
    @Deprecated(forRemoval = true, since = "470")
    @Config("hive.azure.abfs-storage-account")
    public HiveAzureConfig setAbfsStorageAccount(String abfsStorageAccount)
    {
        this.abfsStorageAccount = abfsStorageAccount;
        return this;
    }

    @Deprecated(forRemoval = true, since = "470")
    public Optional<String> getAbfsAccessKey()
    {
        return Optional.ofNullable(abfsAccessKey);
    }

    @ConfigSecuritySensitive
    @Deprecated(forRemoval = true, since = "470")
    @Config("hive.azure.abfs-access-key")
    public HiveAzureConfig setAbfsAccessKey(String abfsAccessKey)
    {
        this.abfsAccessKey = abfsAccessKey;
        return this;
    }

    @ConfigSecuritySensitive
    @Deprecated(forRemoval = true, since = "470")
    @Config("hive.azure.adl-client-id")
    public HiveAzureConfig setAdlClientId(String adlClientId)
    {
        this.adlClientId = adlClientId;
        return this;
    }

    @Deprecated(forRemoval = true, since = "470")
    public Optional<String> getAdlClientId()
    {
        return Optional.ofNullable(adlClientId);
    }

    @ConfigSecuritySensitive
    @Deprecated(forRemoval = true, since = "470")
    @Config("hive.azure.adl-credential")
    public HiveAzureConfig setAdlCredential(String adlCredential)
    {
        this.adlCredential = adlCredential;
        return this;
    }

    @Deprecated(forRemoval = true, since = "470")
    public Optional<String> getAdlCredential()
    {
        return Optional.ofNullable(adlCredential);
    }

    @Deprecated(forRemoval = true, since = "470")
    public Optional<String> getAdlRefreshUrl()
    {
        return Optional.ofNullable(adlRefreshUrl);
    }

    @ConfigSecuritySensitive
    @Deprecated(forRemoval = true, since = "470")
    @Config("hive.azure.adl-refresh-url")
    public HiveAzureConfig setAdlRefreshUrl(String adlRefreshUrl)
    {
        this.adlRefreshUrl = adlRefreshUrl;
        return this;
    }

    @Deprecated(forRemoval = true, since = "470")
    @Config("hive.azure.adl-proxy-host")
    public HiveAzureConfig setAdlProxyHost(HostAndPort adlProxyHost)
    {
        this.adlProxyHost = adlProxyHost;
        return this;
    }

    @Deprecated(forRemoval = true, since = "470")
    public Optional<HostAndPort> getAdlProxyHost()
    {
        return Optional.ofNullable(adlProxyHost);
    }

    @ConfigSecuritySensitive
    @Deprecated(forRemoval = true, since = "470")
    @Config("hive.azure.abfs.oauth.endpoint")
    public HiveAzureConfig setAbfsOAuthClientEndpoint(String endpoint)
    {
        abfsOAuthClientEndpoint = endpoint;
        return this;
    }

    @Deprecated(forRemoval = true, since = "470")
    public Optional<String> getAbfsOAuthClientEndpoint()
    {
        return Optional.ofNullable(abfsOAuthClientEndpoint);
    }

    @ConfigSecuritySensitive
    @Deprecated(forRemoval = true, since = "470")
    @Config("hive.azure.abfs.oauth.client-id")
    public HiveAzureConfig setAbfsOAuthClientId(String id)
    {
        abfsOAuthClientId = id;
        return this;
    }

    @Deprecated(forRemoval = true, since = "470")
    public Optional<String> getAbfsOAuthClientId()
    {
        return Optional.ofNullable(abfsOAuthClientId);
    }

    @ConfigSecuritySensitive
    @Deprecated(forRemoval = true, since = "470")
    @Config("hive.azure.abfs.oauth.secret")
    public HiveAzureConfig setAbfsOAuthClientSecret(String secret)
    {
        abfsOAuthClientSecret = secret;
        return this;
    }

    @Deprecated(forRemoval = true, since = "470")
    public Optional<String> getAbfsOAuthClientSecret()
    {
        return Optional.ofNullable(abfsOAuthClientSecret);
    }
}
