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
import com.google.inject.Inject;
import io.trino.hdfs.ConfigurationInitializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.adl.AdlFileSystem;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Proxy.Type;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.DATA_BLOCKS_BUFFER;
import static org.apache.hadoop.fs.store.DataBlocks.DATA_BLOCKS_BUFFER_ARRAY;

public class TrinoAzureConfigurationInitializer
        implements ConfigurationInitializer
{
    private final Optional<String> wasbAccessKey;
    private final Optional<String> wasbStorageAccount;
    private final Optional<String> adlClientId;
    private final Optional<String> adlCredential;
    private final Optional<String> adlRefreshUrl;
    private final Optional<String> abfsAccessKey;
    private final Optional<String> abfsStorageAccount;
    private final Optional<String> abfsOAuthClientEndpoint;
    private final Optional<String> abfsOAuthClientId;
    private final Optional<String> abfsOAuthClientSecret;

    @Inject
    public TrinoAzureConfigurationInitializer(HiveAzureConfig config)
    {
        this.wasbAccessKey = dropEmpty(config.getWasbAccessKey());
        this.wasbStorageAccount = dropEmpty(config.getWasbStorageAccount());
        if (wasbAccessKey.isPresent() || wasbStorageAccount.isPresent()) {
            checkArgument(
                    wasbAccessKey.isPresent() && wasbStorageAccount.isPresent(),
                    "If WASB storage account or access key is set, both must be set");
        }

        this.abfsAccessKey = dropEmpty(config.getAbfsAccessKey());
        this.abfsStorageAccount = dropEmpty(config.getAbfsStorageAccount());
        if (abfsAccessKey.isPresent() || abfsStorageAccount.isPresent()) {
            checkArgument(
                    abfsStorageAccount.isPresent() && abfsAccessKey.isPresent(),
                    "If ABFS storage account or access key is set, both must be set");
        }

        this.adlClientId = dropEmpty(config.getAdlClientId());
        this.adlCredential = dropEmpty(config.getAdlCredential());
        this.adlRefreshUrl = dropEmpty(config.getAdlRefreshUrl());
        if (adlClientId.isPresent() || adlCredential.isPresent() || adlRefreshUrl.isPresent()) {
            checkArgument(
                    adlClientId.isPresent() && adlCredential.isPresent() && adlRefreshUrl.isPresent(),
                    "If any of ADL client ID, credential, and refresh URL are set, all must be set");
        }

        this.abfsOAuthClientEndpoint = dropEmpty(config.getAbfsOAuthClientEndpoint());
        this.abfsOAuthClientId = dropEmpty(config.getAbfsOAuthClientId());
        this.abfsOAuthClientSecret = dropEmpty(config.getAbfsOAuthClientSecret());
        if (abfsOAuthClientEndpoint.isPresent() || abfsOAuthClientSecret.isPresent() || abfsOAuthClientId.isPresent()) {
            checkArgument(
                    abfsOAuthClientEndpoint.isPresent() && abfsOAuthClientId.isPresent() && abfsOAuthClientSecret.isPresent(),
                    "If any of ABFS OAuth2 Client endpoint, ID, and secret are set, all must be set.");
        }

        checkArgument(
                !(abfsAccessKey.isPresent() && abfsOAuthClientSecret.isPresent()),
                "Multiple ABFS authentication methods configured: access key and OAuth2");

        //noinspection UnnecessaryFullyQualifiedName
        config.getAdlProxyHost().ifPresent(proxyHost ->
                io.trino.hadoop.$internal.com.microsoft.azure.datalake.store.HttpTransport.setConnectionProxy(proxyForHost(proxyHost)));
    }

    @Override
    public void initializeConfiguration(Configuration config)
    {
        if (wasbAccessKey.isPresent() && wasbStorageAccount.isPresent()) {
            config.set(format("fs.azure.account.key.%s.blob.core.windows.net", wasbStorageAccount.get()), wasbAccessKey.get());
        }

        if (abfsAccessKey.isPresent() && abfsStorageAccount.isPresent()) {
            config.set(format("fs.azure.account.key.%s.dfs.core.windows.net", abfsStorageAccount.get()), abfsAccessKey.get());
            config.set("fs.abfs.impl", AzureBlobFileSystem.class.getName());
        }
        if (abfsOAuthClientEndpoint.isPresent() && abfsOAuthClientId.isPresent() && abfsOAuthClientSecret.isPresent()) {
            config.set("fs.azure.account.auth.type", "OAuth");
            config.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider");
            config.set("fs.azure.account.oauth2.client.endpoint", abfsOAuthClientEndpoint.get());
            config.set("fs.azure.account.oauth2.client.id", abfsOAuthClientId.get());
            config.set("fs.azure.account.oauth2.client.secret", abfsOAuthClientSecret.get());
        }

        if (adlClientId.isPresent() && adlCredential.isPresent() && adlRefreshUrl.isPresent()) {
            config.set("fs.adl.oauth2.access.token.provider.type", "ClientCredential");
            config.set("fs.adl.oauth2.client.id", adlClientId.get());
            config.set("fs.adl.oauth2.credential", adlCredential.get());
            config.set("fs.adl.oauth2.refresh.url", adlRefreshUrl.get());
            config.set("fs.adl.impl", AdlFileSystem.class.getName());
        }

        // do not rely on information returned from local system about users and groups
        config.set("fs.azure.skipUserGroupMetadataDuringInitialization", "true");

        // disable buffering Azure output streams to disk(default is DATA_BLOCKS_BUFFER_DISK)
        config.set(DATA_BLOCKS_BUFFER, DATA_BLOCKS_BUFFER_ARRAY);
    }

    private static Optional<String> dropEmpty(Optional<String> optional)
    {
        return optional.filter(value -> !value.isEmpty());
    }

    private static Proxy proxyForHost(HostAndPort address)
    {
        return new Proxy(Type.HTTP, new InetSocketAddress(address.getHost(), address.getPort()));
    }
}
