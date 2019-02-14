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
package io.prestosql.plugin.hive.azure;

import io.prestosql.plugin.hive.ConfigurationInitializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.adl.AdlFileSystem;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;

import javax.inject.Inject;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public class PrestoAzureConfigurationInitializer
        implements ConfigurationInitializer
{
    private final Optional<String> wasbAccessKey;
    private final Optional<String> wasbStorageAccount;
    private final Optional<String> adlClientId;
    private final Optional<String> adlCredential;
    private final Optional<String> adlRefreshUrl;
    private final Optional<String> abfsAccessKey;
    private final Optional<String> abfsStorageAccount;

    @Inject
    public PrestoAzureConfigurationInitializer(HiveAzureConfig hiveAzureConfig)
    {
        this.wasbAccessKey = hiveAzureConfig.getWasbAccessKey();
        this.wasbStorageAccount = hiveAzureConfig.getWasbStorageAccount();
        if (wasbAccessKey.isPresent() || wasbStorageAccount.isPresent()) {
            checkArgument(
                    wasbAccessKey.isPresent() && !wasbAccessKey.get().isEmpty(),
                    "hive.azure.wasb-storage-account is set, but hive.azure.wasb-access-key is not");
            checkArgument(
                    wasbStorageAccount.isPresent() && !wasbStorageAccount.get().isEmpty(),
                    "hive.azure.wasb-access-key is set, but hive.azure.wasb-storage-account is not");
        }

        this.abfsAccessKey = hiveAzureConfig.getAbfsAccessKey();
        this.abfsStorageAccount = hiveAzureConfig.getAbfsStorageAccount();
        if (abfsAccessKey.isPresent() || abfsStorageAccount.isPresent()) {
            checkArgument(
                    abfsAccessKey.isPresent() && !abfsAccessKey.get().isEmpty(),
                    "hive.azure.abfs-storage-account is set, but hive.azure.abfs-access-key is not");
            checkArgument(
                    abfsStorageAccount.isPresent() && !abfsStorageAccount.get().isEmpty(),
                    "hive.azure.abfs-access-key is set, but hive.azure.abfs-storage-account is not");
        }

        this.adlClientId = hiveAzureConfig.getAdlClientId();
        this.adlCredential = hiveAzureConfig.getAdlCredential();
        this.adlRefreshUrl = hiveAzureConfig.getAdlRefreshUrl();
        if (adlClientId.isPresent() || adlCredential.isPresent() || adlRefreshUrl.isPresent()) {
            checkArgument(adlClientId.isPresent() && !adlClientId.get().isEmpty() &&
                            adlCredential.isPresent() && !adlCredential.get().isEmpty() &&
                            adlRefreshUrl.isPresent() && !adlRefreshUrl.get().isEmpty(),
                    "If one of adlClientId, adlCredential, adlRefreshUrl is set, all must be set");
        }
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

        if (adlClientId.isPresent() && adlCredential.isPresent() && adlRefreshUrl.isPresent()) {
            config.set("fs.adl.oauth2.access.token.provider.type", "ClientCredential");
            config.set("fs.adl.oauth2.client.id", adlClientId.get());
            config.set("fs.adl.oauth2.credential", adlCredential.get());
            config.set("fs.adl.oauth2.refresh.url", adlRefreshUrl.get());
            config.set("fs.adl.impl", AdlFileSystem.class.getName());
        }
        // do not rely on information returned from local system about users and groups
        config.set("fs.azure.skipUserGroupMetadataDuringInitialization", "true");
    }
}
