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

import javax.inject.Inject;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public class PrestoAzureConfigurationInitializer
        implements ConfigurationInitializer
{
    private final Optional<String> wasbAccessKey;
    private final Optional<String> wasbStorageAccount;

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
    }

    @Override
    public void initializeConfiguration(Configuration config)
    {
        if (wasbAccessKey.isPresent() && wasbStorageAccount.isPresent()) {
            config.set(format("fs.azure.account.key.%s.blob.core.windows.net", wasbStorageAccount.get()), wasbAccessKey.get());
        }
    }
}
