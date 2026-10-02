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
package io.trino.filesystem.azure;

import com.azure.core.client.traits.AzureSasCredentialTrait;
import com.azure.core.client.traits.HttpTrait;
import com.azure.core.credential.AzureSasCredential;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public final class AzureAuthSasToken
        implements AzureAuth
{
    private final Map<String, String> sasTokens;
    private final Optional<Supplier<Map<String, String>>> credentialsRefresher;

    public AzureAuthSasToken(Map<String, String> sasTokens)
    {
        this(sasTokens, Optional.empty());
    }

    public AzureAuthSasToken(Map<String, String> sasTokens, Optional<Supplier<Map<String, String>>> credentialsRefresher)
    {
        this.sasTokens = ImmutableMap.copyOf(sasTokens);
        this.credentialsRefresher = requireNonNull(credentialsRefresher, "credentialsRefresher is null");
    }

    @Override
    public void setAuth(String storageAccount, BlobContainerClientBuilder builder)
    {
        if (credentialsRefresher.isPresent()) {
            installCredentialRefresher(storageAccount, builder, credentialsRefresher.get());
        }
        else {
            builder.sasToken(sasToken(storageAccount));
        }
    }

    @Override
    public void setAuth(String storageAccount, DataLakeServiceClientBuilder builder)
    {
        if (credentialsRefresher.isPresent()) {
            installCredentialRefresher(storageAccount, builder, credentialsRefresher.get());
        }
        else {
            builder.sasToken(sasToken(storageAccount));
        }
    }

    private <T extends HttpTrait<T> & AzureSasCredentialTrait<T>> void installCredentialRefresher(String storageAccount, T builder, Supplier<Map<String, String>> credentialsRefresher)
    {
        AzureSasCredential credential = new AzureSasCredential(sasToken(storageAccount));
        builder.credential(credential);
        builder.addPolicy(new SasTokenRefreshHttpPipelinePolicy(storageAccount, credential, credentialsRefresher));
    }

    private String sasToken(String storageAccount)
    {
        String sasToken = sasTokens.get(storageAccount);
        if (sasToken == null) {
            throw new IllegalStateException("No SAS token provided for storage account: " + storageAccount);
        }
        return sasToken;
    }
}
