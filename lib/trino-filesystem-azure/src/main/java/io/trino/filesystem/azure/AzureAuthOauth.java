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

import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.UserDelegationKey;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.google.inject.Inject;

import java.time.OffsetDateTime;

public final class AzureAuthOauth
        implements AzureAuth
{
    private final ClientSecretCredential credential;
    private final AzureUserDelegationKeyCache userDelegationKeyCache = new AzureUserDelegationKeyCache();

    @Inject
    public AzureAuthOauth(AzureAuthOAuthConfig config)
    {
        this(config.getClientEndpoint(), config.getTenantId(), config.getClientId(), config.getClientSecret());
    }

    public AzureAuthOauth(String clientEndpoint, String tenantId, String clientId, String clientSecret)
    {
        credential = new ClientSecretCredentialBuilder()
                .authorityHost(clientEndpoint)
                .tenantId(tenantId)
                .clientId(clientId)
                .clientSecret(clientSecret)
                .build();
    }

    public UserDelegationKey userDelegationKey(BlobServiceClient client, OffsetDateTime startTime, OffsetDateTime expiryTime)
    {
        return userDelegationKeyCache.get(client.getAccountUrl(), startTime, expiryTime, client::getUserDelegationKey);
    }

    @Override
    public void setAuth(String storageAccount, BlobContainerClientBuilder builder)
    {
        builder.credential(credential);
    }

    @Override
    public void setAuth(String storageAccount, DataLakeServiceClientBuilder builder)
    {
        builder.credential(credential);
    }
}
