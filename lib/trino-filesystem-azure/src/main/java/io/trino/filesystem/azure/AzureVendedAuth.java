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

import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;

import java.util.Map;
import java.util.Optional;

public final class AzureVendedAuth
        implements AzureAuth
{
    private final Map<String, String> sasTokens;
    private final AzureAuth fallbackAuth;

    public AzureVendedAuth(Map<String, String> sasTokens, AzureAuth fallbackAuth)
    {
        this.sasTokens = sasTokens;
        this.fallbackAuth = fallbackAuth;
    }

    @Override
    public void setAuth(String storageAccount, BlobContainerClientBuilder builder)
    {
        getSasToken(storageAccount)
                .ifPresentOrElse(builder::sasToken, () -> fallbackAuth.setAuth(storageAccount, builder));
    }

    @Override
    public void setAuth(String storageAccount, DataLakeServiceClientBuilder builder)
    {
        getSasToken(storageAccount)
                .ifPresentOrElse(builder::sasToken, () -> fallbackAuth.setAuth(storageAccount, builder));
    }

    public Optional<String> getSasToken(String storageAccount)
    {
        return Optional.ofNullable(sasTokens.get(AzureFileSystemConstants.EXTRA_SAS_TOKEN_PROPERTY_PREFIX + storageAccount));
    }
}
