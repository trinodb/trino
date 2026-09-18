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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.common.collect.ImmutableMap;

import java.time.Instant;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import static io.trino.filesystem.azure.AzureFileSystemConstants.EXTRA_CREDENTIALS_AZURE_SAS_TOKEN_PREFIX;
import static java.util.Objects.requireNonNull;

record AzureVendedCredentials(Map<String, String> sasTokens, Optional<Instant> expirationTime)
        implements VendedCredentials
{
    public AzureVendedCredentials
    {
        sasTokens = ImmutableMap.copyOf(sasTokens);
        requireNonNull(expirationTime, "expirationTime is null");
    }

    @Override
    public Map<String, String> toExtraCredentials()
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (Entry<String, String> entry : sasTokens.entrySet()) {
            builder.put(EXTRA_CREDENTIALS_AZURE_SAS_TOKEN_PREFIX + entry.getKey(), entry.getValue());
        }
        return builder.buildOrThrow();
    }

    @Override
    public Optional<Instant> expiresAt()
    {
        return expirationTime;
    }

    /**
     * Returns the SAS token for the given storage account, or {@code null} if none is present.
     */
    public String get(String storageAccount)
    {
        return sasTokens.get(storageAccount);
    }
}
