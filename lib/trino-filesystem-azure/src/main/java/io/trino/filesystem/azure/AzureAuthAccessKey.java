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
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.google.inject.Inject;

import static java.util.Objects.requireNonNull;

public class AzureAuthAccessKey
        implements AzureAuth
{
    private final String accessKey;

    @Inject
    public AzureAuthAccessKey(AzureAuthAccessKeyConfig config)
    {
        this(config.getAccessKey());
    }

    public AzureAuthAccessKey(String accessKey)
    {
        this.accessKey = requireNonNull(accessKey, "accessKey is null");
    }

    @Override
    public void setAuth(String storageAccount, BlobContainerClientBuilder builder)
    {
        builder.credential(new StorageSharedKeyCredential(storageAccount, accessKey));
    }

    @Override
    public void setAuth(String storageAccount, DataLakeServiceClientBuilder builder)
    {
        builder.credential(new StorageSharedKeyCredential(storageAccount, accessKey));
    }
}
