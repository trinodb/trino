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
package io.varada.cloudstorage.azure;

import com.azure.core.http.HttpClient;
import com.azure.core.util.ClientOptions;
import com.azure.core.util.TracingOptions;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.common.Utility;
import io.trino.filesystem.Location;
import io.trino.filesystem.azure.AzureAuth;
import io.trino.filesystem.azure.AzureFileSystemFactory;
import io.varada.cloudstorage.CloudStorageService;

import java.io.IOException;

import static io.varada.cloudstorage.azure.AzureUtils.handleAzureException;
import static java.util.Objects.requireNonNull;

public class AzureCloudStorage
        extends CloudStorageService
{
    private final HttpClient httpClient;
    private final TracingOptions tracingOptions;
    private final AzureAuth azureAuth;

    public AzureCloudStorage(AzureFileSystemFactory fileSystemFactory,
                             HttpClient httpClient,
                             TracingOptions tracingOptions,
                             AzureAuth azureAuth)
    {
        super(fileSystemFactory);
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.tracingOptions = requireNonNull(tracingOptions, "tracingOptions is null");
        this.azureAuth = requireNonNull(azureAuth, "azureAuth is null");
    }

    @Override
    public void uploadFile(Location source, Location target)
            throws IOException
    {
        target.verifyValidFileLocation();

        AzureLocation destinationLocation = new AzureLocation(target);
        BlobClient client = createBlobClient(destinationLocation);

        try {
            client.uploadFromFile(source.toString(), true);
        }
        catch (RuntimeException e) {
            throw handleAzureException(e, "upload file", destinationLocation);
        }
    }

    @Override
    public void downloadFile(Location source, Location target)
            throws IOException
    {
        source.verifyValidFileLocation();

        AzureLocation sourceLocation = new AzureLocation(source);
        BlobClient client = createBlobClient(sourceLocation);

        try {
            client.downloadToFile(target.toString());
        }
        catch (RuntimeException e) {
            throw handleAzureException(e, "download file", sourceLocation);
        }
    }

    @Override
    public void copyFile(Location source, Location destination)
            throws IOException
    {
        source.verifyValidFileLocation();
        destination.verifyValidFileLocation();

        AzureLocation sourceLocation = new AzureLocation(source);
        AzureLocation targetLocation = new AzureLocation(destination);

        if (!sourceLocation.account().equals(targetLocation.account())) {
            throw new IOException("Cannot copy across storage accounts");
        }

        BlobClient sourceClient = createBlobClient(sourceLocation);
        BlobClient targetClient = createBlobClient(targetLocation);

        try {
            targetClient.copyFromUrl(sourceClient.getBlobUrl());
        }
        catch (RuntimeException e) {
            throw handleAzureException(e, "copy file", sourceLocation);
        }
    }

    private BlobClient createBlobClient(AzureLocation location)
    {
        // encode the path using the Azure url encoder utility
        String path = Utility.urlEncode(location.path());
        return createBlobContainerClient(location).getBlobClient(path);
    }

    private BlobContainerClient createBlobContainerClient(AzureLocation location)
    {
        requireNonNull(location, "location is null");

        BlobContainerClientBuilder builder = new BlobContainerClientBuilder()
                .httpClient(httpClient)
                .clientOptions(new ClientOptions().setTracingOptions(tracingOptions))
                .endpoint(String.format("https://%s.blob.core.windows.net", location.account()));
        azureAuth.setAuth(location.account(), builder);
        location.container().ifPresent(builder::containerName);
        return builder.buildClient();
    }
}
