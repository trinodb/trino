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

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.models.BlobProperties;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.filesystem.azure.AzureUtils.handleAzureException;
import static java.util.Objects.requireNonNull;

class AzureInputFile
        implements TrinoInputFile
{
    private final AzureLocation location;
    private final BlobClient blobClient;
    private final int readBlockSizeBytes;

    private OptionalLong length;
    private Optional<Instant> lastModified = Optional.empty();

    public AzureInputFile(AzureLocation location, OptionalLong length, BlobClient blobClient, int readBlockSizeBytes)
    {
        this.location = requireNonNull(location, "location is null");
        location.location().verifyValidFileLocation();
        this.length = requireNonNull(length, "length is null");
        this.blobClient = requireNonNull(blobClient, "blobClient is null");
        checkArgument(readBlockSizeBytes >= 0, "readBlockSizeBytes is negative");
        this.readBlockSizeBytes = readBlockSizeBytes;
    }

    @Override
    public Location location()
    {
        return location.location();
    }

    @Override
    public boolean exists()
    {
        return blobClient.exists();
    }

    @Override
    public TrinoInputStream newStream()
            throws IOException
    {
        return new AzureInputStream(location, blobClient, readBlockSizeBytes);
    }

    @Override
    public TrinoInput newInput()
            throws IOException
    {
        try {
            return new AzureInput(location, blobClient, readBlockSizeBytes, length);
        }
        catch (RuntimeException e) {
            throw handleAzureException(e, "opening file", location);
        }
    }

    @Override
    public Instant lastModified()
            throws IOException
    {
        if (lastModified.isEmpty()) {
            loadProperties();
        }
        return lastModified.orElseThrow();
    }

    @Override
    public long length()
            throws IOException
    {
        if (length.isEmpty()) {
            loadProperties();
        }
        return length.orElseThrow();
    }

    private void loadProperties()
            throws IOException
    {
        BlobProperties properties;
        try {
            properties = blobClient.getProperties();
        }
        catch (RuntimeException e) {
            throw handleAzureException(e, "fetching properties for file", location);
        }
        if (length.isEmpty()) {
            length = OptionalLong.of(properties.getBlobSize());
        }
        if (lastModified.isEmpty()) {
            lastModified = Optional.of(properties.getLastModified().toInstant());
        }
    }
}
