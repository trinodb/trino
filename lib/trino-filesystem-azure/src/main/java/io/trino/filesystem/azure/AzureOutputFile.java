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
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.memory.context.AggregatedMemoryContext;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

class AzureOutputFile
        implements TrinoOutputFile
{
    private final AzureLocation location;
    private final BlobClient blobClient;
    private final long writeBlockSizeBytes;
    private final int maxWriteConcurrency;
    private final long maxSingleUploadSizeBytes;

    public AzureOutputFile(AzureLocation location, BlobClient blobClient, long writeBlockSizeBytes, int maxWriteConcurrency, long maxSingleUploadSizeBytes)
    {
        this.location = requireNonNull(location, "location is null");
        location.location().verifyValidFileLocation();
        this.blobClient = requireNonNull(blobClient, "blobClient is null");
        checkArgument(writeBlockSizeBytes >= 0, "writeBlockSizeBytes is negative");
        this.writeBlockSizeBytes = writeBlockSizeBytes;
        checkArgument(maxWriteConcurrency >= 0, "maxWriteConcurrency is negative");
        this.maxWriteConcurrency = maxWriteConcurrency;
        checkArgument(maxSingleUploadSizeBytes >= 0, "maxSingleUploadSizeBytes is negative");
        this.maxSingleUploadSizeBytes = maxSingleUploadSizeBytes;
    }

    public boolean exists()
    {
        return blobClient.exists();
    }

    @Override
    public OutputStream create(AggregatedMemoryContext memoryContext)
            throws IOException
    {
        // Azure can enforce that the file is not overwritten, but it only enforces this during data upload.
        // Check here and then set the stream to check again when data is uploaded just to be sure.
        if (exists()) {
            throw new FileAlreadyExistsException(location.toString());
        }
        return createOutputStream(memoryContext, false);
    }

    @Override
    public OutputStream createOrOverwrite(AggregatedMemoryContext memoryContext)
            throws IOException
    {
        return createOutputStream(memoryContext, true);
    }

    private AzureOutputStream createOutputStream(AggregatedMemoryContext memoryContext, boolean overwrite)
            throws IOException
    {
        return new AzureOutputStream(location, blobClient, overwrite, memoryContext, writeBlockSizeBytes, maxWriteConcurrency, maxSingleUploadSizeBytes);
    }

    @Override
    public Location location()
    {
        return location.location();
    }
}
