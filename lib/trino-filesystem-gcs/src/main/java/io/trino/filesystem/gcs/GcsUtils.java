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
package io.trino.filesystem.gcs;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import io.trino.filesystem.Location;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.api.client.util.Preconditions.checkArgument;
import static com.google.cloud.storage.Blob.BlobSourceOption.shouldReturnRawInputStream;
import static java.util.Objects.requireNonNull;

public class GcsUtils
{
    private GcsUtils() {}

    public static IOException handleGcsException(RuntimeException exception, String action, GcsLocation location)
            throws IOException
    {
        throw new IOException("Error %s: %s".formatted(action, location), exception);
    }

    public static IOException handleGcsException(RuntimeException exception, String action, Collection<Location> locations)
            throws IOException
    {
        throw new IOException("Error %s: %s".formatted(action, locations), exception);
    }

    public static ReadChannel getReadChannel(Blob blob, GcsLocation location, long position, int readBlockSize, OptionalLong limit)
            throws IOException
    {
        long fileSize = requireNonNull(blob.getSize(), "blob size is null");
        if (position >= fileSize) {
            throw new IOException("Cannot read at %s. File size is %s: %s".formatted(position, fileSize, location));
        }
        // Enable shouldReturnRawInputStream: currently set by default but just to ensure the behavior is predictable
        ReadChannel readChannel = blob.reader(shouldReturnRawInputStream(true));

        readChannel.setChunkSize(readBlockSize);
        readChannel.seek(position);
        if (limit.isPresent()) {
            return readChannel.limit(limit.getAsLong());
        }
        return readChannel;
    }

    public static Optional<Blob> getBlob(Storage storage, GcsLocation location, Storage.BlobGetOption... blobGetOptions)
    {
        checkArgument(!location.path().isEmpty(), "Path for location %s is empty", location);
        return Optional.ofNullable(storage.get(BlobId.of(location.bucket(), location.path()), blobGetOptions));
    }

    public static Blob getBlobOrThrow(Storage storage, GcsLocation location, Storage.BlobGetOption... blobGetOptions)
            throws IOException
    {
        return getBlob(storage, location, blobGetOptions).orElseThrow(() -> new FileNotFoundException("File %s not found".formatted(location)));
    }
}
