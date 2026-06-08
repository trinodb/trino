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

import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.gcs.analyticscore.core.GoogleCloudStorageInputStream;
import com.google.cloud.storage.Storage;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.encryption.EncryptionKey;

import java.io.EOFException;
import java.io.IOException;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.filesystem.gcs.GcsUtils.findEncryptionKey;
import static io.trino.filesystem.gcs.GcsUtils.handleGcsException;
import static io.trino.filesystem.gcs.GcsUtils.mapFileNotFound;
import static io.trino.filesystem.gcs.GcsUtils.openGcsInputStream;
import static io.trino.filesystem.gcs.GcsUtils.pathHasSpecialCharacters;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

final class AnalyticsCoreGcsInput
        implements TrinoInput
{
    private final GcsLocation location;
    private final Storage storage;
    private final GcsFileSystem gcsFileSystem;
    private final boolean useSpecialCharactersFallback;

    // Lazily initialized and reused across reads to avoid stream creation overhead
    private GoogleCloudStorageInputStream cachedStream;
    private boolean closed;

    public AnalyticsCoreGcsInput(GcsLocation location, Storage storage, GcsFileSystem gcsFileSystem)
    {
        this.location = requireNonNull(location, "location is null");
        this.storage = requireNonNull(storage, "storage is null");
        this.gcsFileSystem = requireNonNull(gcsFileSystem, "gcsFileSystem is null");
        // Check once at construction - path won't change
        this.useSpecialCharactersFallback = pathHasSpecialCharacters(location.path());
    }

    @Override
    public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        ensureOpen();
        if (position < 0) {
            throw new IOException("Negative seek offset");
        }
        checkFromIndexSize(bufferOffset, bufferLength, buffer.length);
        if (bufferLength == 0) {
            return;
        }

        // Fall back to legacy GCS API for paths with special characters
        // The gcs-analytics-core library internally uses URI.create() which fails for these
        if (useSpecialCharactersFallback) {
            try (GcsInput fallback = new GcsInput(location, storage, OptionalLong.empty(), getFallbackEncryptionKey())) {
                fallback.readFully(position, buffer, bufferOffset, bufferLength);
                return;
            }
        }

        try {
            getOrCreateStream().readFully(position, buffer, bufferOffset, bufferLength);
        }
        catch (EOFException e) {
            throw new EOFException("%s: %s".formatted(e.getMessage(), location));
        }
        catch (IOException e) {
            throw mapFileNotFound(e, location);
        }
        catch (RuntimeException e) {
            throw handleGcsException(e, "reading file", location);
        }
    }

    @Override
    public int readTail(byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        ensureOpen();
        checkFromIndexSize(bufferOffset, bufferLength, buffer.length);

        // Fall back to legacy GCS API for paths with special characters
        // The gcs-analytics-core library internally uses URI.create() which fails for these
        if (useSpecialCharactersFallback) {
            try (GcsInput fallback = new GcsInput(location, storage, OptionalLong.empty(), getFallbackEncryptionKey())) {
                return fallback.readTail(buffer, bufferOffset, bufferLength);
            }
        }

        try {
            return Math.max(0, getOrCreateStream().readTail(buffer, bufferOffset, bufferLength));
        }
        catch (IOException e) {
            throw mapFileNotFound(e, location);
        }
        catch (RuntimeException e) {
            throw handleGcsException(e, "reading file", location);
        }
    }

    private GoogleCloudStorageInputStream getOrCreateStream()
            throws IOException
    {
        if (cachedStream == null) {
            cachedStream = openGcsInputStream(gcsFileSystem, location);
        }
        return cachedStream;
    }

    private Optional<EncryptionKey> getFallbackEncryptionKey()
    {
        return findEncryptionKey(gcsFileSystem).map(GcsUtils::decodeKey);
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Input stream closed: " + location);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        closed = true;
        if (cachedStream != null) {
            cachedStream.close();
            cachedStream = null;
        }
    }

    @Override
    public String toString()
    {
        return location.toString();
    }
}
