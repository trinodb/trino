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

import com.google.cloud.gcs.analyticscore.client.GcsFileInfo;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.encryption.EncryptionKey;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.filesystem.gcs.GcsUtils.blobGetOptionsFromEncodedKey;
import static io.trino.filesystem.gcs.GcsUtils.findEncryptionKey;
import static io.trino.filesystem.gcs.GcsUtils.getBlob;
import static io.trino.filesystem.gcs.GcsUtils.getBlobOrThrow;
import static io.trino.filesystem.gcs.GcsUtils.getGcsFileInfoOrThrow;
import static io.trino.filesystem.gcs.GcsUtils.handleGcsException;
import static io.trino.filesystem.gcs.GcsUtils.pathHasSpecialCharacters;
import static java.util.Objects.requireNonNull;

final class AnalyticsCoreGcsInputFile
        implements TrinoInputFile
{
    private final GcsLocation location;
    private final Storage storage;
    private final GcsFileSystem gcsFileSystem;
    private final OptionalLong predeclaredLength;
    private final int readBlockSizeBytes;
    private OptionalLong length;
    private Optional<Instant> lastModified;

    public AnalyticsCoreGcsInputFile(GcsLocation location, Storage storage, GcsFileSystem gcsFileSystem, OptionalLong predeclaredLength, Optional<Instant> lastModified, int readBlockSizeBytes)
    {
        this.location = requireNonNull(location, "location is null");
        this.storage = requireNonNull(storage, "storage is null");
        this.gcsFileSystem = requireNonNull(gcsFileSystem, "gcsFileSystem is null");
        this.predeclaredLength = requireNonNull(predeclaredLength, "length is null");
        this.readBlockSizeBytes = readBlockSizeBytes;
        this.length = OptionalLong.empty();
        this.lastModified = requireNonNull(lastModified, "lastModified is null");
    }

    @Override
    public TrinoInput newInput()
            throws IOException
    {
        return new AnalyticsCoreGcsInput(location, storage, gcsFileSystem);
    }

    @Override
    public TrinoInputStream newStream()
            throws IOException
    {
        // Fall back to legacy GCS API for paths with special characters
        // The gcs-analytics-core library internally uses URI.create() which fails for these
        if (pathHasSpecialCharacters(location.path())) {
            Blob blob = getBlobOrThrow(storage, location, blobGetOptionsFromEncodedKey(findEncryptionKey(gcsFileSystem)));
            return new GcsInputStream(location, blob, readBlockSizeBytes, predeclaredLength, getFallbackEncryptionKey());
        }
        GcsFileInfo fileInfo = getGcsFileInfoOrThrow(gcsFileSystem, location);
        return new AnalyticsCoreGcsInputStream(location, storage, fileInfo, gcsFileSystem, predeclaredLength);
    }

    private Optional<EncryptionKey> getFallbackEncryptionKey()
    {
        return findEncryptionKey(gcsFileSystem).map(GcsUtils::decodeKey);
    }

    @Override
    public long length()
            throws IOException
    {
        if (predeclaredLength.isPresent()) {
            return predeclaredLength.getAsLong();
        }
        if (length.isEmpty()) {
            loadProperties();
        }
        return length.orElseThrow();
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
    public boolean exists()
            throws IOException
    {
        Optional<Blob> blob = getBlob(storage, location, blobGetOptionsFromEncodedKey(findEncryptionKey(gcsFileSystem)));
        return blob.isPresent() && blob.get().exists();
    }

    @Override
    public Location location()
    {
        return location.location();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("location", location)
                .add("predeclaredLength", predeclaredLength)
                .add("length", length)
                .add("lastModified", lastModified)
                .toString();
    }

    private void loadProperties()
            throws IOException
    {
        // Fall back to legacy GCS API for paths with special characters
        // The gcs-analytics-core library internally uses URI.create() which fails for these
        if (pathHasSpecialCharacters(location.path())) {
            loadPropertiesFromBlob();
            return;
        }

        GcsFileInfo fileInfo = getGcsFileInfoOrThrow(gcsFileSystem, location);
        try {
            length = OptionalLong.of(fileInfo.getItemInfo().getSize());
            if (lastModified.isEmpty()) {
                Blob blob = getBlobOrThrow(
                        storage,
                        location,
                        blobGetOptionsFromEncodedKey(findEncryptionKey(gcsFileSystem)));
                lastModified = Optional.of(Instant.from(blob.getUpdateTimeOffsetDateTime()));
            }
        }
        catch (RuntimeException e) {
            throw handleGcsException(e, "fetching properties for file", location);
        }
    }

    private void loadPropertiesFromBlob()
            throws IOException
    {
        Blob blob = getBlobOrThrow(
                storage,
                location,
                blobGetOptionsFromEncodedKey(findEncryptionKey(gcsFileSystem)));
        try {
            length = OptionalLong.of(blob.getSize());
            if (lastModified.isEmpty()) {
                lastModified = Optional.of(Instant.from(blob.getUpdateTimeOffsetDateTime()));
            }
        }
        catch (RuntimeException e) {
            throw handleGcsException(e, "fetching properties for file", location);
        }
    }
}
