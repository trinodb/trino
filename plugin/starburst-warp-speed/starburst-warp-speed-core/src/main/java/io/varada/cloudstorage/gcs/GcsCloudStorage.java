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
package io.varada.cloudstorage.gcs;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import io.trino.filesystem.Location;
import io.trino.filesystem.gcs.GcsFileSystemFactory;
import io.varada.cloudstorage.CloudStorageService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

import static com.google.api.client.util.Preconditions.checkState;
import static io.varada.cloudstorage.gcs.GcsUtils.getBlob;
import static io.varada.cloudstorage.gcs.GcsUtils.getBlobId;
import static java.util.Objects.requireNonNull;

public class GcsCloudStorage
        extends CloudStorageService
{
    private final Storage storage;

    public GcsCloudStorage(GcsFileSystemFactory fileSystemFactory, Storage storage)
    {
        super(fileSystemFactory);
        this.storage = requireNonNull(storage, "storage is null");
    }

    @Override
    public void uploadFile(Location source, Location target)
            throws IOException
    {
        GcsLocation gcsLocation = new GcsLocation(target);
        checkIsValidFile(gcsLocation);

        BlobInfo blobInfo = BlobInfo.newBuilder(getBlobId(gcsLocation)).build();
        Optional<Blob> blob = getBlob(storage, gcsLocation);

        Storage.BlobWriteOption precondition = blob.map(value -> Storage.BlobWriteOption.generationMatch(value.getGeneration()))
                .orElseGet(Storage.BlobWriteOption::doesNotExist);

        storage.createFrom(blobInfo, Path.of(source.toString()), precondition);
    }

    @Override
    public void downloadFile(Location source, Location target)
    {
        GcsLocation gcsLocation = new GcsLocation(source);
        checkIsValidFile(gcsLocation);
        getBlob(storage, gcsLocation).ifPresent(blob -> blob.downloadTo(Path.of(target.toString())));
    }

    @Override
    public void copyFile(Location source, Location destination)
    {
        GcsLocation sourceLocation = new GcsLocation(source);
        GcsLocation targetLocation = new GcsLocation(destination);

        checkIsValidFile(sourceLocation);
        checkIsValidFile(targetLocation);

        getBlob(storage, sourceLocation).ifPresent(blob -> blob.copyTo(getBlobId(targetLocation)));
    }

    @Override
    public void renameFile(Location source, Location target)
            throws IOException
    {
        copyFile(source, target);
        deleteFile(source);
    }

    private static void checkIsValidFile(GcsLocation gcsLocation)
    {
        checkState(!gcsLocation.path().isEmpty(), "Location path is empty: %s", gcsLocation);
        checkState(!gcsLocation.path().endsWith("/"), "Location path ends with a slash: %s", gcsLocation);
    }
}
