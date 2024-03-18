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
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;

import java.util.Optional;

import static com.google.api.client.util.Preconditions.checkArgument;

public class GcsUtils
{
    private GcsUtils()
    {
    }

    public static BlobId getBlobId(GcsLocation location)
    {
        checkArgument(!location.path().isEmpty(), "Path for location %s is empty", location);
        return BlobId.of(location.bucket(), location.path());
    }

    public static Optional<Blob> getBlob(Storage storage, GcsLocation location, Storage.BlobGetOption... blobGetOptions)
    {
        return Optional.ofNullable(storage.get(getBlobId(location), blobGetOptions));
    }
}
