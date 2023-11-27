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

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;

import java.io.IOException;
import java.time.Instant;
import java.util.Iterator;
import java.util.Optional;

import static io.trino.filesystem.gcs.GcsUtils.handleGcsException;
import static java.util.Objects.requireNonNull;

public class GcsFileIterator
        implements FileIterator
{
    private final GcsLocation location;
    private Iterator<Blob> blobIterator;

    public GcsFileIterator(GcsLocation location, Page<Blob> page)
    {
        this.location = requireNonNull(location, "location is null");
        this.blobIterator = page.streamAll()
                .filter(blob -> !blob.isDirectory())
                .filter(blob -> !blob.getName().endsWith("/"))
                .iterator();
    }

    @Override
    public boolean hasNext()
            throws IOException
    {
        try {
            return blobIterator.hasNext();
        }
        catch (RuntimeException e) {
            throw handleGcsException(e, "listing files", location);
        }
    }

    @Override
    public FileEntry next()
            throws IOException
    {
        try {
            Blob blob = blobIterator.next();
            long length = requireNonNull(blob.getSize(), "blob size is null");
            return new FileEntry(Location.of(location.getBase() + blob.getName()), length, Instant.from(blob.getUpdateTimeOffsetDateTime()), Optional.empty());
        }
        catch (RuntimeException e) {
            throw handleGcsException(e, "listing files", location);
        }
    }
}
