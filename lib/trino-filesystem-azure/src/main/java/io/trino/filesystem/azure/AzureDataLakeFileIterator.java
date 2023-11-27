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

import com.azure.storage.file.datalake.models.PathItem;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;

import static io.trino.filesystem.azure.AzureUtils.handleAzureException;
import static java.util.Objects.requireNonNull;

final class AzureDataLakeFileIterator
        implements FileIterator
{
    private final AzureLocation location;
    private final Iterator<PathItem> iterator;
    private final Location baseLocation;

    AzureDataLakeFileIterator(AzureLocation location, Iterator<PathItem> iterator)
    {
        this.location = requireNonNull(location, "location is null");
        this.iterator = requireNonNull(iterator, "iterator is null");
        this.baseLocation = location.baseLocation();
    }

    @Override
    public boolean hasNext()
            throws IOException
    {
        try {
            return iterator.hasNext();
        }
        catch (RuntimeException e) {
            throw handleAzureException(e, "listing files", location);
        }
    }

    @Override
    public FileEntry next()
            throws IOException
    {
        try {
            PathItem pathItem = iterator.next();
            return new FileEntry(
                    baseLocation.appendPath(pathItem.getName()),
                    pathItem.getContentLength(),
                    pathItem.getLastModified().toInstant(),
                    Optional.empty());
        }
        catch (RuntimeException e) {
            throw handleAzureException(e, "listing files", location);
        }
    }
}
