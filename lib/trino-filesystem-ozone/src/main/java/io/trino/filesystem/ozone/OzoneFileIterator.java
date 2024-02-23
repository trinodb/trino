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
package io.trino.filesystem.ozone;

import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import org.apache.hadoop.ozone.client.OzoneKey;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class OzoneFileIterator
        implements FileIterator
{
    private final OzoneLocation location;
    private final Iterator<? extends OzoneKey> iterator;

    public OzoneFileIterator(OzoneLocation location, Iterator<? extends OzoneKey> iterator)
    {
        this.location = requireNonNull(location, "location is null");
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext()
            throws IOException
    {
        try {
            return iterator.hasNext();
        }
        catch (RuntimeException e) {
            // TODO
            throw e;
        }
    }

    @Override
    public FileEntry next()
            throws IOException
    {
        try {
            OzoneKey blob = iterator.next();
            long length = blob.getDataSize();
            return new FileEntry(
                    location.baseLocation().appendPath(blob.getName()),
                    length,
                    blob.getModificationTime(),
                    Optional.empty());
        }
        catch (RuntimeException e) {
            // TODO
            throw e;
        }
    }
}
