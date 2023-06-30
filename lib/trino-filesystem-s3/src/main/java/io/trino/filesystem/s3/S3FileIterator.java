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
package io.trino.filesystem.s3;

import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

final class S3FileIterator
        implements FileIterator
{
    private final S3Location location;
    private final Iterator<S3Object> iterator;
    private final String base;

    public S3FileIterator(S3Location location, Iterator<S3Object> iterator)
    {
        this.location = requireNonNull(location, "location is null");
        this.iterator = requireNonNull(iterator, "iterator is null");
        this.base = "%s://%s/".formatted(location.scheme(), location.bucket());
    }

    @Override
    public boolean hasNext()
            throws IOException
    {
        try {
            return iterator.hasNext();
        }
        catch (SdkException e) {
            throw new IOException("Failed to list location: " + location, e);
        }
    }

    @Override
    public FileEntry next()
            throws IOException
    {
        try {
            S3Object object = iterator.next();

            verify(object.key().startsWith(location.key()), "S3 listed key [%s] does not start with prefix [%s]", object.key(), location.key());

            return new FileEntry(
                    Location.of(base + object.key()),
                    object.size(),
                    object.lastModified(),
                    Optional.empty());
        }
        catch (SdkException e) {
            throw new IOException("Failed to list location: " + location, e);
        }
    }
}
