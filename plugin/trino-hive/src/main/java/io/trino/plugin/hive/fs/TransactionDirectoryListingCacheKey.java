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
package io.trino.plugin.hive.fs;

import io.trino.filesystem.Location;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public class TransactionDirectoryListingCacheKey
{
    private static final long INSTANCE_SIZE = instanceSize(TransactionDirectoryListingCacheKey.class);

    private final long transactionId;
    private final Location path;

    public TransactionDirectoryListingCacheKey(long transactionId, Location path)
    {
        this.transactionId = transactionId;
        this.path = requireNonNull(path, "path is null");
    }

    public Location getPath()
    {
        return path;
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + estimatedSizeOf(path.toString());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TransactionDirectoryListingCacheKey that = (TransactionDirectoryListingCacheKey) o;
        return transactionId == that.transactionId && path.equals(that.path);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(transactionId, path);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("transactionId", transactionId)
                .add("path", path)
                .toString();
    }
}
