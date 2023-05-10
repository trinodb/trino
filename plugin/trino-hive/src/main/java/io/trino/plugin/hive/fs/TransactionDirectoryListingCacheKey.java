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

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public class TransactionDirectoryListingCacheKey
{
    private static final long INSTANCE_SIZE = instanceSize(TransactionDirectoryListingCacheKey.class);

    private final long transactionId;
    private final DirectoryListingCacheKey key;

    public TransactionDirectoryListingCacheKey(long transactionId, DirectoryListingCacheKey key)
    {
        this.transactionId = transactionId;
        this.key = requireNonNull(key, "key is null");
    }

    public DirectoryListingCacheKey getKey()
    {
        return key;
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + key.getRetainedSizeInBytes();
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
        return transactionId == that.transactionId && key.equals(that.key);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(transactionId, key);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("transactionId", transactionId)
                .add("key", key)
                .toString();
    }
}
