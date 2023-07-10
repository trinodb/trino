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
package io.trino.plugin.deltalake.transactionlog;

import io.airlift.slice.SizeOf;

import java.util.Objects;

import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public class CanonicalColumnName
{
    private static final int INSTANCE_SIZE = instanceSize(CanonicalColumnName.class);

    private int hash;
    private final String originalName;

    public CanonicalColumnName(String originalName)
    {
        this.originalName = requireNonNull(originalName, "originalName is null");
    }

    public String getOriginalName()
    {
        return originalName;
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
        CanonicalColumnName that = (CanonicalColumnName) o;
        return hashCode() == that.hashCode() // compare hash codes because they are cached, so this is cheap and efficient
                && Objects.equals(TransactionLogAccess.canonicalizeColumnName(originalName), TransactionLogAccess.canonicalizeColumnName(that.originalName));
    }

    @Override
    public int hashCode()
    {
        if (this.hash == 0) {
            int newHash = TransactionLogAccess.canonicalizeColumnName(originalName).hashCode();
            if (newHash == 0) {
                newHash = 1;
            }
            this.hash = newHash;
        }
        return this.hash;
    }

    public long getRetainedSize()
    {
        return INSTANCE_SIZE + SizeOf.estimatedSizeOf(originalName);
    }
}
