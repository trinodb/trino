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
package io.trino.plugin.raptor.legacy;

import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.type.UuidType;

public class RaptorUnbucketedUpdateFunction
        implements BucketFunction
{
    private final int bucketCount;

    public RaptorUnbucketedUpdateFunction(int bucketCount)
    {
        this.bucketCount = bucketCount;
    }

    @Override
    public int getBucket(Page page, int position)
    {
        Block row = page.getBlock(0).getObject(position, Block.class);
        Slice uuid = UuidType.UUID.getSlice(row, 1); // uuid field of row ID
        return (uuid.hashCode() & Integer.MAX_VALUE) % bucketCount;
    }
}
