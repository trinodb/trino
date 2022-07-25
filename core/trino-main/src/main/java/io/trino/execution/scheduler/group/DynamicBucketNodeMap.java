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
package io.trino.execution.scheduler.group;

import io.trino.execution.scheduler.BucketNodeMap;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Split;

import java.util.function.ToIntFunction;

import static com.google.common.base.Preconditions.checkArgument;

public class DynamicBucketNodeMap
        extends BucketNodeMap
{
    private final int bucketCount;

    public DynamicBucketNodeMap(ToIntFunction<Split> splitToBucket, int bucketCount)
    {
        super(splitToBucket);
        checkArgument(bucketCount > 0, "bucketCount must be positive");
        this.bucketCount = bucketCount;
    }

    @Override
    public InternalNode getAssignedNode(int bucketedId)
    {
        throw new UnsupportedOperationException("DynamicBucketNodeMap does not support assigned nodes");
    }

    @Override
    public int getBucketCount()
    {
        return bucketCount;
    }

    @Override
    public boolean isDynamic()
    {
        return true;
    }
}
