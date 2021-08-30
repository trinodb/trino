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
package io.trino.sql.planner;

import io.trino.operator.HashGenerator;
import io.trino.spi.Page;
import io.trino.spi.connector.BucketFunction;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

public class HashBucketFunction
        implements BucketFunction
{
    private final HashGenerator generator;
    private final int bucketCount;

    public HashBucketFunction(HashGenerator generator, int bucketCount)
    {
        checkArgument(bucketCount > 0, "partitionCount must be at least 1");
        this.generator = generator;
        this.bucketCount = bucketCount;
    }

    @Override
    public int getBucket(Page page, int position)
    {
        return generator.getPartition(bucketCount, position, page);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("generator", generator)
                .add("bucketCount", bucketCount)
                .toString();
    }
}
