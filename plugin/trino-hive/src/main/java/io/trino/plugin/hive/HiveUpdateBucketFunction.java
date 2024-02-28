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
package io.trino.plugin.hive;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.BucketFunction;

import static io.trino.plugin.hive.HivePageSource.BUCKET_CHANNEL;
import static io.trino.spi.type.IntegerType.INTEGER;

public class HiveUpdateBucketFunction
        implements BucketFunction
{
    private final int bucketCount;

    public HiveUpdateBucketFunction(int bucketCount)
    {
        this.bucketCount = bucketCount;
    }

    @Override
    public int getBucket(Page page, int position)
    {
        Block block = page.getBlock(0);
        SqlRow bucketRow = ((RowBlock) block.getUnderlyingValueBlock()).getRow(block.getUnderlyingValuePosition(position));
        long value = INTEGER.getInt(bucketRow.getRawFieldBlock(BUCKET_CHANNEL), bucketRow.getRawIndex());
        return (int) (value & Integer.MAX_VALUE) % bucketCount;
    }
}
