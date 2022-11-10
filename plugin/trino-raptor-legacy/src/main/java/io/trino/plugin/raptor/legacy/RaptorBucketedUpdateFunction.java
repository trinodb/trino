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

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.BucketFunction;

import static io.trino.spi.type.IntegerType.INTEGER;
import static java.lang.Math.toIntExact;

public class RaptorBucketedUpdateFunction
        implements BucketFunction
{
    @Override
    public int getBucket(Page page, int position)
    {
        Block row = page.getBlock(0).getObject(position, Block.class);
        return toIntExact(INTEGER.getLong(row, 0)); // bucket field of row ID
    }
}
