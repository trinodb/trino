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

import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import static io.trino.block.BlockAssertions.createIntsBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRaptorBucketFunction
{
    @Test
    public void testBigint()
    {
        BucketFunction function = bucketFunction(50, BIGINT);
        assertThat(getBucket(function, createLongsBlock(123456789012L))).isEqualTo(12);
        assertThat(getBucket(function, createLongsBlock(454345325))).isEqualTo(16);
        assertThat(getBucket(function, createLongsBlock(365363))).isEqualTo(42);
        assertThat(getBucket(function, createLongsBlock(45645747))).isEqualTo(41);
        assertThat(getBucket(function, createLongsBlock(3244))).isEqualTo(29);

        function = bucketFunction(2, BIGINT);
        assertThat(getBucket(function, createLongsBlock(123456789012L))).isEqualTo(0);
        assertThat(getBucket(function, createLongsBlock(454345325))).isEqualTo(0);
        assertThat(getBucket(function, createLongsBlock(365363))).isEqualTo(0);
        assertThat(getBucket(function, createLongsBlock(45645747))).isEqualTo(1);
        assertThat(getBucket(function, createLongsBlock(3244))).isEqualTo(1);
    }

    @Test
    public void testInteger()
    {
        BucketFunction function = bucketFunction(50, INTEGER);
        assertThat(getBucket(function, createIntsBlock(454345325))).isEqualTo(16);
        assertThat(getBucket(function, createIntsBlock(365363))).isEqualTo(42);
        assertThat(getBucket(function, createIntsBlock(45645747))).isEqualTo(41);
        assertThat(getBucket(function, createIntsBlock(3244))).isEqualTo(29);
    }

    @Test
    public void testVarchar()
    {
        BucketFunction function = bucketFunction(50, createUnboundedVarcharType());
        assertThat(getBucket(function, createStringsBlock("lorem ipsum"))).isEqualTo(2);
        assertThat(getBucket(function, createStringsBlock("lorem"))).isEqualTo(26);
        assertThat(getBucket(function, createStringsBlock("ipsum"))).isEqualTo(3);
        assertThat(getBucket(function, createStringsBlock("hello"))).isEqualTo(19);
    }

    @Test
    public void testVarcharBigint()
    {
        BucketFunction function = bucketFunction(50, createUnboundedVarcharType(), BIGINT);
        assertThat(getBucket(function, createStringsBlock("lorem ipsum"), createLongsBlock(123456789012L))).isEqualTo(24);
        assertThat(getBucket(function, createStringsBlock("lorem"), createLongsBlock(454345325))).isEqualTo(32);
        assertThat(getBucket(function, createStringsBlock("ipsum"), createLongsBlock(365363))).isEqualTo(21);
        assertThat(getBucket(function, createStringsBlock("hello"), createLongsBlock(45645747))).isEqualTo(34);
        assertThat(getBucket(function, createStringsBlock("world"), createLongsBlock(3244))).isEqualTo(4);
    }

    private static int getBucket(BucketFunction function, Block... blocks)
    {
        return function.getBucket(new Page(blocks), 0);
    }

    private static BucketFunction bucketFunction(int bucketCount, Type... types)
    {
        return new RaptorBucketFunction(bucketCount, ImmutableList.copyOf(types));
    }
}
