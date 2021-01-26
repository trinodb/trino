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
package io.trino.type;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.SqlTimestamp;

import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;

public class TestTimestampType
        extends AbstractTestType
{
    public TestTimestampType()
    {
        super(TIMESTAMP_MILLIS, SqlTimestamp.class, createTestBlock());
    }

    public static Block createTestBlock()
    {
        BlockBuilder blockBuilder = TIMESTAMP_MILLIS.createBlockBuilder(null, 15);
        TIMESTAMP_MILLIS.writeLong(blockBuilder, 1111_000);
        TIMESTAMP_MILLIS.writeLong(blockBuilder, 1111_000);
        TIMESTAMP_MILLIS.writeLong(blockBuilder, 1111_000);
        TIMESTAMP_MILLIS.writeLong(blockBuilder, 2222_000);
        TIMESTAMP_MILLIS.writeLong(blockBuilder, 2222_000);
        TIMESTAMP_MILLIS.writeLong(blockBuilder, 2222_000);
        TIMESTAMP_MILLIS.writeLong(blockBuilder, 2222_000);
        TIMESTAMP_MILLIS.writeLong(blockBuilder, 2222_000);
        TIMESTAMP_MILLIS.writeLong(blockBuilder, 3333_000);
        TIMESTAMP_MILLIS.writeLong(blockBuilder, 3333_000);
        TIMESTAMP_MILLIS.writeLong(blockBuilder, 4444_000);
        return blockBuilder.build();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        return ((Long) value) + 1;
    }
}
