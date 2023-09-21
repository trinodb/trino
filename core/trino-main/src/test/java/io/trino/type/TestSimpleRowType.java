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

import com.google.common.collect.ImmutableList;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.type.RowType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSimpleRowType
        extends AbstractTestType
{
    private static final RowType TYPE = RowType.from(ImmutableList.of(
            field("a", BIGINT),
            field("b", VARCHAR)));

    public TestSimpleRowType()
    {
        super(TYPE, List.class, createTestBlock());
    }

    private static Block createTestBlock()
    {
        RowBlockBuilder blockBuilder = TYPE.createBlockBuilder(null, 3);

        blockBuilder.buildEntry(fieldBuilders -> {
            BIGINT.writeLong(fieldBuilders.get(0), 1);
            VARCHAR.writeSlice(fieldBuilders.get(1), utf8Slice("cat"));
        });

        blockBuilder.buildEntry(fieldBuilders -> {
            BIGINT.writeLong(fieldBuilders.get(0), 2);
            VARCHAR.writeSlice(fieldBuilders.get(1), utf8Slice("cats"));
        });

        blockBuilder.buildEntry(fieldBuilders -> {
            BIGINT.writeLong(fieldBuilders.get(0), 3);
            VARCHAR.writeSlice(fieldBuilders.get(1), utf8Slice("dog"));
        });

        return blockBuilder.build();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        return buildRowValue(TYPE, fieldBuilders -> {
            Block block = (Block) value;
            BIGINT.writeLong(fieldBuilders.get(0), block.getSingleValueBlock(0).getLong(0, 0) + 1);
            VARCHAR.writeSlice(fieldBuilders.get(1), block.getSingleValueBlock(1).getSlice(0, 0, 1));
        });
    }

    @Test
    public void testRange()
    {
        assertThat(type.getRange())
                .isEmpty();
    }

    @Test
    public void testPreviousValue()
    {
        assertThat(type.getPreviousValue(getSampleValue()))
                .isEmpty();
    }

    @Test
    public void testNextValue()
    {
        assertThat(type.getNextValue(getSampleValue()))
                .isEmpty();
    }
}
