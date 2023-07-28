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
package io.trino.spi.block;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRowBlockBuilder
        extends AbstractTestBlockBuilder<TestRowBlockBuilder.TestRow>
{
    @Test
    public void testBuilderProducesNullRleForNullRows()
    {
        // empty block
        assertIsAllNulls(blockBuilder().build(), 0);

        // single null
        assertIsAllNulls(blockBuilder().appendNull().build(), 1);

        // multiple nulls
        assertIsAllNulls(blockBuilder().appendNull().appendNull().build(), 2);
    }

    private static BlockBuilder blockBuilder()
    {
        return new RowBlockBuilder(ImmutableList.of(BIGINT), null, 10);
    }

    private static void assertIsAllNulls(Block block, int expectedPositionCount)
    {
        assertThat(block.getPositionCount()).isEqualTo(expectedPositionCount);
        if (expectedPositionCount <= 1) {
            assertThat(block.getClass()).isEqualTo(RowBlock.class);
        }
        else {
            assertThat(block.getClass()).isEqualTo(RunLengthEncodedBlock.class);
            assertThat(((RunLengthEncodedBlock) block).getValue().getClass()).isEqualTo(RowBlock.class);
        }
        if (expectedPositionCount > 0) {
            assertThat(block.isNull(0)).isTrue();
        }
    }

    @Override
    protected BlockBuilder createBlockBuilder()
    {
        return new RowBlockBuilder(List.of(VARCHAR, INTEGER, BOOLEAN), null, 1);
    }

    @Override
    protected List<TestRow> getTestValues()
    {
        return List.of(
                new TestRow("apple", 2, true),
                new TestRow("bear", 5, false),
                new TestRow(null, 7, true),
                new TestRow("dinosaur", 9, false),
                new TestRow("", 22, true));
    }

    @Override
    protected TestRow getUnusedTestValue()
    {
        return new TestRow("unused", -1, false);
    }

    @Override
    protected ValueBlock blockFromValues(Iterable<TestRow> values)
    {
        RowBlockBuilder blockBuilder = new RowBlockBuilder(List.of(VARCHAR, INTEGER, BOOLEAN), null, 1);
        for (TestRow row : values) {
            if (row == null) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.buildEntry(fieldBuilders -> {
                    if (row.name() == null) {
                        fieldBuilders.get(0).appendNull();
                    }
                    else {
                        VARCHAR.writeString(fieldBuilders.get(0), row.name());
                    }
                    INTEGER.writeLong(fieldBuilders.get(1), row.number());
                    BOOLEAN.writeBoolean(fieldBuilders.get(2), row.flag());
                });
            }
        }
        return blockBuilder.buildValueBlock();
    }

    @Override
    protected List<TestRow> blockToValues(ValueBlock valueBlock)
    {
        RowBlock block = (RowBlock) valueBlock;
        List<TestRow> actualValues = new ArrayList<>(block.getPositionCount());
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                actualValues.add(null);
            }
            else {
                SqlRow sqlRow = block.getRow(i);
                actualValues.add(new TestRow(
                        (String) VARCHAR.getObjectValue(null, sqlRow.getUnderlyingFieldBlock(0), sqlRow.getUnderlyingFieldPosition(0)),
                        INTEGER.getInt(sqlRow.getUnderlyingFieldBlock(1), sqlRow.getUnderlyingFieldPosition(1)),
                        BOOLEAN.getBoolean(sqlRow.getUnderlyingFieldBlock(2), sqlRow.getUnderlyingFieldPosition(2))));
            }
        }
        return actualValues;
    }

    public record TestRow(String name, int number, boolean flag) {}
}
