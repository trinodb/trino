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
import io.trino.spi.PageBuilder;
import io.trino.spi.type.ArrayType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestArrayBlockBuilder
        extends AbstractTestBlockBuilder<List<String>>
{
    // ArrayBlockBuilder: isNull, offset, 3 * value (FixedWidthBlockBuilder: isNull, value)
    private static final int THREE_INTS_ENTRY_SIZE = Byte.BYTES + Integer.BYTES + 3 * (Byte.BYTES + Long.BYTES);
    private static final int EXPECTED_ENTRY_COUNT = 100;

    @Test
    public void testArrayBlockIsFull()
    {
        testIsFull(PageBuilder.withMaxPageSize(THREE_INTS_ENTRY_SIZE * EXPECTED_ENTRY_COUNT, ImmutableList.of(new ArrayType(BIGINT))));
    }

    private void testIsFull(PageBuilder pageBuilder)
    {
        ArrayBlockBuilder blockBuilder = (ArrayBlockBuilder) pageBuilder.getBlockBuilder(0);
        assertThat(pageBuilder.isEmpty()).isTrue();
        while (!pageBuilder.isFull()) {
            pageBuilder.declarePosition();
            blockBuilder.buildEntry(elementBuilder -> {
                BIGINT.writeLong(elementBuilder, 12);
                elementBuilder.appendNull();
                BIGINT.writeLong(elementBuilder, 34);
            });
        }
        assertThat(blockBuilder.getPositionCount()).isEqualTo(EXPECTED_ENTRY_COUNT);
        assertThat(pageBuilder.isFull()).isEqualTo(true);
    }

    @Test
    public void testRetainedSizeInBytes()
    {
        BlockBuilderStatus status = new PageBuilderStatus(1024).createBlockBuilderStatus();
        BlockBuilder elements = BIGINT.createBlockBuilder(status, 1);
        ArrayBlockBuilder builder = new ArrayBlockBuilder(elements, status, 1);
        long baseSize = instanceSize(ArrayBlockBuilder.class) + BlockBuilderStatus.INSTANCE_SIZE;
        assertThat(builder.getRetainedSizeInBytes()).isEqualTo(baseSize + elements.getRetainedSizeInBytes());

        builder.appendNull();
        ArrayBlock block = (ArrayBlock) builder.buildValueBlock();
        assertThat(builder.getRetainedSizeInBytes())
                .isEqualTo(baseSize + elements.getRetainedSizeInBytes() + sizeOf(block.getRawOffsets()) + sizeOf(block.getRawValueIsValid()));

        builder.buildEntry(elementBuilder -> BIGINT.writeLong(elementBuilder, 123));
        block = (ArrayBlock) builder.build();
        assertThat(builder.getRetainedSizeInBytes())
                .isEqualTo(baseSize + elements.getRetainedSizeInBytes() + sizeOf(block.getRawOffsets()) + sizeOf(block.getRawValueIsValid()));
    }

    @Test
    public void testEmptyBlockOffsets()
    {
        ArrayBlockBuilder builder = new ArrayBlockBuilder(BIGINT, null, 8);
        long initialSize = builder.getRetainedSizeInBytes();
        builder.buildEntry();
        assertThatThrownBy(builder::buildValueBlock).isInstanceOf(IllegalStateException.class);
        builder.resetTo(0);
        assertThat(builder.build().getPositionCount()).isZero();
        assertThat(builder.getRetainedSizeInBytes()).isEqualTo(initialSize);

        int[] offsets = ((ArrayBlock) builder.buildValueBlock()).getRawOffsets();
        assertThat(offsets).containsExactly(0);
        assertThat(builder.getRetainedSizeInBytes()).isEqualTo(initialSize + sizeOf(offsets));
        assertThat(((ArrayBlock) builder.buildValueBlock()).getRawOffsets()).isSameAs(offsets);

        BlockBuilder other = builder.newBlockBuilderLike(null);
        assertThat(other.getRetainedSizeInBytes()).isEqualTo(initialSize);
        assertThat(((ArrayBlock) other.buildValueBlock()).getRawOffsets()).isNotSameAs(offsets);

        builder.buildEntry(elementBuilder -> BIGINT.writeLong(elementBuilder, 123));
        assertThat(((ArrayBlock) builder.build()).getRawOffsets()).hasSize(9).startsWith(0, 1);
    }

    @Test
    public void testAppendRangeRejectsInvalidRange()
    {
        ValueBlock source = blockFromValues(List.of(List.of(), List.of("keep"), List.of("outside"))).getRegion(0, 2);
        BlockBuilder builder = createBlockBuilder();
        BlockBuilder other = createBlockBuilder();

        assertThatThrownBy(() -> builder.appendRange(source, 1, -1)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> builder.appendRange(source, 2, 1)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThat(builder.getPositionCount()).isZero();

        builder.appendRange(source, 0, 2);
        other.appendRange(source, 1, 1);
        assertThat(blockToValues(builder.buildValueBlock())).containsExactly(List.of(), List.of("keep"));
        assertThat(blockToValues(other.buildValueBlock())).containsExactly(List.of("keep"));
    }

    @Test
    public void testConcurrentWriting()
    {
        ArrayBlockBuilder blockBuilder = new ArrayBlockBuilder(BIGINT, null, EXPECTED_ENTRY_COUNT);
        blockBuilder.buildEntry(elementBuilder -> {
            BIGINT.writeLong(elementBuilder, 45);
            assertThatThrownBy(() -> blockBuilder.buildEntry(_ -> {}))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Expected current entry to be closed but was opened");
        });
    }

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

    @Test
    void buildEntry()
    {
        List<List<String>> values = getTestValues();
        assertThat(values)
                .hasSize(5)
                .doesNotHaveDuplicates()
                .doesNotContainNull();

        ArrayBlockBuilder blockBuilder = (ArrayBlockBuilder) createBlockBuilder();
        for (List<String> array : values) {
            blockBuilder.buildEntry(elementBuilder -> {
                for (String element : array) {
                    if (element == null) {
                        elementBuilder.appendNull();
                    }
                    else {
                        VARCHAR.writeString(elementBuilder, element);
                    }
                }
            });
        }
        assertThat(blockToValues(blockBuilder.buildValueBlock())).isEqualTo(values);

        blockBuilder = (ArrayBlockBuilder) createBlockBuilder();
        for (List<String> array : values) {
            ArrayEntryBuilder arrayEntryBuilder = blockBuilder.buildEntry();
            for (String element : array) {
                if (element == null) {
                    arrayEntryBuilder.getElementBuilder().appendNull();
                }
                else {
                    VARCHAR.writeString(arrayEntryBuilder.getElementBuilder(), element);
                }
            }
            arrayEntryBuilder.build();
        }
        assertThat(blockToValues(blockBuilder.buildValueBlock())).isEqualTo(values);

        blockBuilder = (ArrayBlockBuilder) createBlockBuilder();
        blockBuilder.buildEntry();
        assertThatThrownBy(blockBuilder::buildEntry).isInstanceOf(IllegalStateException.class);

        blockBuilder = (ArrayBlockBuilder) createBlockBuilder();
        ArrayEntryBuilder multipleEntryBuilder = blockBuilder.buildEntry();
        multipleEntryBuilder.build();
        assertThatThrownBy(multipleEntryBuilder::getElementBuilder).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(multipleEntryBuilder::build).isInstanceOf(IllegalStateException.class);
    }

    private static BlockBuilder blockBuilder()
    {
        return new ArrayBlockBuilder(BIGINT, null, 10);
    }

    private static void assertIsAllNulls(Block block, int expectedPositionCount)
    {
        assertThat(block.getPositionCount()).isEqualTo(expectedPositionCount);
        if (expectedPositionCount <= 1) {
            assertThat(block.getClass()).isEqualTo(ArrayBlock.class);
        }
        else {
            assertThat(block.getClass()).isEqualTo(RunLengthEncodedBlock.class);
            assertThat(((RunLengthEncodedBlock) block).getValue().getClass()).isEqualTo(ArrayBlock.class);
        }
        if (expectedPositionCount > 0) {
            assertThat(block.isNull(0)).isTrue();
        }
    }

    @Override
    protected BlockBuilder createBlockBuilder()
    {
        return new ArrayBlockBuilder(new VariableWidthBlockBuilder(null, 1, 100), null, 1);
    }

    @Override
    protected List<List<String>> getTestValues()
    {
        return List.of(
                List.of("a", "apple", "ape"),
                Arrays.asList("b", null, "bear", "break"),
                List.of("c", "cherry"),
                Arrays.asList("d", "date", "dinosaur", null, "dirt"),
                List.of("e", "eggplant", "empty", ""));
    }

    @Override
    protected List<String> getUnusedTestValue()
    {
        return List.of("unused", "ignore me");
    }

    @Override
    protected ValueBlock blockFromValues(Iterable<List<String>> values)
    {
        ArrayBlockBuilder blockBuilder = new ArrayBlockBuilder(new VariableWidthBlockBuilder(null, 1, 100), null, 1);
        for (List<String> array : values) {
            if (array == null) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.buildEntry(elementBuilder -> {
                    for (String entry : array) {
                        if (entry == null) {
                            elementBuilder.appendNull();
                        }
                        else {
                            VARCHAR.writeString(elementBuilder, entry);
                        }
                    }
                });
            }
        }
        return blockBuilder.buildValueBlock();
    }

    @Override
    protected List<List<String>> blockToValues(ValueBlock valueBlock)
    {
        ArrayBlock block = (ArrayBlock) valueBlock;
        List<List<String>> actualValues = new ArrayList<>(block.getPositionCount());
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                actualValues.add(null);
            }
            else {
                Block array = block.getArray(i);
                ArrayList<String> arrayBuilder = new ArrayList<>();
                for (int j = 0; j < array.getPositionCount(); j++) {
                    if (array.isNull(j)) {
                        arrayBuilder.add(null);
                    }
                    else {
                        arrayBuilder.add(VARCHAR.getSlice(array, j).toStringUtf8());
                    }
                }
                actualValues.add(arrayBuilder);
            }
        }
        return actualValues;
    }
}
