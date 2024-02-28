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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.trino.spi.block.ColumnarTestUtils.alternatingNullValues;
import static io.trino.spi.block.ColumnarTestUtils.assertBlock;
import static io.trino.spi.block.ColumnarTestUtils.assertBlockPosition;
import static io.trino.spi.block.ColumnarTestUtils.createTestDictionaryBlock;
import static io.trino.spi.block.ColumnarTestUtils.createTestDictionaryExpectedValues;
import static io.trino.spi.block.ColumnarTestUtils.createTestRleBlock;
import static io.trino.spi.block.ColumnarTestUtils.createTestRleExpectedValues;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRowBlockFieldExtraction
{
    @Test
    public void testBlockFieldExtraction()
    {
        int fieldCount = 5;
        RowType rowType = RowType.anonymous(Collections.nCopies(fieldCount, VARCHAR));
        Slice[][] expectedValues = new Slice[20][];
        for (int rowIndex = 0; rowIndex < expectedValues.length; rowIndex++) {
            expectedValues[rowIndex] = new Slice[fieldCount];
            for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
                if (fieldIndex % 3 != 1) {
                    expectedValues[rowIndex][fieldIndex] = Slices.utf8Slice(format("%d.%d", rowIndex, fieldIndex));
                }
            }
        }
        Block block = createBlockBuilderWithValues(rowType, expectedValues);
        verifyBlock(rowType, block, expectedValues);

        Slice[][] expectedValuesWithNull = alternatingNullValues(expectedValues);
        Block blockWithNull = createBlockBuilderWithValues(rowType, expectedValuesWithNull);
        verifyBlock(rowType, blockWithNull, expectedValuesWithNull);
    }

    private static <T> void verifyBlock(RowType rowType, Block block, T[] expectedValues)
    {
        assertBlock(rowType, block, expectedValues);

        assertGetFields(rowType, block, expectedValues);
        assertGetNullSuppressedFields(rowType, block, expectedValues);
        assertDictionaryBlock(rowType, block, expectedValues);
        assertRunLengthEncodedBlock(rowType, block, expectedValues);

        int offset = 1;
        int length = expectedValues.length - 2;
        Block blockRegion = block.getRegion(offset, length);
        T[] expectedValuesRegion = Arrays.copyOfRange(expectedValues, offset, offset + length);

        assertBlock(rowType, blockRegion, expectedValuesRegion);

        assertGetFields(rowType, blockRegion, expectedValuesRegion);
        assertGetNullSuppressedFields(rowType, blockRegion, expectedValuesRegion);
        assertDictionaryBlock(rowType, blockRegion, expectedValuesRegion);
        assertRunLengthEncodedBlock(rowType, blockRegion, expectedValuesRegion);
    }

    private static <T> void assertDictionaryBlock(RowType rowType, Block block, T[] expectedValues)
    {
        Block dictionaryBlock = createTestDictionaryBlock(block);
        T[] expectedDictionaryValues = createTestDictionaryExpectedValues(expectedValues);

        assertBlock(rowType, dictionaryBlock, expectedDictionaryValues);
        assertGetFields(rowType, dictionaryBlock, expectedDictionaryValues);
        assertGetNullSuppressedFields(rowType, dictionaryBlock, expectedDictionaryValues);
        assertRunLengthEncodedBlock(rowType, dictionaryBlock, expectedDictionaryValues);
    }

    private static <T> void assertRunLengthEncodedBlock(RowType rowType, Block block, T[] expectedValues)
    {
        for (int position = 0; position < block.getPositionCount(); position++) {
            RunLengthEncodedBlock runLengthEncodedBlock = createTestRleBlock(block, position);
            T[] expectedDictionaryValues = createTestRleExpectedValues(expectedValues, position);

            assertBlock(rowType, runLengthEncodedBlock, expectedDictionaryValues);
            assertGetFields(rowType, runLengthEncodedBlock, expectedDictionaryValues);
            assertGetNullSuppressedFields(rowType, runLengthEncodedBlock, expectedDictionaryValues);
        }
    }

    private static <T> void assertGetFields(RowType rowType, Block block, T[] expectedValues)
    {
        assertThat(block.getPositionCount()).isEqualTo(expectedValues.length);
        List<Block> nullSuppressedFields = RowBlock.getRowFieldsFromBlock(block);

        for (int fieldId = 0; fieldId < 5; fieldId++) {
            Block fieldBlock = nullSuppressedFields.get(fieldId);
            Type fieldType = rowType.getTypeParameters().get(fieldId);
            for (int position = 0; position < expectedValues.length; position++) {
                T expectedRow = expectedValues[position];
                assertThat(block.isNull(position)).isEqualTo(expectedRow == null);

                Object expectedElement = expectedRow == null ? null : Array.get(expectedRow, fieldId);
                assertBlockPosition(fieldType, fieldBlock, position, expectedElement);
            }
        }
    }

    private static <T> void assertGetNullSuppressedFields(RowType rowType, Block block, T[] expectedValues)
    {
        assertThat(block.getPositionCount()).isEqualTo(expectedValues.length);
        List<Block> nullSuppressedFields = RowBlock.getNullSuppressedRowFieldsFromBlock(block);

        for (int fieldId = 0; fieldId < 5; fieldId++) {
            Block fieldBlock = nullSuppressedFields.get(fieldId);
            Type fieldType = rowType.getTypeParameters().get(fieldId);
            int nullSuppressedPosition = 0;
            for (int position = 0; position < expectedValues.length; position++) {
                T expectedRow = expectedValues[position];
                assertThat(block.isNull(position)).isEqualTo(expectedRow == null);
                if (expectedRow == null) {
                    continue;
                }
                Object expectedElement = Array.get(expectedRow, fieldId);
                assertBlockPosition(fieldType, fieldBlock, nullSuppressedPosition, expectedElement);
                nullSuppressedPosition++;
            }
        }
    }

    public static Block createBlockBuilderWithValues(RowType rowType, Slice[][] expectedValues)
    {
        RowBlockBuilder blockBuilder = rowType.createBlockBuilder(null, 100);
        for (Slice[] expectedValue : expectedValues) {
            if (expectedValue == null) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.buildEntry(fieldBuilders -> {
                    for (int i = 0; i < expectedValue.length; i++) {
                        Slice v = expectedValue[i];
                        if (v == null) {
                            fieldBuilders.get(i).appendNull();
                        }
                        else {
                            VARCHAR.writeSlice(fieldBuilders.get(i), v);
                        }
                    }
                });
            }
        }
        return blockBuilder.build();
    }
}
