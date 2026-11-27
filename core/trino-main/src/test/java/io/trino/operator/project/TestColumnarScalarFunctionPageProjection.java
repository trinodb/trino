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
package io.trino.operator.project;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import io.trino.operator.project.ColumnarScalarFunctionPageProjection.Argument;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.SourcePage;
import io.trino.sql.ir.Reference;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestColumnarScalarFunctionPageProjection
{
    @Test
    public void testArgumentsAreNotCopied()
    {
        Block input = longBlock(10, 20, 30);
        SourcePage sourcePage = SourcePage.create(new Page(input));
        ColumnarScalarFunctionPageProjection projection = new ColumnarScalarFunctionPageProjection(
                new Reference(BIGINT, "test"),
                true,
                new InputChannels(0),
                ImmutableList.of(Argument.input(0), Argument.input(0), Argument.constant(writeNativeValue(BIGINT, 7L))),
                (_, arguments, selectedPositions) -> {
                    assertThat(arguments.getPositionCount()).isEqualTo(3);
                    assertThat(arguments.getChannelCount()).isEqualTo(3);
                    assertThat(arguments.getBlock(0)).isSameAs(input);
                    assertThat(arguments.getBlock(2)).isInstanceOf(RunLengthEncodedBlock.class);
                    assertThat(arguments.getBlock(2).getPositionCount()).isEqualTo(3);
                    assertThat(BIGINT.getLong(arguments.getBlock(2), 0)).isEqualTo(7);
                    assertThat(selectedPositions.isList()).isTrue();
                    assertThat(selectedPositions.size()).isEqualTo(2);
                    assertThatThrownBy(() -> arguments.selectPositions(new int[] {0}, 0, 1))
                            .isInstanceOf(UnsupportedOperationException.class);
                    return selectedPositions.select(arguments.getBlock(1));
                });

        Block result = projection.project(SESSION, sourcePage, SelectedPositions.positionsList(new int[] {2, 0}, 0, 2));
        assertThat(result.getPositionCount()).isEqualTo(2);
        assertThat(BIGINT.getLong(result, 0)).isEqualTo(30);
        assertThat(BIGINT.getLong(result, 1)).isEqualTo(10);
        assertThat(sourcePage.getPositionCount()).isEqualTo(3);
    }

    @Test
    public void testRangeSelection()
    {
        Block result = projectSelected(SelectedPositions.positionsRange(1, 3), 3, ImmutableList.of(0, 1, 2), false);
        assertLongs(result, 20, 30, 40);
    }

    @Test
    public void testListSelection()
    {
        // only the region between the first and the last selected position is passed to the function
        Block result = projectSelected(SelectedPositions.positionsList(new int[] {99, 1, 3, 4, 99}, 1, 3), 4, ImmutableList.of(0, 2, 3), true);
        assertLongs(result, 20, 40, 50);
    }

    @Test
    public void testWideListSelection()
    {
        // the selected positions span the page, so the page is passed with the selected positions unchanged
        Block result = projectSelected(SelectedPositions.positionsList(new int[] {99, 0, 2, 5}, 1, 3), 6, ImmutableList.of(0, 2, 5), true);
        assertLongs(result, 10, 30, 60);

        // a region of 14 of the 16 positions covers 7/8 of the page
        result = projectSelected(SelectedPositions.positionsList(new int[] {13, 0, 7}, 0, 3), 16, 16, ImmutableList.of(13, 0, 7), true);
        assertLongs(result, 140, 10, 80);

        // a region of 13 of the 16 positions is passed as a region
        result = projectSelected(SelectedPositions.positionsList(new int[] {1, 5, 13}, 0, 3), 16, 13, ImmutableList.of(0, 4, 12), true);
        assertLongs(result, 20, 60, 140);
    }

    @Test
    public void testContiguousListSelection()
    {
        Block result = projectSelected(SelectedPositions.positionsList(new int[] {2, 3, 4}, 0, 3), 3, ImmutableList.of(0, 1, 2), false);
        assertLongs(result, 30, 40, 50);
    }

    @Test
    public void testUnorderedListSelection()
    {
        Block result = projectSelected(SelectedPositions.positionsList(new int[] {3, 2, 1}, 0, 3), 3, ImmutableList.of(2, 1, 0), true);
        assertLongs(result, 40, 30, 20);
    }

    @Test
    public void testRejectsIncorrectResultPositionCount()
    {
        ColumnarScalarFunctionPageProjection projection = new ColumnarScalarFunctionPageProjection(
                new Reference(BIGINT, "test"),
                true,
                new InputChannels(0),
                ImmutableList.of(Argument.input(0)),
                (_, _, _) -> longBlock(1));

        assertThatThrownBy(() -> projection.project(
                SESSION,
                SourcePage.create(new Page(longBlock(10, 20))),
                SelectedPositions.positionsRange(0, 2)))
                .isInstanceOf(VerifyException.class)
                .hasMessage("Columnar scalar function returned 1 positions, expected 2");
    }

    private static Block projectSelected(SelectedPositions selectedPositions, int expectedArgumentPositions, List<Integer> expectedSelected, boolean expectedList)
    {
        return projectSelected(selectedPositions, 6, expectedArgumentPositions, expectedSelected, expectedList);
    }

    private static Block projectSelected(SelectedPositions selectedPositions, int pagePositionCount, int expectedArgumentPositions, List<Integer> expectedSelected, boolean expectedList)
    {
        ColumnarScalarFunctionPageProjection projection = new ColumnarScalarFunctionPageProjection(
                new Reference(BIGINT, "test"),
                true,
                new InputChannels(0),
                ImmutableList.of(Argument.input(0)),
                (_, arguments, selected) -> {
                    assertThat(arguments.getPositionCount()).isEqualTo(expectedArgumentPositions);
                    assertThat(selected.isList()).isEqualTo(expectedList);
                    assertThat(toList(selected)).isEqualTo(expectedSelected);
                    return selected.select(arguments.getBlock(0));
                });
        long[] values = new long[pagePositionCount];
        for (int position = 0; position < pagePositionCount; position++) {
            values[position] = (position + 1) * 10L;
        }
        return projection.project(SESSION, SourcePage.create(new Page(longBlock(values))), selectedPositions);
    }

    private static List<Integer> toList(io.trino.spi.function.SelectedPositions selectedPositions)
    {
        ImmutableList.Builder<Integer> positions = ImmutableList.builder();
        for (int index = 0; index < selectedPositions.size(); index++) {
            if (selectedPositions.isList()) {
                positions.add(selectedPositions.positions()[selectedPositions.offset() + index]);
            }
            else {
                positions.add(selectedPositions.offset() + index);
            }
        }
        return positions.build();
    }

    private static void assertLongs(Block block, long... expected)
    {
        assertThat(block.getPositionCount()).isEqualTo(expected.length);
        for (int position = 0; position < expected.length; position++) {
            assertThat(BIGINT.getLong(block, position)).isEqualTo(expected[position]);
        }
    }

    private static Block longBlock(long... values)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(values.length);
        for (long value : values) {
            BIGINT.writeLong(builder, value);
        }
        return builder.build();
    }
}
