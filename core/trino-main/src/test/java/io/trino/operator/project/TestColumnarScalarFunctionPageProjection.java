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
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.SourcePage;
import io.trino.sql.ir.Reference;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestColumnarScalarFunctionPageProjection
{
    @Test
    public void testArgumentsAreSelectedAndImmutable()
    {
        SourcePage sourcePage = SourcePage.create(new Page(longBlock(10, 20, 30)));
        ColumnarScalarFunctionPageProjection projection = new ColumnarScalarFunctionPageProjection(
                new Reference(BIGINT, "test"),
                true,
                new InputChannels(0),
                ImmutableList.of(Argument.input(0), Argument.input(0), Argument.constant(writeNativeValue(BIGINT, 7L))),
                (_, arguments) -> {
                    assertThat(arguments.getPositionCount()).isEqualTo(2);
                    assertThat(arguments.getChannelCount()).isEqualTo(3);
                    assertThat(arguments.getBlock(0)).isInstanceOf(DictionaryBlock.class);
                    assertThat(arguments.getBlock(2)).isInstanceOf(RunLengthEncodedBlock.class);
                    assertThat(BIGINT.getLong(arguments.getBlock(2), 0)).isEqualTo(7);
                    assertThatThrownBy(() -> arguments.selectPositions(new int[] {0}, 0, 1))
                            .isInstanceOf(UnsupportedOperationException.class);
                    return arguments.getBlock(1);
                });

        Block result = projection.project(SESSION, sourcePage, SelectedPositions.positionsList(new int[] {2, 0}, 0, 2));
        assertThat(BIGINT.getLong(result, 0)).isEqualTo(30);
        assertThat(BIGINT.getLong(result, 1)).isEqualTo(10);
        assertThat(sourcePage.getPositionCount()).isEqualTo(3);
    }

    @Test
    public void testRejectsIncorrectResultPositionCount()
    {
        ColumnarScalarFunctionPageProjection projection = new ColumnarScalarFunctionPageProjection(
                new Reference(BIGINT, "test"),
                true,
                new InputChannels(0),
                ImmutableList.of(Argument.input(0)),
                (_, _) -> longBlock(1));

        assertThatThrownBy(() -> projection.project(
                SESSION,
                SourcePage.create(new Page(longBlock(10, 20))),
                SelectedPositions.positionsRange(0, 2)))
                .isInstanceOf(VerifyException.class)
                .hasMessage("Columnar scalar function returned 1 positions, expected 2");
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
