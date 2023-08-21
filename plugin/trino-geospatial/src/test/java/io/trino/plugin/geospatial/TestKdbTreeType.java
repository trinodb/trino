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
package io.trino.plugin.geospatial;

import io.trino.geospatial.KdbTree;
import io.trino.geospatial.KdbTree.Node;
import io.trino.geospatial.Rectangle;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.type.AbstractTestType;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.plugin.geospatial.KdbTreeType.KDB_TREE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestKdbTreeType
        extends AbstractTestType
{
    protected TestKdbTreeType()
    {
        super(KDB_TREE, KdbTree.class, createTestBlock());
    }

    private static Block createTestBlock()
    {
        BlockBuilder blockBuilder = KDB_TREE.createBlockBuilder(null, 1);
        KdbTree kdbTree = new KdbTree(
                new Node(
                        new Rectangle(10, 20, 30, 40),
                        OptionalInt.of(42),
                        Optional.empty(),
                        Optional.empty()));
        KDB_TREE.writeObject(blockBuilder, kdbTree);
        return blockBuilder.build();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        return null;
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
        Object sampleValue = getSampleValue();
        if (!type.isOrderable()) {
            assertThatThrownBy(() -> type.getPreviousValue(sampleValue))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Type is not orderable: " + type);
            return;
        }
        assertThat(type.getPreviousValue(sampleValue))
                .isEmpty();
    }

    @Test
    public void testNextValue()
    {
        Object sampleValue = getSampleValue();
        if (!type.isOrderable()) {
            assertThatThrownBy(() -> type.getNextValue(sampleValue))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Type is not orderable: " + type);
            return;
        }
        assertThat(type.getNextValue(sampleValue))
                .isEmpty();
    }
}
