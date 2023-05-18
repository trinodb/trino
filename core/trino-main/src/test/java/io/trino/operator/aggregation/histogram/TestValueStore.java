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
package io.trino.operator.aggregation.histogram;

import io.trino.block.BlockAssertions;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;
import io.trino.type.BlockTypeOperators;
import io.trino.type.BlockTypeOperators.BlockPositionEqual;
import io.trino.type.BlockTypeOperators.BlockPositionHashCode;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestValueStore
{
    private ValueStore valueStore;
    private Block block;
    private BlockPositionHashCode hashCodeOperator;
    private ValueStore valueStoreSmall;

    @BeforeMethod(alwaysRun = true)
    public void setUp()
    {
        VarcharType type = VarcharType.createVarcharType(100);
        BlockTypeOperators blockTypeOperators = new BlockTypeOperators(new TypeOperators());
        BlockPositionEqual equalOperator = blockTypeOperators.getEqualOperator(type);
        hashCodeOperator = blockTypeOperators.getHashCodeOperator(type);
        BlockBuilder blockBuilder = type.createBlockBuilder(null, 100, 10);
        valueStore = new ValueStore(type, equalOperator, 100, blockBuilder);
        valueStoreSmall = new ValueStore(type, equalOperator, 1, blockBuilder);
        block = BlockAssertions.createStringsBlock("a", "b", "c", "d");
    }

    @Test
    public void testUniqueness()
    {
        assertThat(valueStore.addAndGetPosition(block, 0, hashCodeOperator.hashCode(block, 0))).isEqualTo(0);
        assertThat(valueStore.addAndGetPosition(block, 1, hashCodeOperator.hashCode(block, 1))).isEqualTo(1);
        assertThat(valueStore.addAndGetPosition(block, 2, hashCodeOperator.hashCode(block, 2))).isEqualTo(2);
        assertThat(valueStore.addAndGetPosition(block, 1, hashCodeOperator.hashCode(block, 1))).isEqualTo(1);
        assertThat(valueStore.addAndGetPosition(block, 3, hashCodeOperator.hashCode(block, 1))).isEqualTo(3);
    }

    @Test
    public void testTriggerRehash()
    {
        long hash0 = hashCodeOperator.hashCode(block, 0);
        long hash1 = hashCodeOperator.hashCode(block, 1);
        long hash2 = hashCodeOperator.hashCode(block, 2);

        assertThat(valueStoreSmall.addAndGetPosition(block, 0, hash0)).isEqualTo(0);
        assertThat(valueStoreSmall.addAndGetPosition(block, 1, hash1)).isEqualTo(1);

        // triggers rehash and hash1 will end up in position 3
        assertThat(valueStoreSmall.addAndGetPosition(block, 2, hash2)).isEqualTo(2);

        // this is just to make sure we trigger rehash code positions should be the same
        assertThat(valueStoreSmall.getRehashCount()).isPositive();
        assertThat(valueStoreSmall.addAndGetPosition(block, 0, hash0)).isEqualTo(0);
        assertThat(valueStoreSmall.addAndGetPosition(block, 1, hash1)).isEqualTo(1);
        assertThat(valueStoreSmall.addAndGetPosition(block, 2, hash2)).isEqualTo(2);
    }
}
