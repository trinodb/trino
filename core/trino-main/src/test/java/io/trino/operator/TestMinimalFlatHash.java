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
package io.trino.operator;

import io.trino.spi.block.Block;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.block.BlockAssertions.createRandomIntsBlock;
import static io.trino.block.BlockAssertions.createStringSequenceBlock;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMinimalFlatHash
{
    @Test
    public void testBasicPutIfAbsent()
    {
        TypeOperators operators = new TypeOperators();
        MinimalFlatHashStrategyCompiler compiler = new MinimalFlatHashStrategyCompiler(operators);
        MinimalFlatHashStrategy flatHashStrategy = compiler.getFlatHashStrategy(List.of(VARCHAR, INTEGER, VARCHAR));
        MinimalFlatHash flatHash = new MinimalFlatHash(flatHashStrategy, FlatGroupByHash.HashMode.CACHED, 10_000, () -> true);
        Block[] blocks = new Block[] {
                createStringSequenceBlock(0, 10),
                createRandomIntsBlock(10, 0.5f),
                createStringSequenceBlock(10, 20)
        };
        for (int i = 0; i < 10; i++) {
            assertThat(flatHash.putIfAbsent(blocks, i)).isEqualTo(i);
        }
        for (int i = 0; i < 10; i++) {
            assertThat(flatHash.putIfAbsent(blocks, i)).isEqualTo(i);
        }
    }
}
