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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.operator.FlatHashStrategyCompiler.compileFlatHashStrategy;
import static io.trino.operator.UpdateMemory.NOOP;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

class TestFlatHash
{
    private static final List<Type> TYPES = ImmutableList.of(BIGINT, VARCHAR);
    private static final FlatHashStrategy FLAT_HASH_STRATEGY = compileFlatHashStrategy(TYPES, new TypeOperators());

    private static final int DISTINCT_VALUES = 20_000;
    private static final int POSITIONS = DISTINCT_VALUES * 3;

    private static List<ControlMatcher> matchers()
    {
        return ImmutableList.of(new ScalarControlMatcher(), new VectorControlMatcher());
    }

    /**
     * The vectorized and the scalar matcher must build the exact same hash table, including the group id assignment,
     * which is only determined by the insertion order.
     */
    @ParameterizedTest
    @MethodSource("matchers")
    void testAgreesWithReferenceMapping(ControlMatcher matcher)
    {
        Block[] blocks = createBlocks();
        FlatHash flatHash = new FlatHash(FLAT_HASH_STRATEGY, true, 16, NOOP, matcher);

        for (int position = 0; position < POSITIONS; position++) {
            assertThat(flatHash.putIfAbsent(blocks, position)).isEqualTo(expectedGroupId(position));
        }
        assertThat(flatHash.size()).isEqualTo(DISTINCT_VALUES);

        for (int position = 0; position < POSITIONS; position++) {
            assertThat(flatHash.getIfPresent(blocks, position)).isEqualTo(expectedGroupId(position));
        }

        Block[] missingBlocks = createBlocks(DISTINCT_VALUES);
        for (int position = 0; position < POSITIONS; position++) {
            assertThat(flatHash.getIfPresent(missingBlocks, position)).isEqualTo(-1);
        }
    }

    @ParameterizedTest
    @MethodSource("matchers")
    void testAppendTo(ControlMatcher matcher)
    {
        Block[] blocks = createBlocks();
        FlatHash flatHash = new FlatHash(FLAT_HASH_STRATEGY, false, 16, NOOP, matcher);
        for (int position = 0; position < POSITIONS; position++) {
            flatHash.putIfAbsent(blocks, position);
        }

        PageBuilder pageBuilder = new PageBuilder(flatHash.size(), TYPES);
        BlockBuilder[] blockBuilders = new BlockBuilder[] {pageBuilder.getBlockBuilder(0), pageBuilder.getBlockBuilder(1)};
        for (int groupId = 0; groupId < flatHash.size(); groupId++) {
            pageBuilder.declarePosition();
            flatHash.appendTo(groupId, blockBuilders);
        }

        Page page = pageBuilder.build();
        Block bigintValues = page.getBlock(0);
        Block varcharValues = page.getBlock(1);
        for (int value = 0; value < DISTINCT_VALUES; value++) {
            // group ids are assigned in the order the values were first seen, which is the natural order of the values
            assertThat(BIGINT.getLong(bigintValues, value)).isEqualTo(value);
            assertThat(VARCHAR.getSlice(varcharValues, value)).isEqualTo(varcharValue(value));
        }
    }

    private static int expectedGroupId(int position)
    {
        return position % DISTINCT_VALUES;
    }

    private static Block[] createBlocks()
    {
        return createBlocks(0);
    }

    private static Block[] createBlocks(int offset)
    {
        PageBuilder pageBuilder = new PageBuilder(POSITIONS, TYPES);
        for (int position = 0; position < POSITIONS; position++) {
            int value = offset + expectedGroupId(position);
            pageBuilder.declarePosition();
            BIGINT.writeLong(pageBuilder.getBlockBuilder(0), value);
            VARCHAR.writeSlice(pageBuilder.getBlockBuilder(1), varcharValue(value));
        }
        Page page = pageBuilder.build();
        return new Block[] {page.getBlock(0), page.getBlock(1)};
    }

    private static Slice varcharValue(int value)
    {
        return utf8Slice("value " + value);
    }
}
