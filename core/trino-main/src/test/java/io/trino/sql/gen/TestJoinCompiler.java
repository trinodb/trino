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
package io.trino.sql.gen;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.trino.block.BlockAssertions;
import io.trino.operator.PagesHashStrategy;
import io.trino.operator.SimplePagesHashStrategy;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.JoinCompiler.PagesHashStrategyFactory;
import io.trino.type.BlockTypeOperators;
import io.trino.type.BlockTypeOperators.BlockPositionEqual;
import io.trino.type.BlockTypeOperators.BlockPositionHashCode;
import io.trino.type.BlockTypeOperators.BlockPositionIsDistinctFrom;
import io.trino.type.TypeTestUtils;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.operator.PageAssertions.assertPageEquals;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestJoinCompiler
{
    private static final TypeOperators typeOperators = new TypeOperators();
    private static final BlockTypeOperators blockTypeOperators = new BlockTypeOperators(typeOperators);
    private static final JoinCompiler joinCompiler = new JoinCompiler(typeOperators);

    @DataProvider(name = "hashEnabledValues")
    public static Object[][] hashEnabledValuesProvider()
    {
        return new Object[][] {{true}, {false}};
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testSingleChannel(boolean hashEnabled)
    {
        List<Type> joinTypes = ImmutableList.of(VARCHAR);
        List<Integer> joinChannels = Ints.asList(0);

        // compile a single channel hash strategy
        PagesHashStrategyFactory pagesHashStrategyFactory = joinCompiler.compilePagesHashStrategyFactory(joinTypes, joinChannels);

        // create hash strategy with a single channel blocks -- make sure there is some overlap in values
        ObjectArrayList<Block> channel = new ObjectArrayList<>();
        channel.add(BlockAssertions.createStringSequenceBlock(10, 20));
        channel.add(BlockAssertions.createStringSequenceBlock(20, 30));
        channel.add(BlockAssertions.createStringSequenceBlock(15, 25));

        OptionalInt hashChannel = OptionalInt.empty();
        List<ObjectArrayList<Block>> channels = ImmutableList.of(channel);
        if (hashEnabled) {
            ObjectArrayList<Block> hashChannelBuilder = new ObjectArrayList<>();
            for (Block block : channel) {
                hashChannelBuilder.add(TypeTestUtils.getHashBlock(joinTypes, block));
            }
            hashChannel = OptionalInt.of(1);
            channels = ImmutableList.of(channel, hashChannelBuilder);
        }
        PagesHashStrategy hashStrategy = pagesHashStrategyFactory.createPagesHashStrategy(channels, hashChannel);

        // verify channel count
        assertEquals(hashStrategy.getChannelCount(), 1);

        BlockTypeOperators blockTypeOperators = new BlockTypeOperators();
        BlockPositionEqual equalOperator = blockTypeOperators.getEqualOperator(VARCHAR);
        BlockPositionIsDistinctFrom distinctFromOperator = blockTypeOperators.getDistinctFromOperator(VARCHAR);
        BlockPositionHashCode hashCodeOperator = blockTypeOperators.getHashCodeOperator(VARCHAR);

        // verify hashStrategy is consistent with equals and hash code from block
        for (int leftBlockIndex = 0; leftBlockIndex < channel.size(); leftBlockIndex++) {
            Block leftBlock = channel.get(leftBlockIndex);

            PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(VARCHAR));

            for (int leftBlockPosition = 0; leftBlockPosition < leftBlock.getPositionCount(); leftBlockPosition++) {
                // hash code of position must match block hash
                assertEquals(hashStrategy.hashPosition(leftBlockIndex, leftBlockPosition), hashCodeOperator.hashCodeNullSafe(leftBlock, leftBlockPosition));

                // position must be equal to itself
                assertTrue(hashStrategy.positionEqualsPositionIgnoreNulls(leftBlockIndex, leftBlockPosition, leftBlockIndex, leftBlockPosition));

                // check equality of every position against every other position in the block
                for (int rightBlockIndex = 0; rightBlockIndex < channel.size(); rightBlockIndex++) {
                    Block rightBlock = channel.get(rightBlockIndex);
                    for (int rightBlockPosition = 0; rightBlockPosition < rightBlock.getPositionCount(); rightBlockPosition++) {
                        boolean expected = equalOperator.equalNullSafe(leftBlock, leftBlockPosition, rightBlock, rightBlockPosition);
                        boolean expectedNotDistinct = !distinctFromOperator.isDistinctFrom(leftBlock, leftBlockPosition, rightBlock, rightBlockPosition);
                        assertEquals(hashStrategy.positionEqualsRow(leftBlockIndex, leftBlockPosition, rightBlockPosition, new Page(rightBlock)), expected);
                        assertEquals(hashStrategy.positionNotDistinctFromRow(leftBlockIndex, leftBlockPosition, rightBlockPosition, new Page(rightBlock)), expectedNotDistinct);
                        assertEquals(hashStrategy.rowEqualsRow(leftBlockPosition, new Page(leftBlock), rightBlockPosition, new Page(rightBlock)), expected);
                        assertEquals(hashStrategy.rowNotDistinctFromRow(leftBlockPosition, new Page(leftBlock), rightBlockPosition, new Page(rightBlock)), expectedNotDistinct);
                        assertEquals(hashStrategy.positionEqualsRowIgnoreNulls(leftBlockIndex, leftBlockPosition, rightBlockPosition, new Page(rightBlock)), expected);
                        assertEquals(hashStrategy.positionEqualsPositionIgnoreNulls(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition), expected);
                        assertEquals(hashStrategy.positionEqualsPosition(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition), expected);
                        assertEquals(hashStrategy.positionNotDistinctFromPosition(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition), expectedNotDistinct);
                    }
                }

                // check equality of every position against every other position in the block cursor
                for (int rightBlockIndex = 0; rightBlockIndex < channel.size(); rightBlockIndex++) {
                    Block rightBlock = channel.get(rightBlockIndex);
                    for (int rightBlockPosition = 0; rightBlockPosition < rightBlock.getPositionCount(); rightBlockPosition++) {
                        boolean expected = equalOperator.equalNullSafe(leftBlock, leftBlockPosition, rightBlock, rightBlockPosition);
                        boolean expectedNotDistinct = !distinctFromOperator.isDistinctFrom(leftBlock, leftBlockPosition, rightBlock, rightBlockPosition);
                        assertEquals(hashStrategy.positionEqualsRow(leftBlockIndex, leftBlockPosition, rightBlockPosition, new Page(rightBlock)), expected);
                        assertEquals(hashStrategy.positionNotDistinctFromRow(leftBlockIndex, leftBlockPosition, rightBlockPosition, new Page(rightBlock)), expectedNotDistinct);
                        assertEquals(hashStrategy.rowEqualsRow(leftBlockPosition, new Page(leftBlock), rightBlockPosition, new Page(rightBlock)), expected);
                        assertEquals(hashStrategy.rowNotDistinctFromRow(leftBlockPosition, new Page(leftBlock), rightBlockPosition, new Page(rightBlock)), expectedNotDistinct);
                        assertEquals(hashStrategy.positionEqualsRowIgnoreNulls(leftBlockIndex, leftBlockPosition, rightBlockPosition, new Page(rightBlock)), expected);
                        assertEquals(hashStrategy.positionEqualsPositionIgnoreNulls(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition), expected);
                        assertEquals(hashStrategy.positionEqualsPosition(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition), expected);
                        assertEquals(hashStrategy.positionNotDistinctFromPosition(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition), expectedNotDistinct);
                    }
                }

                // write position to output block
                pageBuilder.declarePosition();
                hashStrategy.appendTo(leftBlockIndex, leftBlockPosition, pageBuilder, 0);
            }

            // verify output block matches
            assertBlockEquals(VARCHAR, pageBuilder.build().getBlock(0), leftBlock);
        }
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testMultiChannel(boolean hashEnabled)
    {
        // compile a single channel hash strategy
        List<Type> types = ImmutableList.of(VARCHAR, VARCHAR, BIGINT, DOUBLE, BOOLEAN, VARCHAR);
        List<Type> joinTypes = ImmutableList.of(VARCHAR, BIGINT, DOUBLE, BOOLEAN);
        List<Type> outputTypes = ImmutableList.of(VARCHAR, BIGINT, DOUBLE, BOOLEAN, VARCHAR);
        List<Integer> joinChannels = Ints.asList(1, 2, 3, 4);
        List<Integer> outputChannels = Ints.asList(1, 2, 3, 4, 0);

        // crate hash strategy with a single channel blocks -- make sure there is some overlap in values
        ObjectArrayList<Block> extraChannel = new ObjectArrayList<>();
        extraChannel.add(BlockAssertions.createStringSequenceBlock(10, 20));
        extraChannel.add(BlockAssertions.createStringSequenceBlock(20, 30));
        extraChannel.add(BlockAssertions.createStringSequenceBlock(15, 25));
        ObjectArrayList<Block> varcharChannel = new ObjectArrayList<>();
        varcharChannel.add(BlockAssertions.createStringSequenceBlock(10, 20));
        varcharChannel.add(BlockAssertions.createStringSequenceBlock(20, 30));
        varcharChannel.add(BlockAssertions.createStringSequenceBlock(15, 25));
        ObjectArrayList<Block> longChannel = new ObjectArrayList<>();
        longChannel.add(BlockAssertions.createLongSequenceBlock(10, 20));
        longChannel.add(BlockAssertions.createLongSequenceBlock(20, 30));
        longChannel.add(BlockAssertions.createLongSequenceBlock(15, 25));
        ObjectArrayList<Block> doubleChannel = new ObjectArrayList<>();
        doubleChannel.add(BlockAssertions.createDoubleSequenceBlock(10, 20));
        doubleChannel.add(BlockAssertions.createDoubleSequenceBlock(20, 30));
        doubleChannel.add(BlockAssertions.createDoubleSequenceBlock(15, 25));
        ObjectArrayList<Block> booleanChannel = new ObjectArrayList<>();
        booleanChannel.add(BlockAssertions.createBooleanSequenceBlock(10, 20));
        booleanChannel.add(BlockAssertions.createBooleanSequenceBlock(20, 30));
        booleanChannel.add(BlockAssertions.createBooleanSequenceBlock(15, 25));
        ObjectArrayList<Block> extraUnusedChannel = new ObjectArrayList<>();
        extraUnusedChannel.add(BlockAssertions.createBooleanSequenceBlock(10, 20));
        extraUnusedChannel.add(BlockAssertions.createBooleanSequenceBlock(20, 30));
        extraUnusedChannel.add(BlockAssertions.createBooleanSequenceBlock(15, 25));

        OptionalInt hashChannel = OptionalInt.empty();
        ImmutableList<ObjectArrayList<Block>> channels = ImmutableList.of(extraChannel, varcharChannel, longChannel, doubleChannel, booleanChannel, extraUnusedChannel);
        ObjectArrayList<Block> precomputedHash = new ObjectArrayList<>();
        if (hashEnabled) {
            for (int i = 0; i < 3; i++) {
                precomputedHash.add(TypeTestUtils.getHashBlock(joinTypes, varcharChannel.get(i), longChannel.get(i), doubleChannel.get(i), booleanChannel.get(i)));
            }
            hashChannel = OptionalInt.of(6);
            channels = ImmutableList.of(extraChannel, varcharChannel, longChannel, doubleChannel, booleanChannel, extraUnusedChannel, precomputedHash);
            types = ImmutableList.of(VARCHAR, VARCHAR, BIGINT, DOUBLE, BOOLEAN, VARCHAR, BIGINT);
            outputTypes = ImmutableList.of(VARCHAR, BIGINT, DOUBLE, BOOLEAN, VARCHAR, BIGINT);
            outputChannels = Ints.asList(1, 2, 3, 4, 0, 6);
        }

        PagesHashStrategyFactory pagesHashStrategyFactory = joinCompiler.compilePagesHashStrategyFactory(types, joinChannels, Optional.of(outputChannels));
        PagesHashStrategy hashStrategy = pagesHashStrategyFactory.createPagesHashStrategy(channels, hashChannel);
        // todo add tests for filter function
        PagesHashStrategy expectedHashStrategy = new SimplePagesHashStrategy(types, outputChannels, channels, joinChannels, hashChannel, Optional.empty(), blockTypeOperators);

        // verify channel count
        assertEquals(hashStrategy.getChannelCount(), outputChannels.size());
        // verify size
        int instanceSize = instanceSize(hashStrategy.getClass());
        long sizeInBytes = instanceSize +
                (channels.size() > 0 ? sizeOf(channels.get(0).elements()) * channels.size() : 0) +
                channels.stream()
                        .flatMap(List::stream)
                        .mapToLong(Block::getRetainedSizeInBytes)
                        .sum();
        assertEquals(hashStrategy.getSizeInBytes(), sizeInBytes);

        // verify hashStrategy is consistent with equals and hash code from block
        for (int leftBlockIndex = 0; leftBlockIndex < varcharChannel.size(); leftBlockIndex++) {
            PageBuilder pageBuilder = new PageBuilder(outputTypes);

            Block[] leftBlocks = new Block[4];
            leftBlocks[0] = varcharChannel.get(leftBlockIndex);
            leftBlocks[1] = longChannel.get(leftBlockIndex);
            leftBlocks[2] = doubleChannel.get(leftBlockIndex);
            leftBlocks[3] = booleanChannel.get(leftBlockIndex);

            int leftPositionCount = varcharChannel.get(leftBlockIndex).getPositionCount();
            for (int leftBlockPosition = 0; leftBlockPosition < leftPositionCount; leftBlockPosition++) {
                // hash code of position must match block hash
                assertEquals(
                        hashStrategy.hashPosition(leftBlockIndex, leftBlockPosition),
                        expectedHashStrategy.hashPosition(leftBlockIndex, leftBlockPosition));

                // position must be equal to itself
                assertTrue(hashStrategy.positionEqualsPositionIgnoreNulls(leftBlockIndex, leftBlockPosition, leftBlockIndex, leftBlockPosition));
                assertTrue(hashStrategy.positionEqualsPosition(leftBlockIndex, leftBlockPosition, leftBlockIndex, leftBlockPosition));
                assertTrue(hashStrategy.positionNotDistinctFromPosition(leftBlockIndex, leftBlockPosition, leftBlockIndex, leftBlockPosition));

                // check equality of every position against every other position in the block
                for (int rightBlockIndex = 0; rightBlockIndex < varcharChannel.size(); rightBlockIndex++) {
                    Block rightBlock = varcharChannel.get(rightBlockIndex);
                    for (int rightBlockPosition = 0; rightBlockPosition < rightBlock.getPositionCount(); rightBlockPosition++) {
                        assertEquals(
                                hashStrategy.positionEqualsPositionIgnoreNulls(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition),
                                expectedHashStrategy.positionEqualsPositionIgnoreNulls(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition));
                        assertEquals(
                                hashStrategy.positionEqualsPosition(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition),
                                expectedHashStrategy.positionEqualsPosition(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition));
                        assertEquals(
                                hashStrategy.positionNotDistinctFromPosition(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition),
                                expectedHashStrategy.positionNotDistinctFromPosition(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition));
                    }
                }

                // check equality of every position against every other position in the block cursor
                for (int rightBlockIndex = 0; rightBlockIndex < varcharChannel.size(); rightBlockIndex++) {
                    Block[] rightBlocks = new Block[4];
                    rightBlocks[0] = varcharChannel.get(rightBlockIndex);
                    rightBlocks[1] = longChannel.get(rightBlockIndex);
                    rightBlocks[2] = doubleChannel.get(rightBlockIndex);
                    rightBlocks[3] = booleanChannel.get(rightBlockIndex);

                    int rightPositionCount = varcharChannel.get(rightBlockIndex).getPositionCount();
                    for (int rightPosition = 0; rightPosition < rightPositionCount; rightPosition++) {
                        boolean expected = expectedHashStrategy.positionEqualsRow(leftBlockIndex, leftBlockPosition, rightPosition, new Page(rightBlocks));
                        boolean expectedNotDistinct = expectedHashStrategy.positionNotDistinctFromRow(leftBlockIndex, leftBlockPosition, rightPosition, new Page(rightBlocks));

                        assertEquals(hashStrategy.positionEqualsRow(leftBlockIndex, leftBlockPosition, rightPosition, new Page(rightBlocks)), expected);
                        assertEquals(hashStrategy.positionNotDistinctFromRow(leftBlockIndex, leftBlockPosition, rightPosition, new Page(rightBlocks)), expectedNotDistinct);
                        assertEquals(hashStrategy.rowEqualsRow(leftBlockPosition, new Page(leftBlocks), rightPosition, new Page(rightBlocks)), expected);
                        assertEquals(hashStrategy.rowNotDistinctFromRow(leftBlockPosition, new Page(leftBlocks), rightPosition, new Page(rightBlocks)), expectedNotDistinct);
                        assertEquals(hashStrategy.positionEqualsRowIgnoreNulls(leftBlockIndex, leftBlockPosition, rightPosition, new Page(rightBlocks)), expected);
                    }
                }

                // write position to output block
                pageBuilder.declarePosition();
                hashStrategy.appendTo(leftBlockIndex, leftBlockPosition, pageBuilder, 0);
            }

            // verify output block matches
            Page page = pageBuilder.build();
            if (hashEnabled) {
                assertPageEquals(outputTypes, page, new Page(
                        varcharChannel.get(leftBlockIndex),
                        longChannel.get(leftBlockIndex),
                        doubleChannel.get(leftBlockIndex),
                        booleanChannel.get(leftBlockIndex),
                        extraChannel.get(leftBlockIndex),
                        precomputedHash.get(leftBlockIndex)));
            }
            else {
                assertPageEquals(outputTypes, page, new Page(
                        varcharChannel.get(leftBlockIndex),
                        longChannel.get(leftBlockIndex),
                        doubleChannel.get(leftBlockIndex),
                        booleanChannel.get(leftBlockIndex),
                        extraChannel.get(leftBlockIndex)));
            }
        }
    }

    @Test
    public void testDistinctFrom()
    {
        List<Type> joinTypes = ImmutableList.of(DOUBLE);
        List<Integer> joinChannels = Ints.asList(0);

        // compile a single channel hash strategy
        PagesHashStrategyFactory pagesHashStrategyFactory = joinCompiler.compilePagesHashStrategyFactory(joinTypes, joinChannels);

        // create hash strategy with a single channel blocks -- make sure there is some overlap in values
        ObjectArrayList<Block> channel = new ObjectArrayList<>();
        channel.add(BlockAssertions.createDoubleSequenceBlock(10, 20));
        channel.add(BlockAssertions.createDoublesBlock(Double.NaN, null, Double.NaN, 1.0, null));
        channel.add(BlockAssertions.createDoubleSequenceBlock(20, 30));
        channel.add(BlockAssertions.createDoubleSequenceBlock(15, 25));

        PagesHashStrategy hashStrategy = pagesHashStrategyFactory.createPagesHashStrategy(ImmutableList.of(channel), OptionalInt.empty());

        // verify channel count
        assertEquals(hashStrategy.getChannelCount(), 1);

        BlockTypeOperators blockTypeOperators = new BlockTypeOperators();
        BlockPositionIsDistinctFrom distinctFromOperator = blockTypeOperators.getDistinctFromOperator(DOUBLE);

        // verify hashStrategy is consistent with DISTINCT from block
        for (int leftBlockIndex = 0; leftBlockIndex < channel.size(); leftBlockIndex++) {
            Block leftBlock = channel.get(leftBlockIndex);

            for (int leftBlockPosition = 0; leftBlockPosition < leftBlock.getPositionCount(); leftBlockPosition++) {
                // position must not be distinct from itself
                assertTrue(hashStrategy.positionNotDistinctFromPosition(leftBlockIndex, leftBlockPosition, leftBlockIndex, leftBlockPosition));

                // check distinctiveness of every position against every other position in the block
                for (int rightBlockIndex = 0; rightBlockIndex < channel.size(); rightBlockIndex++) {
                    Block rightBlock = channel.get(rightBlockIndex);
                    for (int rightBlockPosition = 0; rightBlockPosition < rightBlock.getPositionCount(); rightBlockPosition++) {
                        boolean expectedNotDistinct = !distinctFromOperator.isDistinctFrom(leftBlock, leftBlockPosition, rightBlock, rightBlockPosition);
                        assertEquals(hashStrategy.positionNotDistinctFromRow(leftBlockIndex, leftBlockPosition, rightBlockPosition, new Page(rightBlock)), expectedNotDistinct);
                        assertEquals(hashStrategy.rowNotDistinctFromRow(leftBlockPosition, new Page(leftBlock), rightBlockPosition, new Page(rightBlock)), expectedNotDistinct);
                        assertEquals(hashStrategy.positionNotDistinctFromPosition(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition), expectedNotDistinct);
                    }
                }

                // check equality of every position against every other position in the block cursor
                for (int rightBlockIndex = 0; rightBlockIndex < channel.size(); rightBlockIndex++) {
                    Block rightBlock = channel.get(rightBlockIndex);
                    for (int rightBlockPosition = 0; rightBlockPosition < rightBlock.getPositionCount(); rightBlockPosition++) {
                        boolean expectedNotDistinct = !distinctFromOperator.isDistinctFrom(leftBlock, leftBlockPosition, rightBlock, rightBlockPosition);
                        assertEquals(hashStrategy.positionNotDistinctFromRow(leftBlockIndex, leftBlockPosition, rightBlockPosition, new Page(rightBlock)), expectedNotDistinct);
                        assertEquals(hashStrategy.rowNotDistinctFromRow(leftBlockPosition, new Page(leftBlock), rightBlockPosition, new Page(rightBlock)), expectedNotDistinct);
                        assertEquals(hashStrategy.positionNotDistinctFromPosition(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition), expectedNotDistinct);
                    }
                }
            }
        }
    }
}
