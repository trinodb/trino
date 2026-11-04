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
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.JoinCompiler.PagesHashStrategyFactory;
import io.trino.type.BlockTypeOperators;
import io.trino.type.BlockTypeOperators.BlockPositionEqual;
import io.trino.type.BlockTypeOperators.BlockPositionHashCode;
import io.trino.type.BlockTypeOperators.BlockPositionIsIdentical;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.operator.PageAssertions.assertPageEquals;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class TestJoinCompiler
{
    private final TypeOperators typeOperators = new TypeOperators();
    private final BlockTypeOperators blockTypeOperators = new BlockTypeOperators(typeOperators);
    private final JoinCompiler joinCompiler = new JoinCompiler(typeOperators);

    @Test
    void testHashStrategyIsSharedWithLookupSourceFactory()
    {
        // a compiler of its own, so the assertion does not depend on what other tests compiled
        JoinCompiler joinCompiler = new JoinCompiler(typeOperators);
        List<Type> types = ImmutableList.of(BIGINT, VARCHAR);
        List<Integer> joinChannels = ImmutableList.of(0);

        // the lookup source takes its strategy from the cache rather than compiling its own
        joinCompiler.compileLookupSourceFactory(types, joinChannels, OptionalInt.empty(), Optional.empty());
        assertThat(joinCompiler.getHashStrategiesStats().getLoadCount()).isEqualTo(1);

        // so the equivalent factory reuses that strategy instead of compiling a second one
        joinCompiler.compilePagesHashStrategyFactory(types, joinChannels);
        assertThat(joinCompiler.getHashStrategiesStats().getLoadCount()).isEqualTo(1);
    }

    @Test
    void testEmptyChannels()
    {
        // compile hash strategy with empty join channels
        PagesHashStrategyFactory pagesHashStrategyFactory = joinCompiler.compilePagesHashStrategyFactory(ImmutableList.of(BIGINT), ImmutableList.of());

        // create hash strategy with a single channel blocks
        ObjectArrayList<Block> channel = new ObjectArrayList<>();
        channel.add(BlockAssertions.createStringSequenceBlock(10, 20));
        channel.add(BlockAssertions.createStringSequenceBlock(20, 30));
        channel.add(BlockAssertions.createStringSequenceBlock(15, 25));

        List<ObjectArrayList<Block>> channels = ImmutableList.of(channel);
        PagesHashStrategy hashStrategy = pagesHashStrategyFactory.createPagesHashStrategy(channels);

        assertThat(hashStrategy.getChannelCount()).isEqualTo(1);
        assertThat(hashStrategy.getSizeInBytes()).isGreaterThan(0L);
        assertThat(hashStrategy.hashRow(0, new Page(channel.get(0)))).isEqualTo(0L);
        assertThat(hashStrategy.hashPosition(0, 5)).isEqualTo(0L);
        assertThat(hashStrategy.positionEqualsPositionIgnoreNulls(0, 5, 0, 5)).isTrue();
        assertThat(hashStrategy.positionIdenticalToPosition(0, 5, 0, 5)).isTrue();
        assertThat(hashStrategy.positionIdenticalToRow(0, 5, 5, new Page(channel.get(0)))).isTrue();
        assertThat(hashStrategy.rowIdenticalToRow(5, new Page(channel.get(0)), 5, new Page(channel.get(0)))).isTrue();
        assertThat(hashStrategy.positionEqualsRowIgnoreNulls(0, 5, 5, new Page(channel.get(0)))).isTrue();
        assertThat(hashStrategy.positionEqualsPositionIgnoreNulls(0, 5, 0, 5)).isTrue();
    }

    @Test
    public void testSingleChannel()
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

        List<ObjectArrayList<Block>> channels = ImmutableList.of(channel);
        PagesHashStrategy hashStrategy = pagesHashStrategyFactory.createPagesHashStrategy(channels);

        // verify channel count
        assertThat(hashStrategy.getChannelCount()).isEqualTo(1);

        BlockTypeOperators blockTypeOperators = new BlockTypeOperators();
        BlockPositionEqual equalOperator = blockTypeOperators.getEqualOperator(VARCHAR);
        BlockPositionIsIdentical identicalOperator = blockTypeOperators.getIdenticalOperator(VARCHAR);
        BlockPositionHashCode hashCodeOperator = blockTypeOperators.getHashCodeOperator(VARCHAR);

        // verify hashStrategy is consistent with equals and hash code from block
        for (int leftBlockIndex = 0; leftBlockIndex < channel.size(); leftBlockIndex++) {
            Block leftBlock = channel.get(leftBlockIndex);

            PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(VARCHAR));

            for (int leftBlockPosition = 0; leftBlockPosition < leftBlock.getPositionCount(); leftBlockPosition++) {
                // hash code of position must match block hash
                assertThat(hashStrategy.hashPosition(leftBlockIndex, leftBlockPosition)).isEqualTo(hashCodeOperator.hashCodeNullSafe(leftBlock, leftBlockPosition));

                // position must be equal to itself
                assertThat(hashStrategy.positionEqualsPositionIgnoreNulls(leftBlockIndex, leftBlockPosition, leftBlockIndex, leftBlockPosition)).isTrue();

                // check equality of every position against every other position in the block
                for (int rightBlockIndex = 0; rightBlockIndex < channel.size(); rightBlockIndex++) {
                    Block rightBlock = channel.get(rightBlockIndex);
                    for (int rightBlockPosition = 0; rightBlockPosition < rightBlock.getPositionCount(); rightBlockPosition++) {
                        boolean expected = equalOperator.equalNullSafe(leftBlock, leftBlockPosition, rightBlock, rightBlockPosition);
                        boolean expectedIdentical = identicalOperator.isIdentical(leftBlock, leftBlockPosition, rightBlock, rightBlockPosition);
                        assertThat(hashStrategy.positionIdenticalToRow(leftBlockIndex, leftBlockPosition, rightBlockPosition, new Page(rightBlock))).isEqualTo(expectedIdentical);
                        assertThat(hashStrategy.rowIdenticalToRow(leftBlockPosition, new Page(leftBlock), rightBlockPosition, new Page(rightBlock))).isEqualTo(expectedIdentical);
                        assertThat(hashStrategy.positionEqualsRowIgnoreNulls(leftBlockIndex, leftBlockPosition, rightBlockPosition, new Page(rightBlock))).isEqualTo(expected);
                        assertThat(hashStrategy.positionEqualsPositionIgnoreNulls(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition)).isEqualTo(expected);
                        assertThat(hashStrategy.positionIdenticalToPosition(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition)).isEqualTo(expectedIdentical);
                    }
                }

                // check equality of every position against every other position in the block cursor
                for (int rightBlockIndex = 0; rightBlockIndex < channel.size(); rightBlockIndex++) {
                    Block rightBlock = channel.get(rightBlockIndex);
                    for (int rightBlockPosition = 0; rightBlockPosition < rightBlock.getPositionCount(); rightBlockPosition++) {
                        boolean expected = equalOperator.equalNullSafe(leftBlock, leftBlockPosition, rightBlock, rightBlockPosition);
                        boolean expectedIdentical = identicalOperator.isIdentical(leftBlock, leftBlockPosition, rightBlock, rightBlockPosition);
                        assertThat(hashStrategy.positionIdenticalToRow(leftBlockIndex, leftBlockPosition, rightBlockPosition, new Page(rightBlock))).isEqualTo(expectedIdentical);
                        assertThat(hashStrategy.rowIdenticalToRow(leftBlockPosition, new Page(leftBlock), rightBlockPosition, new Page(rightBlock))).isEqualTo(expectedIdentical);
                        assertThat(hashStrategy.positionEqualsRowIgnoreNulls(leftBlockIndex, leftBlockPosition, rightBlockPosition, new Page(rightBlock))).isEqualTo(expected);
                        assertThat(hashStrategy.positionEqualsPositionIgnoreNulls(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition)).isEqualTo(expected);
                        assertThat(hashStrategy.positionIdenticalToPosition(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition)).isEqualTo(expectedIdentical);
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

    @Test
    public void testMultiChannel()
    {
        // compile a single channel hash strategy
        List<Type> types = ImmutableList.of(VARCHAR, VARCHAR, BIGINT, DOUBLE, BOOLEAN, VARCHAR);
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

        List<ObjectArrayList<Block>> channels = ImmutableList.of(extraChannel, varcharChannel, longChannel, doubleChannel, booleanChannel, extraUnusedChannel);
        PagesHashStrategyFactory pagesHashStrategyFactory = joinCompiler.compilePagesHashStrategyFactory(types, joinChannels, Optional.of(outputChannels));
        PagesHashStrategy hashStrategy = pagesHashStrategyFactory.createPagesHashStrategy(channels);
        // todo add tests for filter function
        PagesHashStrategy expectedHashStrategy = new SimplePagesHashStrategy(types, outputChannels, channels, joinChannels, Optional.empty(), blockTypeOperators);

        // verify channel count
        assertThat(hashStrategy.getChannelCount()).isEqualTo(outputChannels.size());
        // verify size
        int instanceSize = instanceSize(hashStrategy.getClass());
        long sizeInBytes = instanceSize +
                (channels.size() > 0 ? sizeOf(channels.get(0).elements()) * channels.size() : 0) +
                channels.stream()
                        .flatMap(List::stream)
                        .mapToLong(Block::getRetainedSizeInBytes)
                        .sum();
        assertThat(hashStrategy.getSizeInBytes()).isEqualTo(sizeInBytes);

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
                assertThat(hashStrategy.hashPosition(leftBlockIndex, leftBlockPosition)).isEqualTo(expectedHashStrategy.hashPosition(leftBlockIndex, leftBlockPosition));

                // position must be equal to itself
                assertThat(hashStrategy.positionEqualsPositionIgnoreNulls(leftBlockIndex, leftBlockPosition, leftBlockIndex, leftBlockPosition)).isTrue();
                assertThat(hashStrategy.positionIdenticalToPosition(leftBlockIndex, leftBlockPosition, leftBlockIndex, leftBlockPosition)).isTrue();

                // check equality of every position against every other position in the block
                for (int rightBlockIndex = 0; rightBlockIndex < varcharChannel.size(); rightBlockIndex++) {
                    Block rightBlock = varcharChannel.get(rightBlockIndex);
                    for (int rightBlockPosition = 0; rightBlockPosition < rightBlock.getPositionCount(); rightBlockPosition++) {
                        assertThat(hashStrategy.positionEqualsPositionIgnoreNulls(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition)).isEqualTo(expectedHashStrategy.positionEqualsPositionIgnoreNulls(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition));
                        assertThat(hashStrategy.positionIdenticalToPosition(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition)).isEqualTo(expectedHashStrategy.positionIdenticalToPosition(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition));
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
                        boolean expected = expectedHashStrategy.positionEqualsRowIgnoreNulls(leftBlockIndex, leftBlockPosition, rightPosition, new Page(rightBlocks));
                        boolean expectedIdentical = expectedHashStrategy.positionIdenticalToRow(leftBlockIndex, leftBlockPosition, rightPosition, new Page(rightBlocks));

                        assertThat(hashStrategy.positionIdenticalToRow(leftBlockIndex, leftBlockPosition, rightPosition, new Page(rightBlocks))).isEqualTo(expectedIdentical);
                        assertThat(hashStrategy.rowIdenticalToRow(leftBlockPosition, new Page(leftBlocks), rightPosition, new Page(rightBlocks))).isEqualTo(expectedIdentical);
                        assertThat(hashStrategy.positionEqualsRowIgnoreNulls(leftBlockIndex, leftBlockPosition, rightPosition, new Page(rightBlocks))).isEqualTo(expected);
                    }
                }

                // write position to output block
                pageBuilder.declarePosition();
                hashStrategy.appendTo(leftBlockIndex, leftBlockPosition, pageBuilder, 0);
            }

            // verify output block matches
            Page page = pageBuilder.build();
            assertPageEquals(outputTypes, page, new Page(
                    varcharChannel.get(leftBlockIndex),
                    longChannel.get(leftBlockIndex),
                    doubleChannel.get(leftBlockIndex),
                    booleanChannel.get(leftBlockIndex),
                    extraChannel.get(leftBlockIndex)));
        }
    }

    @Test
    void testStructuralChannel()
    {
        // ARRAY equality has a nullable return; compiling the hash strategy must adapt it to DEFAULT_ON_NULL.
        Type arrayType = new ArrayType(BIGINT);
        List<Integer> joinChannels = Ints.asList(0);

        PagesHashStrategyFactory pagesHashStrategyFactory = joinCompiler.compilePagesHashStrategyFactory(ImmutableList.of(arrayType), joinChannels);

        ObjectArrayList<Block> channel = new ObjectArrayList<>();
        channel.add(BlockAssertions.createArrayBigintBlock(asList(
                asList(1L, 2L),
                asList(1L, null),
                null,
                asList(1L, 2L),
                asList(3L))));

        PagesHashStrategy hashStrategy = pagesHashStrategyFactory.createPagesHashStrategy(ImmutableList.of(channel));

        assertThat(hashStrategy.getChannelCount()).isEqualTo(1);

        BlockTypeOperators blockTypeOperators = new BlockTypeOperators();
        BlockPositionEqual equalOperator = blockTypeOperators.getEqualOperator(arrayType);
        BlockPositionIsIdentical identicalOperator = blockTypeOperators.getIdenticalOperator(arrayType);

        Block block = channel.get(0);
        for (int leftPosition = 0; leftPosition < block.getPositionCount(); leftPosition++) {
            for (int rightPosition = 0; rightPosition < block.getPositionCount(); rightPosition++) {
                boolean expectedIdentical = identicalOperator.isIdentical(block, leftPosition, block, rightPosition);
                assertThat(hashStrategy.positionIdenticalToPosition(0, leftPosition, 0, rightPosition)).isEqualTo(expectedIdentical);
                assertThat(hashStrategy.positionIdenticalToRow(0, leftPosition, rightPosition, new Page(block))).isEqualTo(expectedIdentical);
                assertThat(hashStrategy.rowIdenticalToRow(leftPosition, new Page(block), rightPosition, new Page(block))).isEqualTo(expectedIdentical);

                // positionEquals*IgnoreNulls callers guarantee non-null inputs; a non-null array with a null element compares as no match.
                if (!block.isNull(leftPosition) && !block.isNull(rightPosition)) {
                    boolean expected = equalOperator.equal(block, leftPosition, block, rightPosition);
                    assertThat(hashStrategy.positionEqualsPositionIgnoreNulls(0, leftPosition, 0, rightPosition)).isEqualTo(expected);
                    assertThat(hashStrategy.positionEqualsRowIgnoreNulls(0, leftPosition, rightPosition, new Page(block))).isEqualTo(expected);
                }
            }
        }
    }

    @Test
    void testManyOutputChannels()
    {
        // the per-channel appendTo code of 3000 channels exceeds the 64KB JVM method size limit unless it is split
        int channelCount = 3000;
        ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
        ImmutableList.Builder<ObjectArrayList<Block>> channels = ImmutableList.builder();
        for (int channel = 0; channel < channelCount; channel++) {
            ObjectArrayList<Block> blocks = new ObjectArrayList<>();
            if (channel % 2 == 0) {
                typesBuilder.add(BIGINT);
                blocks.add(BlockAssertions.createLongSequenceBlock(channel, channel + 3));
            }
            else {
                typesBuilder.add(VARCHAR);
                blocks.add(BlockAssertions.createStringSequenceBlock(channel, channel + 3));
            }
            channels.add(blocks);
        }
        List<Type> types = typesBuilder.build();
        // output the channels in reverse order, so each output position maps to a different channel
        List<Integer> outputChannels = IntStream.range(0, channelCount)
                .map(channel -> channelCount - 1 - channel)
                .boxed()
                .collect(toImmutableList());
        List<Type> outputTypes = outputChannels.stream()
                .map(types::get)
                .collect(toImmutableList());

        PagesHashStrategy hashStrategy = joinCompiler.compilePagesHashStrategyFactory(types, Ints.asList(0), Optional.of(outputChannels))
                .createPagesHashStrategy(channels.build());

        assertThat(hashStrategy.getChannelCount()).isEqualTo(channelCount);

        // appendTo the page builder at a non-zero output channel offset, after a leading BIGINT column
        PageBuilder pageBuilder = new PageBuilder(ImmutableList.<Type>builder().add(BIGINT).addAll(outputTypes).build());
        int position = 2;
        pageBuilder.declarePosition();
        BIGINT.writeLong(pageBuilder.getBlockBuilder(0), -1);
        hashStrategy.appendTo(0, position, pageBuilder, 1);

        Page page = pageBuilder.build();
        assertThat(BIGINT.getLong(page.getBlock(0), 0)).isEqualTo(-1);
        for (int output = 0; output < channelCount; output++) {
            int channel = channelCount - 1 - output;
            Block pageBlock = page.getBlock(output + 1);
            if (channel % 2 == 0) {
                assertThat(BIGINT.getLong(pageBlock, 0)).isEqualTo(channel + position);
            }
            else {
                assertThat(VARCHAR.getSlice(pageBlock, 0).toStringUtf8()).isEqualTo(String.valueOf(channel + position));
            }
        }
    }

    @Test
    public void testIdentical()
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

        PagesHashStrategy hashStrategy = pagesHashStrategyFactory.createPagesHashStrategy(ImmutableList.of(channel));

        // verify channel count
        assertThat(hashStrategy.getChannelCount()).isEqualTo(1);

        BlockTypeOperators blockTypeOperators = new BlockTypeOperators();
        BlockPositionIsIdentical identicalOperator = blockTypeOperators.getIdenticalOperator(DOUBLE);

        // verify hashStrategy is consistent with DISTINCT from block
        for (int leftBlockIndex = 0; leftBlockIndex < channel.size(); leftBlockIndex++) {
            Block leftBlock = channel.get(leftBlockIndex);

            for (int leftBlockPosition = 0; leftBlockPosition < leftBlock.getPositionCount(); leftBlockPosition++) {
                // position must not be distinct from itself
                assertThat(hashStrategy.positionIdenticalToPosition(leftBlockIndex, leftBlockPosition, leftBlockIndex, leftBlockPosition)).isTrue();

                // check distinctiveness of every position against every other position in the block
                for (int rightBlockIndex = 0; rightBlockIndex < channel.size(); rightBlockIndex++) {
                    Block rightBlock = channel.get(rightBlockIndex);
                    for (int rightBlockPosition = 0; rightBlockPosition < rightBlock.getPositionCount(); rightBlockPosition++) {
                        boolean expectedIdentical = identicalOperator.isIdentical(leftBlock, leftBlockPosition, rightBlock, rightBlockPosition);
                        assertThat(hashStrategy.positionIdenticalToRow(leftBlockIndex, leftBlockPosition, rightBlockPosition, new Page(rightBlock))).isEqualTo(expectedIdentical);
                        assertThat(hashStrategy.rowIdenticalToRow(leftBlockPosition, new Page(leftBlock), rightBlockPosition, new Page(rightBlock))).isEqualTo(expectedIdentical);
                        assertThat(hashStrategy.positionIdenticalToPosition(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition)).isEqualTo(expectedIdentical);
                    }
                }

                // check equality of every position against every other position in the block cursor
                for (int rightBlockIndex = 0; rightBlockIndex < channel.size(); rightBlockIndex++) {
                    Block rightBlock = channel.get(rightBlockIndex);
                    for (int rightBlockPosition = 0; rightBlockPosition < rightBlock.getPositionCount(); rightBlockPosition++) {
                        boolean expectedIdentical = identicalOperator.isIdentical(leftBlock, leftBlockPosition, rightBlock, rightBlockPosition);
                        assertThat(hashStrategy.positionIdenticalToRow(leftBlockIndex, leftBlockPosition, rightBlockPosition, new Page(rightBlock))).isEqualTo(expectedIdentical);
                        assertThat(hashStrategy.rowIdenticalToRow(leftBlockPosition, new Page(leftBlock), rightBlockPosition, new Page(rightBlock))).isEqualTo(expectedIdentical);
                        assertThat(hashStrategy.positionIdenticalToPosition(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition)).isEqualTo(expectedIdentical);
                    }
                }
            }
        }
    }
}
