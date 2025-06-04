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
package io.trino.operator.join.unspilled;

import com.google.common.collect.ImmutableList;
import io.trino.operator.join.LookupSource;
import io.trino.operator.join.unspilled.JoinProbe.JoinProbeFactory;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.OptionalInt;

import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLookupJoinPageBuilder
{
    @Test
    public void testPageBuilder()
    {
        int entries = 10_000;
        BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(entries);
        for (int i = 0; i < entries; i++) {
            BIGINT.writeLong(blockBuilder, i);
        }
        Block block = blockBuilder.build();
        Page page = new Page(block, block);

        JoinProbeFactory joinProbeFactory = new JoinProbeFactory(ImmutableList.of(0, 1), ImmutableList.of(0, 1), OptionalInt.empty(), false);
        LookupSource lookupSource = new TestLookupSource(ImmutableList.of(BIGINT, BIGINT), page);
        JoinProbe probe = joinProbeFactory.createJoinProbe(page, lookupSource);
        LookupJoinPageBuilder lookupJoinPageBuilder = new LookupJoinPageBuilder(ImmutableList.of(BIGINT, BIGINT));

        int joinPosition = 0;
        while (!lookupJoinPageBuilder.isFull() && probe.advanceNextPosition()) {
            lookupJoinPageBuilder.appendRow(probe, lookupSource, joinPosition++);
            lookupJoinPageBuilder.appendNullForBuild(probe);
        }
        assertThat(lookupJoinPageBuilder.isEmpty()).isFalse();

        Page output = lookupJoinPageBuilder.build(probe);
        assertThat(output.getChannelCount()).isEqualTo(4);
        assertThat(output.getBlock(0)).isInstanceOf(DictionaryBlock.class);
        assertThat(output.getBlock(1)).isInstanceOf(DictionaryBlock.class);
        for (int i = 0; i < output.getPositionCount(); i++) {
            assertThat(output.getBlock(0).isNull(i)).isFalse();
            assertThat(output.getBlock(1).isNull(i)).isFalse();
            assertThat(BIGINT.getLong(output.getBlock(0), i)).isEqualTo(i / 2);
            assertThat(BIGINT.getLong(output.getBlock(1), i)).isEqualTo(i / 2);
            if (i % 2 == 0) {
                assertThat(output.getBlock(2).isNull(i)).isFalse();
                assertThat(output.getBlock(3).isNull(i)).isFalse();
                assertThat(BIGINT.getLong(output.getBlock(2), i)).isEqualTo(i / 2);
                assertThat(BIGINT.getLong(output.getBlock(3), i)).isEqualTo(i / 2);
            }
            else {
                assertThat(output.getBlock(2).isNull(i)).isTrue();
                assertThat(output.getBlock(3).isNull(i)).isTrue();
            }
        }
        assertThat(lookupJoinPageBuilder.toString()).contains("positionCount=" + output.getPositionCount());

        lookupJoinPageBuilder.reset();
        assertThat(lookupJoinPageBuilder.isEmpty()).isTrue();
    }

    @Test
    public void testDifferentPositions()
    {
        int entries = 100;
        BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(entries);
        for (int i = 0; i < entries; i++) {
            BIGINT.writeLong(blockBuilder, i);
        }
        Block block = blockBuilder.build();
        Page page = new Page(block);
        JoinProbeFactory joinProbeFactory = new JoinProbeFactory(ImmutableList.of(0), ImmutableList.of(0), OptionalInt.empty(), false);
        LookupSource lookupSource = new TestLookupSource(ImmutableList.of(BIGINT), page);
        LookupJoinPageBuilder lookupJoinPageBuilder = new LookupJoinPageBuilder(ImmutableList.of(BIGINT));

        // empty
        JoinProbe probe = joinProbeFactory.createJoinProbe(page, lookupSource);
        Page output = lookupJoinPageBuilder.build(probe);
        assertThat(output.getChannelCount()).isEqualTo(2);
        assertThat(output.getBlock(0)).isInstanceOf(LongArrayBlock.class);
        assertThat(output.getPositionCount()).isEqualTo(0);
        lookupJoinPageBuilder.reset();

        // the probe covers non-sequential positions
        probe = joinProbeFactory.createJoinProbe(page, lookupSource);
        for (int joinPosition = 0; probe.advanceNextPosition(); joinPosition++) {
            if (joinPosition % 2 == 1) {
                continue;
            }
            lookupJoinPageBuilder.appendRow(probe, lookupSource, joinPosition);
        }
        output = lookupJoinPageBuilder.build(probe);
        assertThat(output.getChannelCount()).isEqualTo(2);
        assertThat(output.getBlock(0)).isInstanceOf(DictionaryBlock.class);
        assertThat(output.getPositionCount()).isEqualTo(entries / 2);
        for (int i = 0; i < entries / 2; i++) {
            assertThat(BIGINT.getLong(output.getBlock(0), i)).isEqualTo(i * 2L);
            assertThat(BIGINT.getLong(output.getBlock(1), i)).isEqualTo(i * 2L);
        }
        lookupJoinPageBuilder.reset();

        // the probe covers everything
        probe = joinProbeFactory.createJoinProbe(page, lookupSource);
        for (int joinPosition = 0; probe.advanceNextPosition(); joinPosition++) {
            lookupJoinPageBuilder.appendRow(probe, lookupSource, joinPosition);
        }
        output = lookupJoinPageBuilder.build(probe);
        assertThat(output.getChannelCount()).isEqualTo(2);
        assertThat(output.getBlock(0)).isNotInstanceOf(DictionaryBlock.class);
        assertThat(output.getPositionCount()).isEqualTo(entries);
        for (int i = 0; i < entries; i++) {
            assertThat(BIGINT.getLong(output.getBlock(0), i)).isEqualTo(i);
            assertThat(BIGINT.getLong(output.getBlock(1), i)).isEqualTo(i);
        }
        lookupJoinPageBuilder.reset();

        // the probe covers some sequential positions
        probe = joinProbeFactory.createJoinProbe(page, lookupSource);
        for (int joinPosition = 0; probe.advanceNextPosition(); joinPosition++) {
            if (joinPosition < 10 || joinPosition >= 50) {
                continue;
            }
            lookupJoinPageBuilder.appendRow(probe, lookupSource, joinPosition);
        }
        output = lookupJoinPageBuilder.build(probe);
        assertThat(output.getChannelCount()).isEqualTo(2);
        assertThat(output.getBlock(0)).isNotInstanceOf(DictionaryBlock.class);
        assertThat(output.getPositionCount()).isEqualTo(40);
        for (int i = 10; i < 50; i++) {
            assertThat(BIGINT.getLong(output.getBlock(0), i - 10)).isEqualTo(i);
            assertThat(BIGINT.getLong(output.getBlock(1), i - 10)).isEqualTo(i);
        }
    }

    @Test
    public void testCrossJoinWithEmptyBuild()
    {
        BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(1);
        BIGINT.writeLong(blockBuilder, 0);
        Page page = new Page(blockBuilder.build());

        // nothing on the build side so we don't append anything
        LookupSource lookupSource = new TestLookupSource(ImmutableList.of(), page);
        JoinProbe probe = new JoinProbeFactory(ImmutableList.of(0), ImmutableList.of(0), OptionalInt.empty(), false).createJoinProbe(page, lookupSource);
        LookupJoinPageBuilder lookupJoinPageBuilder = new LookupJoinPageBuilder(ImmutableList.of(BIGINT));

        // append the same row many times should also flush in the end
        probe.advanceNextPosition();
        for (int i = 0; i < 300_000; i++) {
            lookupJoinPageBuilder.appendRow(probe, lookupSource, 0);
        }
        assertThat(lookupJoinPageBuilder.isFull()).isTrue();
    }

    private static final class TestLookupSource
            implements LookupSource
    {
        private final List<Type> types;
        private final Page page;

        public TestLookupSource(List<Type> types, Page page)
        {
            this.types = types;
            this.page = page;
        }

        @Override
        public boolean isEmpty()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getJoinPositionCount()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long joinPositionWithinPartition(long joinPosition)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getInMemorySizeInBytes()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getJoinPosition(int position, Page page, Page allChannelsPage, long rawHash)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage)
        {
            return -1;
        }

        @Override
        public long getNextJoinPosition(long currentJoinPosition, int probePosition, Page allProbeChannelsPage)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isJoinPositionEligible(long currentJoinPosition, int probePosition, Page allProbeChannelsPage)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset)
        {
            for (int i = 0; i < types.size(); i++) {
                types.get(i).appendTo(page.getBlock(i), (int) position, pageBuilder.getBlockBuilder(i));
            }
        }

        @Override
        public void close() {}
    }
}
