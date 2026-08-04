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

import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.BigintType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.stream.IntStream;

import static io.trino.operator.BigintGroupByHash.invMurmurHash3;
import static io.trino.operator.UpdateMemory.NOOP;
import static io.trino.spi.type.BigintType.BIGINT;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestBigintGroupByHash
{
    private static final long[] EDGE_VALUES = {0, 1, -1, Long.MIN_VALUE, Long.MAX_VALUE, 42, -42};

    @Test
    public void testInvMurmurHash3RoundTrip()
    {
        SplittableRandom random = new SplittableRandom(42);
        for (int i = 0; i < 1_000_000; i++) {
            long value = random.nextLong();
            assertThat(invMurmurHash3(murmurHash3(value))).isEqualTo(value);
        }
        for (long value : EDGE_VALUES) {
            assertThat(invMurmurHash3(murmurHash3(value))).isEqualTo(value);
        }
    }

    @Test
    public void testRandomValuesWithNulls()
    {
        SplittableRandom random = new SplittableRandom(13);
        List<Long> values = new ArrayList<>();
        for (long value : EDGE_VALUES) {
            values.add(value);
        }
        for (int i = 0; i < 10_000; i++) {
            switch (random.nextInt(4)) {
                case 0 -> values.add(null);
                case 1 -> values.add(random.nextLong());
                // ensure duplicates
                default -> values.add((long) random.nextInt(100));
            }
        }
        assertGroupByMatchesModel(values);
    }

    @Test
    public void testManyRehashesAndRecordWidthTransitions()
    {
        // grows from capacity 16 to 4M slots, crossing several record width transitions and
        // record segment boundaries
        for (boolean identityHashing : new boolean[] {true, false}) {
            SplittableRandom random = new SplittableRandom(7);
            BigintGroupByHash groupByHash = new BigintGroupByHash(10, identityHashing, NOOP);
            Map<Long, Integer> model = new HashMap<>();

            for (int batch = 0; batch < 30; batch++) {
                List<Long> values = new ArrayList<>();
                for (int i = 0; i < 100_000; i++) {
                    values.add(random.nextLong());
                }
                assertThat(addToHash(groupByHash, values)).isEqualTo(addToModel(model, values));
            }

            assertThat(groupByHash.getGroupCount()).isEqualTo(model.size());
            assertValuesAndHashes(groupByHash, model);
        }
    }

    @Test
    public void testSwitchesToMurmurOnStructuredKeys()
    {
        // multiples of 4096 share their low 12 bits, which degenerates identity bucketing in a
        // way that growing the table cannot fix
        BigintGroupByHash groupByHash = new BigintGroupByHash(10, NOOP);
        Map<Long, Integer> model = new HashMap<>();
        assertThat(groupByHash.isIdentityHashing()).isTrue();

        for (int batch = 0; batch < 5; batch++) {
            List<Long> values = new ArrayList<>();
            for (int i = 0; i < 100_000; i++) {
                values.add(((long) (batch * 100_000 + i)) << 12);
            }
            assertThat(addToHash(groupByHash, values)).isEqualTo(addToModel(model, values));
        }

        assertThat(groupByHash.isIdentityHashing()).isFalse();
        assertValuesAndHashes(groupByHash, model);
    }

    @Test
    public void testStaysOnIdentityHashingForDenseKeys()
    {
        // sequential keys are the identity-hashing best case
        BigintGroupByHash sequential = new BigintGroupByHash(10, NOOP);
        List<Long> values = new ArrayList<>();
        for (int i = 0; i < 500_000; i++) {
            values.add((long) i);
        }
        addToHash(sequential, values);
        assertThat(sequential.isIdentityHashing()).isTrue();

        // random dense keys wrap around intermediate capacities, which clusters transiently; the
        // table must recover by growing rather than permanently abandoning identity hashing
        SplittableRandom random = new SplittableRandom(23);
        BigintGroupByHash dense = new BigintGroupByHash(10, NOOP);
        values = new ArrayList<>();
        for (int i = 0; i < 1_000_000; i++) {
            values.add((long) random.nextInt(700_000));
        }
        addToHash(dense, values);
        assertThat(dense.isIdentityHashing()).isTrue();
    }

    @Test
    public void testStartReleasingOutput()
    {
        SplittableRandom random = new SplittableRandom(17);
        BigintGroupByHash groupByHash = new BigintGroupByHash(10, NOOP);
        Map<Long, Integer> model = new HashMap<>();

        List<Long> values = new ArrayList<>();
        values.add(null);
        for (int i = 0; i < 100_000; i++) {
            values.add(random.nextLong());
        }
        assertThat(addToHash(groupByHash, values)).isEqualTo(addToModel(model, values));

        long sizeBeforeRelease = groupByHash.getEstimatedSize();
        groupByHash.startReleasingOutput();
        assertThat(groupByHash.getEstimatedSize()).isLessThan(sizeBeforeRelease);
        assertValuesAndHashes(groupByHash, model);

        Page page = buildPage(List.of(1L));
        assertThatThrownBy(() -> groupByHash.addPage(page)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> groupByHash.getGroupIds(page)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testStartReleasingOutputMemoryAccounting()
    {
        SplittableRandom random = new SplittableRandom(29);
        RecordingUpdateMemory updateMemory = new RecordingUpdateMemory();
        BigintGroupByHash groupByHash = new BigintGroupByHash(10, updateMemory);
        updateMemory.groupByHash = groupByHash;

        List<Long> values = new ArrayList<>();
        for (int i = 0; i < 100_000; i++) {
            values.add(random.nextLong());
        }
        addToHash(groupByHash, values);

        long sizeBeforeRelease = groupByHash.getEstimatedSize();
        int callsBeforeRelease = updateMemory.sizes.size();
        groupByHash.startReleasingOutput();

        List<Long> releaseSizes = updateMemory.sizes.subList(callsBeforeRelease, updateMemory.sizes.size());
        assertThat(releaseSizes).hasSize(2);
        // the reservation for the materialized values is visible before the table is released
        assertThat(releaseSizes.getFirst()).isGreaterThan(groupByHash.getEstimatedSize());
        // the final report reflects the released table
        assertThat(releaseSizes.getLast()).isEqualTo(groupByHash.getEstimatedSize());
        assertThat(groupByHash.getEstimatedSize()).isLessThan(sizeBeforeRelease);
    }

    @Test
    public void testDictionaryBlocks()
    {
        SplittableRandom random = new SplittableRandom(3);
        BlockBuilder dictionaryBuilder = BIGINT.createFixedSizeBlockBuilder(1000);
        dictionaryBuilder.appendNull();
        List<Long> dictionaryValues = new ArrayList<>();
        dictionaryValues.add(null);
        for (int i = 0; i < 999; i++) {
            long value = random.nextLong();
            dictionaryValues.add(value);
            BIGINT.writeLong(dictionaryBuilder, value);
        }
        Block dictionary = dictionaryBuilder.build();
        int[] ids = IntStream.range(0, 5000).map(_ -> random.nextInt(1000)).toArray();

        BigintGroupByHash groupByHash = new BigintGroupByHash(10, NOOP);
        Map<Long, Integer> model = new HashMap<>();
        List<Long> values = new ArrayList<>();
        for (int id : ids) {
            values.add(dictionaryValues.get(id));
        }
        int[] groupIds = getGroupIds(groupByHash, new Page(DictionaryBlock.create(ids.length, dictionary, ids)));
        assertThat(groupIds).isEqualTo(addToModel(model, values));
        assertValuesAndHashes(groupByHash, model);
    }

    @Test
    public void testRunLengthEncodedBlocks()
    {
        BigintGroupByHash groupByHash = new BigintGroupByHash(10, NOOP);
        BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(1);
        BIGINT.writeLong(blockBuilder, 12345);
        int[] groupIds = getGroupIds(groupByHash, new Page(RunLengthEncodedBlock.create(blockBuilder.build(), 100)));
        assertThat(groupIds).containsOnly(0);

        BlockBuilder nullBuilder = BIGINT.createFixedSizeBlockBuilder(1);
        nullBuilder.appendNull();
        groupIds = getGroupIds(groupByHash, new Page(RunLengthEncodedBlock.create(nullBuilder.build(), 100)));
        assertThat(groupIds).containsOnly(1);
        assertThat(groupByHash.getGroupCount()).isEqualTo(2);
    }

    @Test
    public void testCopy()
    {
        SplittableRandom random = new SplittableRandom(11);
        BigintGroupByHash groupByHash = new BigintGroupByHash(10, NOOP);
        List<Long> values = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            values.add(random.nextLong());
        }
        int[] originalGroupIds = addToHash(groupByHash, values);

        GroupByHash copy = groupByHash.copy();
        assertThat(copy.getGroupCount()).isEqualTo(groupByHash.getGroupCount());
        assertThat(getGroupIds(copy, buildPage(values))).isEqualTo(originalGroupIds);

        // copy after release still appends values
        groupByHash.startReleasingOutput();
        GroupByHash releasedCopy = groupByHash.copy();
        assertThat(releasedCopy.getGroupCount()).isEqualTo(groupByHash.getGroupCount());
    }

    private static void assertGroupByMatchesModel(List<Long> values)
    {
        for (boolean identityHashing : new boolean[] {true, false}) {
            BigintGroupByHash groupByHash = new BigintGroupByHash(10, identityHashing, NOOP);
            Map<Long, Integer> model = new HashMap<>();
            assertThat(addToHash(groupByHash, values)).isEqualTo(addToModel(model, values));
            assertThat(groupByHash.getGroupCount()).isEqualTo(model.size());
            assertValuesAndHashes(groupByHash, model);
        }
    }

    private static void assertValuesAndHashes(BigintGroupByHash groupByHash, Map<Long, Integer> model)
    {
        int groupCount = groupByHash.getGroupCount();
        PageBuilder pageBuilder = new PageBuilder(groupCount, List.of(BIGINT));
        for (int groupId = 0; groupId < groupCount; groupId++) {
            pageBuilder.declarePosition();
            groupByHash.appendValuesTo(groupId, pageBuilder);
        }
        Block block = pageBuilder.build().getBlock(0);

        for (Map.Entry<Long, Integer> entry : model.entrySet()) {
            int groupId = entry.getValue();
            if (entry.getKey() == null) {
                assertThat(block.isNull(groupId)).isTrue();
            }
            else {
                long value = entry.getKey();
                assertThat(block.isNull(groupId)).isFalse();
                assertThat(BIGINT.getLong(block, groupId)).isEqualTo(value);
                assertThat(groupByHash.getRawHash(groupId)).isEqualTo(BigintType.hash(value));
            }
        }
    }

    private static int[] addToHash(GroupByHash groupByHash, List<Long> values)
    {
        return getGroupIds(groupByHash, buildPage(values));
    }

    private static int[] addToModel(Map<Long, Integer> model, List<Long> values)
    {
        int[] groupIds = new int[values.size()];
        for (int i = 0; i < values.size(); i++) {
            groupIds[i] = model.computeIfAbsent(values.get(i), _ -> model.size());
        }
        return groupIds;
    }

    private static Page buildPage(List<Long> values)
    {
        BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(values.size());
        for (Long value : values) {
            if (value == null) {
                blockBuilder.appendNull();
            }
            else {
                BIGINT.writeLong(blockBuilder, value);
            }
        }
        return new Page(blockBuilder.build());
    }

    private static final class RecordingUpdateMemory
            implements UpdateMemory
    {
        private final List<Long> sizes = new ArrayList<>();
        private GroupByHash groupByHash;

        @Override
        public boolean update()
        {
            if (groupByHash != null) {
                sizes.add(groupByHash.getEstimatedSize());
            }
            return true;
        }
    }

    private static int[] getGroupIds(GroupByHash groupByHash, Page page)
    {
        Work<int[]> work = groupByHash.getGroupIds(page);
        boolean finished;
        do {
            finished = work.process();
        }
        while (!finished);
        return work.getResult();
    }
}
