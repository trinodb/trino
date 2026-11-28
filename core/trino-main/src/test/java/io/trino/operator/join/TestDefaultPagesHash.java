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
package io.trino.operator.join;

import com.google.common.collect.ImmutableList;
import io.trino.RowPagesBuilder;
import io.trino.operator.HashArraySizeSupplier;
import io.trino.operator.InterpretedHashGenerator;
import io.trino.operator.NullSafeHashCompiler;
import io.trino.operator.PagesHashStrategy;
import io.trino.operator.SimplePagesHashStrategy;
import io.trino.operator.SyntheticAddress;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.function.OperatorMethodHandle;
import io.trino.spi.type.AbstractLongType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeDescriptor;
import io.trino.spi.type.TypeOperatorDeclaration;
import io.trino.spi.type.TypeOperators;
import io.trino.type.BlockTypeOperators;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static io.airlift.slice.SizeOf.sizeOfByteArray;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.operator.HashArraySizeSupplier.defaultHashArraySizeSupplier;
import static io.trino.operator.InterpretedHashGenerator.createPagePrefixHashGenerator;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDefaultPagesHash
{
    @Test
    public void testEmptyHash()
    {
        List<Type> types = ImmutableList.of(VARCHAR, BIGINT);
        List<Integer> joinChannels = ImmutableList.of(0, 1);
        List<ObjectArrayList<Block>> channels = ImmutableList.of(new ObjectArrayList<>(), new ObjectArrayList<>());
        IntArrayList positionCounts = new IntArrayList();
        LongArrayList addresses = new LongArrayList();
        TypeOperators typeOperators = new TypeOperators();
        PagesHashStrategy strategy = new SimplePagesHashStrategy(types, joinChannels, channels, joinChannels, Optional.empty(), new BlockTypeOperators(typeOperators));
        InterpretedHashGenerator hashGenerator = createPagePrefixHashGenerator(types, new NullSafeHashCompiler(typeOperators));
        Page probePage = rowPagesBuilder(types).row("a", 1L).build().get(0);
        long[] probeHashes = hashPage(hashGenerator, probePage);

        for (boolean usePackedTags : new boolean[] {true, false}) {
            DefaultPagesHash hash = new DefaultPagesHash(
                    addresses,
                    strategy,
                    channels,
                    positionCounts,
                    joinChannels,
                    hashGenerator,
                    ArrayPositionLinks.builder(0),
                    defaultHashArraySizeSupplier(),
                    usePackedTags);
            assertThat(hash.getPositionCount()).isEqualTo(0);
            assertThat(hash.getAddressIndex(0, probePage)).isEqualTo(-1);
            assertThat(hash.getAddressIndex(0, probePage, probeHashes[0])).isEqualTo(-1);
            assertThat(hash.getAddressIndex(new int[] {0}, probePage, probeHashes)).containsExactly(-1);
            assertThat(hash.getAddressIndex(new int[0], probePage, probeHashes)).isEmpty();
        }
    }

    @Test
    public void testPositionZeroNullsDuplicatesAndBatchProbe()
    {
        List<Type> types = ImmutableList.of(VARCHAR, BIGINT);
        // Page 0 has nulls (exercises indexPositions) and valid position 0.
        // Page 1 has no nulls (exercises indexRange) and duplicates across pages.
        List<Page> buildPages = rowPagesBuilder(types)
                .row("zero", 0L)
                .row(null, 1L)
                .row("two", null)
                .row("dup", 42L)
                .pageBreak()
                .row("dup", 42L)
                .row("unique", 99L)
                .row("dup", 42L)
                .build();

        BuildData buildData = toBuildData(types, buildPages, ImmutableList.of(0, 1));
        Page probePage = rowPagesBuilder(types)
                .row("zero", 0L)
                .row("dup", 42L)
                .row("missing", 123L)
                .row("unique", 99L)
                .build()
                .get(0);
        long[] probeHashes = hashPage(buildData.hashGenerator(), probePage);

        for (boolean usePackedTags : new boolean[] {true, false}) {
            PositionLinks.FactoryBuilder linksBuilder = ArrayPositionLinks.builder(buildData.addresses().size());
            DefaultPagesHash hash = new DefaultPagesHash(
                    buildData.addresses(),
                    buildData.strategy(),
                    buildData.channels(),
                    buildData.positionCounts(),
                    buildData.joinChannels(),
                    buildData.hashGenerator(),
                    linksBuilder,
                    defaultHashArraySizeSupplier(),
                    usePackedTags);

            assertThat(hash.getPositionCount()).isEqualTo(7);

            // Position 0 must be found even though realPosition == 0
            assertThat(hash.getAddressIndex(0, probePage)).isEqualTo(0);
            assertThat(hash.getAddressIndex(0, probePage, probeHashes[0])).isEqualTo(0);
            int dupStart = hash.getAddressIndex(1, probePage);
            assertThat(dupStart).isIn(3, 4, 6);
            assertThat(hash.getAddressIndex(2, probePage)).isEqualTo(-1);
            assertThat(hash.getAddressIndex(3, probePage)).isEqualTo(5);

            // Batched probe matches single-row probe
            int[] positions = {0, 1, 2, 3};
            int[] batchResult = hash.getAddressIndex(positions, probePage, probeHashes);
            assertThat(batchResult).containsExactly(0, dupStart, -1, 5);

            // Verify duplicate linking chain contains all 3 duplicate positions
            PositionLinks links = linksBuilder.build().create(ImmutableList.of());
            int count = 0;
            int pos = dupStart;
            while (pos != -1) {
                count++;
                pos = links.next(pos, 1, probePage);
            }
            assertThat(count).isEqualTo(3);

            // Verify appendTo for position 0 and unique row
            PageBuilder pageBuilder = new PageBuilder(types);
            pageBuilder.declarePosition();
            hash.appendTo(0, pageBuilder, 0);
            pageBuilder.declarePosition();
            hash.appendTo(5, pageBuilder, 0);
            Page output = pageBuilder.build();
            assertThat(VARCHAR.getSlice(output.getBlock(0), 0).toStringUtf8()).isEqualTo("zero");
            assertThat(BIGINT.getLong(output.getBlock(1), 0)).isEqualTo(0L);
            assertThat(VARCHAR.getSlice(output.getBlock(0), 1).toStringUtf8()).isEqualTo("unique");
            assertThat(BIGINT.getLong(output.getBlock(1), 1)).isEqualTo(99L);
        }
    }

    @Test
    public void testTagAndSlotCollisionsMatchBetweenPackedAndUnpacked()
    {
        // Force heavy collisions with a constant raw hash (same slot and same 7-bit tag for every key),
        // including low bytes 0x00, 0x7F, 0x80, and 0xFF (-1L) to cover sign-bit and -1 sentinel edge cases,
        // across both non-null pages (indexRange) and nullable pages (indexPositions).
        for (boolean includeNulls : new boolean[] {false, true}) {
            for (long forcedHash : new long[] {0L, 0x7FL, 0x80L, -1L}) {
                ConstantHashLongType keyType = new ConstantHashLongType(forcedHash);
                List<Type> types = ImmutableList.of(keyType);
                RowPagesBuilder buildBuilder = rowPagesBuilder(types);
                for (int i = 0; i < 600; i++) {
                    if (includeNulls && i > 0 && i % 7 == 0) {
                        buildBuilder.row((Object) null);
                    }
                    else {
                        buildBuilder.row((long) i);
                    }
                    if (i % 64 == 63) {
                        buildBuilder.pageBreak();
                    }
                }
                BuildData buildData = toBuildData(types, buildBuilder.build(), ImmutableList.of(0));

                DefaultPagesHash packed = new DefaultPagesHash(
                        buildData.addresses(),
                        buildData.strategy(),
                        buildData.channels(),
                        buildData.positionCounts(),
                        buildData.joinChannels(),
                        buildData.hashGenerator(),
                        ArrayPositionLinks.builder(600),
                        defaultHashArraySizeSupplier(),
                        true);
                DefaultPagesHash unpacked = new DefaultPagesHash(
                        buildData.addresses(),
                        buildData.strategy(),
                        buildData.channels(),
                        buildData.positionCounts(),
                        buildData.joinChannels(),
                        buildData.hashGenerator(),
                        ArrayPositionLinks.builder(600),
                        defaultHashArraySizeSupplier(),
                        false);

                RowPagesBuilder probeBuilder = rowPagesBuilder(types);
                for (int i = 0; i < 300; i++) {
                    probeBuilder.row(i * 2L);
                    probeBuilder.row(1000L + i);
                }
                Page probePage = probeBuilder.build().get(0);
                int[] positions = IntStream.range(0, probePage.getPositionCount()).toArray();
                long[] rawHashes = new long[probePage.getPositionCount()];
                Arrays.fill(rawHashes, forcedHash);

                int[] packedResult = packed.getAddressIndex(positions, probePage, rawHashes);
                int[] unpackedResult = unpacked.getAddressIndex(positions, probePage, rawHashes);
                assertThat(packedResult).isEqualTo(unpackedResult);

                for (int i = 0; i < 300; i++) {
                    int row = i * 2;
                    int expected = (includeNulls && row > 0 && row % 7 == 0) ? -1 : row;
                    assertThat(packedResult[i * 2]).isEqualTo(expected);
                    assertThat(packedResult[i * 2 + 1]).isEqualTo(-1);
                    assertThat(packed.getAddressIndex(i * 2, probePage, forcedHash)).isEqualTo(expected);
                    assertThat(packed.getAddressIndex(i * 2 + 1, probePage, forcedHash)).isEqualTo(-1);
                    assertThat(packed.getAddressIndex(i * 2, probePage)).isEqualTo(expected);
                }
            }
        }

        // Verify memory accounting reflects omitted positionToHashes in packed mode
        List<Type> types = ImmutableList.of(VARCHAR, BIGINT);
        RowPagesBuilder buildBuilder = rowPagesBuilder(types);
        for (int i = 0; i < 600; i++) {
            buildBuilder.row("k_" + i, (long) i);
            if (i % 64 == 63) {
                buildBuilder.pageBreak();
            }
        }
        BuildData buildData = toBuildData(types, buildBuilder.build(), ImmutableList.of(0, 1));
        HashArraySizeSupplier sizeSupplier = defaultHashArraySizeSupplier();
        DefaultPagesHash packed = new DefaultPagesHash(
                buildData.addresses(),
                buildData.strategy(),
                buildData.channels(),
                buildData.positionCounts(),
                buildData.joinChannels(),
                buildData.hashGenerator(),
                ArrayPositionLinks.builder(600),
                sizeSupplier,
                true);
        DefaultPagesHash unpacked = new DefaultPagesHash(
                buildData.addresses(),
                buildData.strategy(),
                buildData.channels(),
                buildData.positionCounts(),
                buildData.joinChannels(),
                buildData.hashGenerator(),
                ArrayPositionLinks.builder(600),
                sizeSupplier,
                false);
        assertThat(unpacked.getInMemorySizeInBytes() - packed.getInMemorySizeInBytes())
                .isEqualTo(sizeOfByteArray(600));
        assertThat(DefaultPagesHash.getEstimatedRetainedSizeInBytes(DefaultPagesHash.MAX_PACKED_POSITIONS, sizeSupplier, buildData.addresses(), buildData.channels(), 0))
                .isLessThan(DefaultPagesHash.getEstimatedRetainedSizeInBytes(DefaultPagesHash.MAX_PACKED_POSITIONS + 1, sizeSupplier, buildData.addresses(), buildData.channels(), 0));
    }

    private static long[] hashPage(InterpretedHashGenerator hashGenerator, Page page)
    {
        long[] hashes = new long[page.getPositionCount()];
        hashGenerator.hash(page, 0, page.getPositionCount(), hashes);
        return hashes;
    }

    private static BuildData toBuildData(List<Type> types, List<Page> pages, List<Integer> joinChannels)
    {
        List<ObjectArrayList<Block>> channels = IntStream.range(0, types.size())
                .mapToObj(_ -> new ObjectArrayList<Block>())
                .toList();
        IntArrayList positionCounts = new IntArrayList(pages.size());
        LongArrayList addresses = new LongArrayList();
        for (int pageIndex = 0; pageIndex < pages.size(); pageIndex++) {
            Page page = pages.get(pageIndex);
            positionCounts.add(page.getPositionCount());
            for (int ch = 0; ch < types.size(); ch++) {
                channels.get(ch).add(page.getBlock(ch));
            }
            for (int pos = 0; pos < page.getPositionCount(); pos++) {
                addresses.add(SyntheticAddress.encodeSyntheticAddress(pageIndex, pos));
            }
        }
        List<Integer> outputChannels = IntStream.range(0, types.size()).boxed().toList();
        TypeOperators typeOperators = new TypeOperators();
        PagesHashStrategy strategy = new SimplePagesHashStrategy(types, outputChannels, channels, joinChannels, Optional.empty(), new BlockTypeOperators(typeOperators));
        List<Type> joinChannelTypes = joinChannels.stream().map(types::get).toList();
        InterpretedHashGenerator hashGenerator = createPagePrefixHashGenerator(joinChannelTypes, new NullSafeHashCompiler(typeOperators));
        return new BuildData(addresses, channels, positionCounts, joinChannels, strategy, hashGenerator);
    }

    private record BuildData(
            LongArrayList addresses,
            List<ObjectArrayList<Block>> channels,
            IntArrayList positionCounts,
            List<Integer> joinChannels,
            PagesHashStrategy strategy,
            InterpretedHashGenerator hashGenerator) {}

    private static final class ConstantHashLongType
            extends AbstractLongType
    {
        private final long constantHash;
        private final TypeOperatorDeclaration operatorDeclaration;

        private ConstantHashLongType(long constantHash)
        {
            super(new TypeDescriptor("constant_hash_bigint_" + Long.toUnsignedString(constantHash)));
            this.constantHash = constantHash;
            TypeOperatorDeclaration base = BIGINT.getTypeOperatorDeclaration(new TypeOperators());
            OperatorMethodHandle constantHashOperator = new OperatorMethodHandle(
                    simpleConvention(FAIL_ON_NULL, NEVER_NULL),
                    MethodHandles.dropArguments(MethodHandles.constant(long.class, constantHash), 0, long.class));
            this.operatorDeclaration = TypeOperatorDeclaration.builder(long.class)
                    .addReadValueOperators(base.getReadValueOperators())
                    .addEqualOperators(base.getEqualOperators())
                    .addHashCodeOperator(constantHashOperator)
                    .addXxHash64Operator(constantHashOperator)
                    .build();
        }

        @Override
        public TypeOperatorDeclaration getTypeOperatorDeclaration(TypeOperators typeOperators)
        {
            return operatorDeclaration;
        }

        @Override
        public Object getObjectValue(Block block, int position)
        {
            if (block.isNull(position)) {
                return null;
            }
            return getLong(block, position);
        }

        @Override
        public boolean equals(Object other)
        {
            return other instanceof ConstantHashLongType that && constantHash == that.constantHash;
        }

        @Override
        public int hashCode()
        {
            return Long.hashCode(constantHash);
        }
    }
}
