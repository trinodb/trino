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
package io.trino.orc;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.orc.metadata.CompressedMetadataWriter;
import io.trino.orc.metadata.CompressionKind;
import io.trino.orc.metadata.Footer;
import io.trino.orc.metadata.Metadata;
import io.trino.orc.metadata.MetadataWriter;
import io.trino.orc.metadata.RowGroupIndex;
import io.trino.orc.metadata.StripeFooter;
import io.trino.orc.metadata.statistics.BloomFilter;
import io.trino.orc.metadata.statistics.NoOpBloomFilterBuilder;
import io.trino.orc.metadata.statistics.StringStatisticsBuilder;
import io.trino.orc.metadata.statistics.Utf8BloomFilterBuilder;
import io.trino.orc.writer.SliceDictionaryColumnWriter;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.VariableWidthBlockBuilder;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static io.airlift.slice.SliceUtf8.lengthOfCodePoint;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.orc.OrcWriterOptions.DEFAULT_MAX_COMPRESSION_BUFFER_SIZE;
import static io.trino.orc.OrcWriterOptions.DEFAULT_MAX_STRING_STATISTICS_LIMIT;
import static io.trino.orc.metadata.OrcColumnId.ROOT_COLUMN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Character.MAX_CODE_POINT;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestSliceDictionaryColumnWriter
{
    @Test
    public void testDirectConversion()
    {
        SliceDictionaryColumnWriter writer = new SliceDictionaryColumnWriter(
                ROOT_COLUMN,
                VARCHAR,
                CompressionKind.NONE,
                toIntExact(DEFAULT_MAX_COMPRESSION_BUFFER_SIZE.toBytes()),
                () -> new StringStatisticsBuilder(toIntExact(DEFAULT_MAX_STRING_STATISTICS_LIMIT.toBytes()), new NoOpBloomFilterBuilder()));

        int[] validCodepoints = IntStream.range(0, MAX_CODE_POINT)
                .filter(Character::isValidCodePoint)
                .toArray();
        Slice randomUtf8Slice = createRandomUtf8Slice(validCodepoints, megabytes(1));
        Block data = RunLengthEncodedBlock.create(VARCHAR, Slices.wrappedBuffer(randomUtf8Slice.byteArray()), 3000);
        writer.beginRowGroup();
        writer.writeBlock(data);
        writer.finishRowGroup();

        assertFalse(writer.tryConvertToDirect(megabytes(64)).isPresent());
    }

    @Test
    public void testBloomFiltersAfterConvertToDirect()
            throws IOException
    {
        int valuesCount = 1000;
        SliceDictionaryColumnWriter writer = new SliceDictionaryColumnWriter(
                ROOT_COLUMN,
                VARCHAR,
                CompressionKind.NONE,
                toIntExact(DEFAULT_MAX_COMPRESSION_BUFFER_SIZE.toBytes()),
                () -> new StringStatisticsBuilder(toIntExact(DEFAULT_MAX_STRING_STATISTICS_LIMIT.toBytes()), new Utf8BloomFilterBuilder(valuesCount, 0.01)));

        List<Slice> testValues = new ArrayList<>(valuesCount);
        int expectedEntries = (valuesCount / 9) + 1;
        VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(
                null,
                expectedEntries,
                expectedEntries * 500);
        byte base = 0;
        for (int i = 0; i < valuesCount; i++) {
            Slice value = sequentialBytes(base, i);
            testValues.add(value);
            base = (byte) (base + i);
            if (i % 9 == 0) {
                blockBuilder.writeBytes(value, 0, value.length());
                blockBuilder.closeEntry();
            }
        }

        writer.beginRowGroup();
        writer.writeBlock(blockBuilder.build());
        writer.finishRowGroup();

        assertThat(writer.tryConvertToDirect(100_000)).isPresent();
        TestingMetadataWriter metadataWriter = new TestingMetadataWriter();
        assertThat(writer.getBloomFilters(new CompressedMetadataWriter(metadataWriter, CompressionKind.NONE, 500)))
                .isNotEmpty();
        int hits = 0;
        for (int i = 0; i < valuesCount; i++) {
            Slice testValue = testValues.get(i);
            boolean contains = metadataWriter.getBloomFilters().stream().anyMatch(filter -> filter.testSlice(testValue));
            if (i % 9 == 0) {
                // No false negatives
                assertTrue(contains);
            }
            hits += contains ? 1 : 0;
        }
        assertThat((double) hits / valuesCount).isBetween(0.1, 0.115);
    }

    public static Slice createRandomUtf8Slice(int[] codePointSet, int lengthInBytes)
    {
        int[] codePoints = new int[lengthInBytes];
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int totalLength = 0;
        int offset = 0;
        while (totalLength + 4 <= lengthInBytes) {
            int codePoint = codePointSet[random.nextInt(codePointSet.length)];
            codePoints[offset] = codePoint;
            totalLength += lengthOfCodePoint(codePoint);
            offset++;
        }
        // Fill last 0-3 bytes with some 1-byte characters
        while (totalLength < lengthInBytes) {
            codePoints[offset] = random.nextInt('a', 'z');
            totalLength++;
            offset++;
        }
        return utf8Slice(new String(codePoints, 0, offset));
    }

    private static int megabytes(int size)
    {
        return toIntExact(DataSize.of(size, MEGABYTE).toBytes());
    }

    private static Slice sequentialBytes(byte base, int length)
    {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = (byte) (base + i);
        }
        return Slices.wrappedBuffer(bytes);
    }

    private static class TestingMetadataWriter
            implements MetadataWriter
    {
        private List<BloomFilter> bloomFilters;

        @Override
        public List<Integer> getOrcMetadataVersion()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int writePostscript(SliceOutput output, int footerLength, int metadataLength, CompressionKind compression, int compressionBlockSize)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int writeMetadata(SliceOutput output, Metadata metadata)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int writeFooter(SliceOutput output, Footer footer)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int writeStripeFooter(SliceOutput output, StripeFooter footer)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int writeRowIndexes(SliceOutput output, List<RowGroupIndex> rowGroupIndexes)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int writeBloomFilters(SliceOutput output, List<BloomFilter> bloomFilters)
        {
            this.bloomFilters = bloomFilters;
            return 0;
        }

        public List<BloomFilter> getBloomFilters()
        {
            requireNonNull(bloomFilters, "bloomFilters is null");
            return bloomFilters;
        }
    }
}
