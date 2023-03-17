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
package io.trino.execution.buffer;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.trino.metadata.BlockEncodingManager;
import io.trino.metadata.InternalBlockEncodingSerde;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.type.Type;
import io.trino.tpch.LineItem;
import io.trino.tpch.LineItemGenerator;
import org.assertj.core.api.Assertions;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.crypto.SecretKey;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.execution.buffer.PagesSerdeUtil.readPages;
import static io.trino.execution.buffer.PagesSerdeUtil.writePages;
import static io.trino.operator.PageAssertions.assertPageEquals;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static io.trino.util.Ciphers.createRandomAesEncryptionKey;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestPagesSerde
{
    private BlockEncodingSerde blockEncodingSerde;

    @BeforeClass
    public void setup()
    {
        blockEncodingSerde = new InternalBlockEncodingSerde(new BlockEncodingManager(), TESTING_TYPE_MANAGER);
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        blockEncodingSerde = null;
    }

    @Test
    public void testRoundTrip()
    {
        // empty pages
        testRoundTrip(ImmutableList.of(), 0);
        testRoundTrip(ImmutableList.of(BIGINT), 0);
        // tiny pages
        testRoundTrip(ImmutableList.of(BIGINT), 1);
        testRoundTrip(ImmutableList.of(VARCHAR), 1);
        testRoundTrip(ImmutableList.of(VARCHAR, DOUBLE), 1);
        testRoundTrip(ImmutableList.of(BIGINT), 30);
        testRoundTrip(ImmutableList.of(VARCHAR), 20);
        testRoundTrip(ImmutableList.of(VARCHAR, DOUBLE), 15);
        // small pages
        testRoundTrip(ImmutableList.of(BIGINT), 300);
        testRoundTrip(ImmutableList.of(VARCHAR), 200);
        testRoundTrip(ImmutableList.of(VARCHAR, DOUBLE), 150);
        testRoundTrip(ImmutableList.of(BIGINT, VARCHAR, DOUBLE), 300);
        testRoundTrip(ImmutableList.of(VARCHAR, VARCHAR, DOUBLE), 200);
        testRoundTrip(ImmutableList.of(VARCHAR, VARCHAR, VARCHAR, VARCHAR), 150);
        // medium pages
        testRoundTrip(ImmutableList.of(BIGINT, VARCHAR, DOUBLE), 3000);
        testRoundTrip(ImmutableList.of(VARCHAR, VARCHAR, DOUBLE), 2000);
        testRoundTrip(ImmutableList.of(VARCHAR, VARCHAR, VARCHAR, VARCHAR), 1500);
        testRoundTrip(ImmutableList.of(BIGINT, VARCHAR, DOUBLE), 12000);
        testRoundTrip(ImmutableList.of(VARCHAR, VARCHAR, DOUBLE), 9000);
        testRoundTrip(ImmutableList.of(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT), 8000);
        // jumbo pages
        testRoundTrip(ImmutableList.of(BIGINT, VARCHAR, DOUBLE), 30000);
        testRoundTrip(ImmutableList.of(VARCHAR, VARCHAR, DOUBLE), 20000);
        testRoundTrip(ImmutableList.of(VARCHAR, VARCHAR, VARCHAR, VARCHAR), 15000);
        testRoundTrip(ImmutableList.of(BIGINT, VARCHAR, DOUBLE), 120000);
        testRoundTrip(ImmutableList.of(VARCHAR, VARCHAR, DOUBLE), 90000);
        testRoundTrip(ImmutableList.of(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT), 80000);
    }

    private void testRoundTrip(List<Type> types, int rowCount)
    {
        LineItemGenerator lineItemGenerator = new LineItemGenerator(1, 1, 1);
        Iterator<LineItem> iterator = lineItemGenerator.iterator();
        int pageCount = 3;
        List<Page> pages = IntStream.range(0, pageCount)
                .mapToObj(i -> generatePage(types, rowCount, iterator))
                .collect(toImmutableList());
        testRoundTrip(types, pages);
    }

    private void testRoundTrip(List<Type> types, List<Page> pages)
    {
        // small blocks (to test corner cases)
        testRoundTrip(types, pages, 107);
        testRoundTrip(types, pages, 1009);
        // large blocks (to test real world scenario)
        testRoundTrip(types, pages, 64 * 1024);
        testRoundTrip(types, pages, 128 * 1024);
    }

    private void testRoundTrip(List<Type> types, List<Page> pages, int blockSizeInBytes)
    {
        // without compression, encryption
        testRoundTrip(types, pages, false, false, blockSizeInBytes);
        // with compression, without encryption
        testRoundTrip(types, pages, true, false, blockSizeInBytes);
        // without compression, with encryption
        testRoundTrip(types, pages, false, true, blockSizeInBytes);
        // with compression, encryption
        testRoundTrip(types, pages, true, true, blockSizeInBytes);
    }

    private void testRoundTrip(List<Type> types, List<Page> pages, boolean compressionEnabled, boolean encryptionEnabled, int blockSizeInBytes)
    {
        Optional<SecretKey> encryptionKey = encryptionEnabled ? Optional.of(createRandomAesEncryptionKey()) : Optional.empty();
        PageSerializer serializer = new PageSerializer(blockEncodingSerde, compressionEnabled, encryptionKey, blockSizeInBytes);
        PageDeserializer deserializer = new PageDeserializer(blockEncodingSerde, compressionEnabled, encryptionKey, blockSizeInBytes);
        for (Page page : pages) {
            Slice serialized = serializer.serialize(page);
            Page deserialized = deserializer.deserialize(serialized);
            assertPageEquals(types, deserialized, page);
        }
    }

    private static Page generatePage(List<Type> types, int rowCount, Iterator<LineItem> iterator)
    {
        PageBuilder pageBuilder = new PageBuilder(types);
        for (int row = 0; row < rowCount; row++) {
            pageBuilder.declarePosition();
            LineItem lineItem = iterator.next();
            for (int column = 0; column < types.size(); column++) {
                Type type = types.get(column);
                if (BIGINT.equals(type)) {
                    BIGINT.writeLong(pageBuilder.getBlockBuilder(column), lineItem.getOrderKey());
                }
                else if (VARCHAR.equals(type)) {
                    VARCHAR.writeString(pageBuilder.getBlockBuilder(column), lineItem.getComment());
                }
                else if (DOUBLE.equals(type)) {
                    DOUBLE.writeDouble(pageBuilder.getBlockBuilder(column), lineItem.getExtendedPrice());
                }
            }
        }

        return pageBuilder.build();
    }

    @Test
    public void testBigintSerializedSize()
    {
        BlockBuilder builder = BIGINT.createBlockBuilder(null, 5);

        // empty page
        Page page = new Page(builder.build());
        int pageSize = serializedSize(ImmutableList.of(BIGINT), page);
        assertEquals(pageSize, 40);

        // page with one value
        BIGINT.writeLong(builder, 123);
        pageSize = 35; // Now we have moved to the normal block implementation so the page size overhead is 35
        page = new Page(builder.build());
        int firstValueSize = serializedSize(ImmutableList.of(BIGINT), page) - pageSize;
        assertEquals(firstValueSize, 9); // value size + value overhead

        // page with two values
        BIGINT.writeLong(builder, 456);
        page = new Page(builder.build());
        int secondValueSize = serializedSize(ImmutableList.of(BIGINT), page) - (pageSize + firstValueSize);
        assertEquals(secondValueSize, 8); // value size (value overhead is shared with previous value)
    }

    @Test
    public void testVarcharSerializedSize()
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, 5);

        // empty page
        Page page = new Page(builder.build());
        int pageSize = serializedSize(ImmutableList.of(VARCHAR), page);
        assertEquals(pageSize, 48);

        // page with one value
        VARCHAR.writeString(builder, "alice");
        pageSize = 44; // Now we have moved to the normal block implementation so the page size overhead is 44
        page = new Page(builder.build());
        int firstValueSize = serializedSize(ImmutableList.of(VARCHAR), page) - pageSize;
        assertEquals(firstValueSize, 8 + 5); // length + nonNullsCount + "alice"

        // page with two values
        VARCHAR.writeString(builder, "bob");
        page = new Page(builder.build());
        int secondValueSize = serializedSize(ImmutableList.of(VARCHAR), page) - (pageSize + firstValueSize);
        assertEquals(secondValueSize, 4 + 3); // length + "bob" (null shared with first entry)
    }

    private int serializedSize(List<? extends Type> types, Page expectedPage)
    {
        PagesSerdeFactory serdeFactory = new PagesSerdeFactory(blockEncodingSerde, false);
        PageSerializer serializer = serdeFactory.createSerializer(Optional.empty());
        PageDeserializer deserializer = serdeFactory.createDeserializer(Optional.empty());
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        writePages(serializer, sliceOutput, expectedPage);
        Slice slice = sliceOutput.slice();

        Iterator<Page> pageIterator = readPages(deserializer, slice.getInput());
        if (pageIterator.hasNext()) {
            assertPageEquals(types, pageIterator.next(), expectedPage);
        }
        else {
            assertEquals(expectedPage.getPositionCount(), 0);
        }
        assertFalse(pageIterator.hasNext());

        return slice.length();
    }

    @Test
    public void testDeserializationWithRollover()
    {
        // test non-zero rollover when refilling buffer on deserialization
        for (int blockSize = 100; blockSize < 500; blockSize += 101) {
            for (int numberOfEntries = 500; numberOfEntries < 1000; numberOfEntries += 99) {
                testDeserializationWithRollover(blockSize, numberOfEntries);
            }
        }
    }

    private void testDeserializationWithRollover(int blockSize, int numberOfEntries)
    {
        testDeserializationWithRollover(false, false, numberOfEntries, blockSize);
        testDeserializationWithRollover(false, true, numberOfEntries, blockSize);
        testDeserializationWithRollover(true, false, numberOfEntries, blockSize);
        testDeserializationWithRollover(true, true, numberOfEntries, blockSize);
    }

    private void testDeserializationWithRollover(boolean encryptionEnabled, boolean compressionEnabled, int numberOfEntries, int blockSize)
    {
        RolloverBlockSerde blockSerde = new RolloverBlockSerde();
        Optional<SecretKey> encryptionKey = encryptionEnabled ? Optional.of(createRandomAesEncryptionKey()) : Optional.empty();
        PageSerializer serializer = new PageSerializer(blockSerde, compressionEnabled, encryptionKey, blockSize);
        PageDeserializer deserializer = new PageDeserializer(blockSerde, compressionEnabled, encryptionKey, blockSize);

        Page page = createTestPage(numberOfEntries);
        Slice serialized = serializer.serialize(page);
        Page deserialized = deserializer.deserialize(serialized);
        assertEquals(deserialized.getChannelCount(), 1);

        VariableWidthBlock expected = (VariableWidthBlock) page.getBlock(0);
        VariableWidthBlock actual = (VariableWidthBlock) deserialized.getBlock(0);

        Assertions.assertThat(actual.getRawSlice().getBytes()).isEqualTo(expected.getRawSlice().getBytes());
    }

    private static Page createTestPage(int numberOfEntries)
    {
        VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, 1, 1000);
        blockBuilder.writeInt(numberOfEntries);
        for (int i = 0; i < numberOfEntries; i++) {
            blockBuilder.writeLong(i);
        }
        blockBuilder.closeEntry();
        return new Page(blockBuilder.build());
    }

    private static class RolloverBlockSerde
            implements BlockEncodingSerde
    {
        @Override
        public Block readBlock(SliceInput input)
        {
            int numberOfEntries = input.readInt();
            VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, 1, 1000);
            blockBuilder.writeInt(numberOfEntries);
            for (int i = 0; i < numberOfEntries; ++i) {
                // read 8 bytes at a time
                blockBuilder.writeLong(input.readLong());
            }
            blockBuilder.closeEntry();
            return blockBuilder.build();
        }

        @Override
        public void writeBlock(SliceOutput output, Block block)
        {
            int offset = 0;
            int numberOfEntries = block.getInt(0, offset);
            output.writeInt(numberOfEntries);
            offset += 4;
            for (int i = 0; i < numberOfEntries; ++i) {
                long value = block.getLong(0, offset);
                offset += 8;
                long b7 = value >> 56 & 0xffL;
                long b6 = value >> 48 & 0xffL;
                long b5 = value >> 40 & 0xffL;
                long b4 = value >> 32 & 0xffL;
                long b3 = value >> 24 & 0xffL;
                long b2 = value >> 16 & 0xffL;
                long b1 = value >> 8 & 0xffL;
                long b0 = value & 0xffL;
                // write one byte at a time
                output.writeByte((int) b0);
                output.writeByte((int) b1);
                output.writeByte((int) b2);
                output.writeByte((int) b3);
                output.writeByte((int) b4);
                output.writeByte((int) b5);
                output.writeByte((int) b6);
                output.writeByte((int) b7);
            }
        }

        @Override
        public Type readType(SliceInput sliceInput)
        {
            throw new RuntimeException("not implemented");
        }

        @Override
        public void writeType(SliceOutput sliceOutput, Type type)
        {
            throw new RuntimeException("not implemented");
        }
    }
}
