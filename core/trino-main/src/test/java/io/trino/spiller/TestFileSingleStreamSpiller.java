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
package io.trino.spiller;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.execution.buffer.CompressionCodec;
import io.trino.execution.buffer.PageDeserializer;
import io.trino.execution.buffer.PageSerializer;
import io.trino.execution.buffer.PagesSerdeFactory;
import io.trino.execution.buffer.PagesSerdeUtil;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.PageAssertions;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.TestingBlockEncodingSerde;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.MoreFiles.listFiles;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.execution.buffer.CompressionCodec.LZ4;
import static io.trino.execution.buffer.CompressionCodec.NONE;
import static io.trino.execution.buffer.PagesSerdeUtil.isSerializedPageCompressed;
import static io.trino.execution.buffer.PagesSerdeUtil.isSerializedPageEncrypted;
import static io.trino.execution.buffer.PagesSerdes.createSpillingPagesSerdeFactory;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.nio.file.Files.newInputStream;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestFileSingleStreamSpiller
{
    private static final List<Type> TYPES = ImmutableList.of(BIGINT, DOUBLE, VARBINARY);

    private final ListeningExecutorService executor = listeningDecorator(newCachedThreadPool());

    @AfterAll
    public void tearDown()
            throws Exception
    {
        executor.shutdown();
    }

    @Test
    public void testSpill()
            throws Exception
    {
        assertSpill(NONE, false);
    }

    @Test
    public void testSpillCompression()
            throws Exception
    {
        assertSpill(LZ4, false);
    }

    @Test
    public void testSpillEncryption()
            throws Exception
    {
        assertSpill(NONE, true);
    }

    @Test
    public void testSpillEncryptionWithCompression()
            throws Exception
    {
        assertSpill(LZ4, true);
    }

    @Test
    public void testMultiFileSpill()
            throws Exception
    {
        // Writes four pages in a single call and verifies spilled bytes, non-empty spill files,
        // and round-robin ordering when reading the pages back.
        assertMultiFileSpill(spiller -> ImmutableList.copyOf(spiller.getSpilledPages()), false);
    }

    @Test
    public void testMultiFileSpillMultipleCalls()
            throws Exception
    {
        // Performs two spill() calls to ensure rotation resumes on the expected file.
        assertMultiFileSpill(spiller -> ImmutableList.copyOf(spiller.getSpilledPages()), true);
    }

    @Test
    public void testGetAllSpilledPagesMultiFile()
            throws Exception
    {
        // Retrieves all spilled pages asynchronously from multiple files and verifies the combined
        // result preserves the round-robin write order.
        assertMultiFileSpill(spiller -> getFutureValue(spiller.getAllSpilledPages()), false);
    }

    @Test
    public void testGetAllSpilledPagesMultiFileMultipleCalls()
            throws Exception
    {
        // Calls getAllSpilledPages() after spilling in multiple batches to confirm partially filled
        // files are preserved and the merged ordering remains correct.
        assertMultiFileSpill(spiller -> getFutureValue(spiller.getAllSpilledPages()), true);
    }

    private void assertSpill(CompressionCodec compressionCodec, boolean encryption)
            throws Exception
    {
        File spillPath = Files.createTempDirectory("tmp").toFile();
        try {
            FileSingleStreamSpillerFactory spillerFactory = new FileSingleStreamSpillerFactory(
                    executor, // executor won't be closed, because we don't call destroy() on the spiller factory
                    new TestingBlockEncodingSerde(),
                    new SpillerStats(),
                    ImmutableList.of(spillPath.toPath()),
                    1,
                    1.0,
                    compressionCodec,
                    encryption);
            LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext("test");
            SingleStreamSpiller singleStreamSpiller = spillerFactory.create(TYPES, bytes -> {}, memoryContext);
            assertThat(singleStreamSpiller).isInstanceOf(FileSingleStreamSpiller.class);
            FileSingleStreamSpiller spiller = (FileSingleStreamSpiller) singleStreamSpiller;

            Page page = buildPage();

            // The spillers will reserve memory in their constructors
            assertThat(memoryContext.getBytes()).isEqualTo(4096);
            spiller.spill(page).get();
            spiller.spill(Iterators.forArray(page, page, page)).get();
            assertThat(listFiles(spillPath.toPath())).hasSize(1);

            // The spillers release their memory reservations when they are closed, therefore at this point
            // they will have non-zero memory reservation.
            // assertEquals(memoryContext.getBytes(), 0);

            Iterator<Page> spilledPagesIterator = spiller.getSpilledPages();
            assertThat(memoryContext.getBytes()).isEqualTo(SpillFile.BUFFER_SIZE);
            List<Page> spilledPages = ImmutableList.copyOf(spilledPagesIterator);
            // The spillers release their memory reservations when they are closed, therefore at this point
            // they will have non-zero memory reservation.
            // assertEquals(memoryContext.getBytes(), 0);

            assertThat(4).isEqualTo(spilledPages.size());
            for (int i = 0; i < 4; ++i) {
                PageAssertions.assertPageEquals(TYPES, page, spilledPages.get(i));
            }

            // Repeated reads are disallowed
            assertThatThrownBy(spiller::getSpilledPages)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Repeated reads are disallowed to prevent potential resource leaks");

            // Assert the spill codec flags match the expected configuration
            try (InputStream is = newInputStream(listFiles(spillPath.toPath()).get(0))) {
                Iterator<Slice> serializedPages = PagesSerdeUtil.readSerializedPages(is);
                assertThat(serializedPages.hasNext())
                        .describedAs("at least one page should be successfully read back")
                        .isTrue();
                Slice serializedPage = serializedPages.next();
                assertThat(isSerializedPageCompressed(serializedPage)).isEqualTo(compressionCodec == LZ4);
                assertThat(isSerializedPageEncrypted(serializedPage)).isEqualTo(encryption);
            }

            spiller.close();
            assertThat(listFiles(spillPath.toPath())).isEmpty();
            assertThat(memoryContext.getBytes()).isEqualTo(0);
        }
        finally {
            deleteRecursively(spillPath.toPath(), ALLOW_INSECURE);
        }
    }

    private void assertMultiFileSpill(Function<FileSingleStreamSpiller, List<Page>> readPages, boolean multipleCalls)
            throws Exception
    {
        // Create two temporary directories to be used for spilling
        File spillPath1 = Files.createTempDirectory("tmp1").toFile();
        File spillPath2 = Files.createTempDirectory("tmp2").toFile();
        try {
            // Set up serializer and memory tracking objects
            SpillerStats stats = new SpillerStats();
            PagesSerdeFactory serdeFactory = createSpillingPagesSerdeFactory(new TestingBlockEncodingSerde(), NONE);
            LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext("test");
            PageSerializer serializer = serdeFactory.createSerializer(Optional.empty());
            PageDeserializer deserializer = serdeFactory.createDeserializer(Optional.empty());

            // Rotate between the two files using round-robin distribution
            FileSingleStreamSpiller spiller = new FileSingleStreamSpiller(
                    serdeFactory,
                    Optional.empty(),
                    executor,
                    ImmutableList.of(spillPath1.toPath(), spillPath2.toPath()),
                    stats,
                    bytes -> {},
                    memoryContext,
                    () -> {});

            // Build a sequence of pages with distinct numbers
            Page p0 = buildPage(0);
            Page p1 = buildPage(1);
            Page p2 = buildPage(2);
            Page p3 = buildPage(3);

            long expectedRawBytes = p0.getSizeInBytes() + p1.getSizeInBytes() + p2.getSizeInBytes() + p3.getSizeInBytes();
            long expectedSerializedBytes =
                    serializer.serialize(p0).length() +
                            serializer.serialize(p1).length() +
                            serializer.serialize(p2).length() +
                            serializer.serialize(p3).length();

            DataSize spilled;
            if (multipleCalls) {
                DataSize d1 = spiller.spill(Iterators.forArray(p0)).get();
                DataSize d2 = spiller.spill(Iterators.forArray(p1, p2, p3)).get();
                spilled = DataSize.ofBytes(d1.toBytes() + d2.toBytes());
            }
            else {
                spilled = spiller.spill(Iterators.forArray(p0, p1, p2, p3)).get();
            }
            assertThat(spilled.toBytes()).isEqualTo(expectedRawBytes);
            assertThat(stats.getTotalSpilledBytes()).isEqualTo(expectedSerializedBytes);

            // Read pages using the provided function and verify order
            List<Page> pages = readPages.apply(spiller);
            assertThat(pages).hasSize(4);
            for (int i = 0; i < pages.size(); i++) {
                assertThat(BIGINT.getLong(pages.get(i).getBlock(0), 0)).isEqualTo(i);
            }

            // Validate that two non-empty spill files were created
            assertThat(listFiles(spillPath1.toPath()).size() + listFiles(spillPath2.toPath()).size()).isEqualTo(2);
            assertThat(Files.size(listFiles(spillPath1.toPath()).get(0))).isGreaterThan(0);
            assertThat(Files.size(listFiles(spillPath2.toPath()).get(0))).isGreaterThan(0);

            // Validate content distribution per spill file
            try (InputStream input1 = newInputStream(listFiles(spillPath1.toPath()).get(0))) {
                Iterator<Slice> serializedPages1 = PagesSerdeUtil.readSerializedPages(input1);
                List<Long> values = new ArrayList<>();
                while (serializedPages1.hasNext()) {
                    Page page = deserializer.deserialize(serializedPages1.next());
                    values.add(BIGINT.getLong(page.getBlock(0), 0));
                }
                assertThat(values).containsExactly(0L, 2L);
            }
            try (InputStream input2 = newInputStream(listFiles(spillPath2.toPath()).get(0))) {
                Iterator<Slice> serializedPages2 = PagesSerdeUtil.readSerializedPages(input2);
                List<Long> values = new ArrayList<>();
                while (serializedPages2.hasNext()) {
                    Page page = deserializer.deserialize(serializedPages2.next());
                    values.add(BIGINT.getLong(page.getBlock(0), 0));
                }
                assertThat(values).containsExactly(1L, 3L);
            }

            // Close spiller to release resources
            spiller.close();
        }
        finally {
            // Clean up temporary directories
            deleteRecursively(spillPath1.toPath(), ALLOW_INSECURE);
            deleteRecursively(spillPath2.toPath(), ALLOW_INSECURE);
        }
    }

    private Page buildPage()
    {
        return buildPage(42);
    }

    private Page buildPage(int pageNumber)
    {
        BlockBuilder col1 = BIGINT.createFixedSizeBlockBuilder(1);
        BlockBuilder col2 = DOUBLE.createFixedSizeBlockBuilder(1);
        BlockBuilder col3 = VARBINARY.createBlockBuilder(null, 1);

        BIGINT.writeLong(col1, pageNumber);
        DOUBLE.writeDouble(col2, pageNumber + 0.5);
        VARBINARY.writeSlice(col3, Slices.allocate(16).getOutput().appendInt(pageNumber).appendLong(1).slice());

        return new Page(col1.build(), col2.build(), col3.build());
    }
}
