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
import io.trino.execution.buffer.CompressionCodec;
import io.trino.execution.buffer.PagesSerdeUtil;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.PageAssertions;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.TestingBlockEncodingSerde;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.List;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.MoreFiles.listFiles;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.trino.execution.buffer.CompressionCodec.LZ4;
import static io.trino.execution.buffer.CompressionCodec.NONE;
import static io.trino.execution.buffer.PagesSerdeUtil.isSerializedPageCompressed;
import static io.trino.execution.buffer.PagesSerdeUtil.isSerializedPageEncrypted;
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
    private File spillPath;

    @BeforeAll
    public void setUp()
            throws IOException
    {
        spillPath = Files.createTempDirectory("tmp").toFile();
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        executor.shutdown();
        deleteRecursively(spillPath.toPath(), ALLOW_INSECURE);
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

    private void assertSpill(CompressionCodec compressionCodec, boolean encryption)
            throws Exception
    {
        FileSingleStreamSpillerFactory spillerFactory = new FileSingleStreamSpillerFactory(
                executor, // executor won't be closed, because we don't call destroy() on the spiller factory
                new TestingBlockEncodingSerde(),
                new SpillerStats(),
                ImmutableList.of(spillPath.toPath()),
                1.0,
                compressionCodec,
                encryption);
        LocalMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext("test");
        SingleStreamSpiller singleStreamSpiller = spillerFactory.create(TYPES, bytes -> {}, memoryContext);
        assertThat(singleStreamSpiller instanceof FileSingleStreamSpiller).isTrue();
        FileSingleStreamSpiller spiller = (FileSingleStreamSpiller) singleStreamSpiller;

        Page page = buildPage();

        // The spillers will reserve memory in their constructors
        assertThat(memoryContext.getBytes()).isEqualTo(4096);
        spiller.spill(page).get();
        spiller.spill(Iterators.forArray(page, page, page)).get();
        assertThat(listFiles(spillPath.toPath()).size()).isEqualTo(1);

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

        // The spillers release their memory reservations when they are closed, therefore at this point
        // they will have non-zero memory reservation.
        // assertEquals(memoryContext.getBytes(), 0);

        Iterator<Page> spilledPagesIterator = spiller.getSpilledPages();
        assertThat(memoryContext.getBytes()).isEqualTo(FileSingleStreamSpiller.BUFFER_SIZE);
        ImmutableList<Page> spilledPages = ImmutableList.copyOf(spilledPagesIterator);
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

        spiller.close();
        assertThat(listFiles(spillPath.toPath()).size()).isEqualTo(0);
        assertThat(memoryContext.getBytes()).isEqualTo(0);
    }

    private Page buildPage()
    {
        BlockBuilder col1 = BIGINT.createBlockBuilder(null, 1);
        BlockBuilder col2 = DOUBLE.createBlockBuilder(null, 1);
        BlockBuilder col3 = VARBINARY.createBlockBuilder(null, 1);

        BIGINT.writeLong(col1, 42);
        DOUBLE.writeDouble(col2, 43.0);
        VARBINARY.writeSlice(col3, Slices.allocate(16).getOutput().appendDouble(43.0).appendLong(1).slice());

        return new Page(col1.build(), col2.build(), col3.build());
    }
}
