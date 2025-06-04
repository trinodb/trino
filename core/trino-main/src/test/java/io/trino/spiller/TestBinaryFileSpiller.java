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
import io.airlift.slice.Slices;
import io.trino.FeaturesConfig;
import io.trino.RowPagesBuilder;
import io.trino.execution.buffer.PageSerializer;
import io.trino.execution.buffer.PagesSerdeFactory;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.block.TestingBlockEncodingSerde;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.execution.buffer.PagesSerdes.createSpillingPagesSerdeFactory;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.operator.PageAssertions.assertPageEquals;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestBinaryFileSpiller
{
    private static final List<Type> TYPES = ImmutableList.of(BIGINT, VARCHAR, DOUBLE, BIGINT);

    private File spillPath;
    private SpillerStats spillerStats;
    private FileSingleStreamSpillerFactory singleStreamSpillerFactory;
    private SpillerFactory factory;
    private PageSerializer serializer;
    private AggregatedMemoryContext memoryContext;

    @BeforeAll
    public void setUpClass()
            throws IOException
    {
        spillPath = Files.createTempDirectory("tmp").toFile();
    }

    @BeforeEach
    public void setUp()
    {
        spillerStats = new SpillerStats();
        FeaturesConfig featuresConfig = new FeaturesConfig();
        featuresConfig.setSpillerSpillPaths(ImmutableList.of(spillPath.getAbsolutePath()));
        featuresConfig.setSpillMaxUsedSpaceThreshold(1.0);
        NodeSpillConfig nodeSpillConfig = new NodeSpillConfig();
        BlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde();
        singleStreamSpillerFactory = new FileSingleStreamSpillerFactory(blockEncodingSerde, spillerStats, featuresConfig, nodeSpillConfig);
        factory = new GenericSpillerFactory(singleStreamSpillerFactory);
        PagesSerdeFactory pagesSerdeFactory = createSpillingPagesSerdeFactory(blockEncodingSerde, nodeSpillConfig.getSpillCompressionCodec());
        serializer = pagesSerdeFactory.createSerializer(Optional.empty());
        memoryContext = newSimpleAggregatedMemoryContext();
    }

    @AfterEach
    public void tearDown()
            throws Exception
    {
        singleStreamSpillerFactory.destroy();
        deleteRecursively(spillPath.toPath(), ALLOW_INSECURE);
    }

    @Test
    public void testFileSpiller()
            throws Exception
    {
        try (Spiller spiller = factory.create(TYPES, bytes -> {}, memoryContext)) {
            testSimpleSpiller(spiller);
        }
    }

    @Test
    public void testFileVarbinarySpiller()
            throws Exception
    {
        List<Type> types = ImmutableList.of(BIGINT, DOUBLE, VARBINARY);

        BlockBuilder col1 = BIGINT.createFixedSizeBlockBuilder(1);
        BlockBuilder col2 = DOUBLE.createFixedSizeBlockBuilder(1);
        BlockBuilder col3 = VARBINARY.createBlockBuilder(null, 1);

        BIGINT.writeLong(col1, 42);
        DOUBLE.writeDouble(col2, 43.0);
        VARBINARY.writeSlice(col3, Slices.allocate(16).getOutput().appendDouble(43.0).appendLong(1).slice());

        Page page = new Page(col1.build(), col2.build(), col3.build());

        try (Spiller spiller = factory.create(TYPES, bytes -> {}, memoryContext)) {
            testSpiller(types, spiller, ImmutableList.of(page));
        }
    }

    private void testSimpleSpiller(Spiller spiller)
            throws ExecutionException, InterruptedException
    {
        RowPagesBuilder builder = RowPagesBuilder.rowPagesBuilder(TYPES);
        builder.addSequencePage(10, 0, 5, 10, 15);
        builder.pageBreak();
        builder.addSequencePage(10, 0, -5, -10, -15);
        List<Page> firstSpill = builder.build();

        builder = RowPagesBuilder.rowPagesBuilder(TYPES);
        builder.addSequencePage(10, 10, 15, 20, 25);
        builder.pageBreak();
        builder.addSequencePage(10, -10, -15, -20, -25);
        List<Page> secondSpill = builder.build();

        testSpiller(TYPES, spiller, firstSpill, secondSpill);
    }

    @SafeVarargs
    private void testSpiller(List<Type> types, Spiller spiller, List<Page>... spills)
            throws ExecutionException, InterruptedException
    {
        long spilledBytesBefore = spillerStats.getTotalSpilledBytes();
        long spilledBytes = 0;

        assertThat(memoryContext.getBytes()).isEqualTo(0);
        for (List<Page> spill : spills) {
            spilledBytes += spill.stream()
                    .mapToLong(page -> serializer.serialize(page).length())
                    .sum();
            spiller.spill(spill.iterator()).get();
        }
        assertThat(spillerStats.getTotalSpilledBytes() - spilledBytesBefore).isEqualTo(spilledBytes);
        // At this point, the buffers should still be accounted for in the memory context, because
        // the spiller (FileSingleStreamSpiller) doesn't release its memory reservation until it's closed.
        assertThat(memoryContext.getBytes()).isEqualTo((long) spills.length * FileSingleStreamSpiller.BUFFER_SIZE);

        List<Iterator<Page>> actualSpills = spiller.getSpills();
        assertThat(actualSpills).hasSize(spills.length);

        for (int i = 0; i < actualSpills.size(); i++) {
            List<Page> actualSpill = ImmutableList.copyOf(actualSpills.get(i));
            List<Page> expectedSpill = spills[i];

            assertThat(actualSpill).hasSize(expectedSpill.size());
            for (int j = 0; j < actualSpill.size(); j++) {
                assertPageEquals(types, actualSpill.get(j), expectedSpill.get(j));
            }
        }
        spiller.close();
        assertThat(memoryContext.getBytes()).isEqualTo(0);
    }
}
