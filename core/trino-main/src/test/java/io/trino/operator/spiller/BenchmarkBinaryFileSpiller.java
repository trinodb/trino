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
package io.trino.operator.spiller;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import io.trino.execution.buffer.CompressionCodec;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.block.TestingBlockEncodingSerde;
import io.trino.spi.type.Type;
import io.trino.spiller.FileSingleStreamSpillerFactory;
import io.trino.spiller.GenericSpillerFactory;
import io.trino.spiller.Spiller;
import io.trino.spiller.SpillerFactory;
import io.trino.spiller.SpillerStats;
import io.trino.tpch.LineItem;
import io.trino.tpch.LineItemGenerator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.concurrent.TimeUnit.SECONDS;

@State(Scope.Thread)
@OutputTimeUnit(SECONDS)
@Fork(3)
@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkBinaryFileSpiller
{
    private static final List<Type> TYPES = ImmutableList.of(BIGINT, BIGINT, DOUBLE, createUnboundedVarcharType(), DOUBLE);
    private static final BlockEncodingSerde BLOCK_ENCODING_SERDE = new TestingBlockEncodingSerde();
    private static final Path SPILL_PATH = Paths.get(System.getProperty("java.io.tmpdir"), "spills");

    @Benchmark
    public void write(BenchmarkData data)
            throws ExecutionException, InterruptedException
    {
        try (Spiller spiller = data.createSpiller()) {
            spiller.spill(data.getPages().iterator()).get();
        }
    }

    @Benchmark
    public void read(BenchmarkData data)
    {
        List<Iterator<Page>> spills = data.getReadSpiller().getSpills();
        for (Iterator<Page> spill : spills) {
            while (spill.hasNext()) {
                Page next = spill.next();
                next.getPositionCount();
            }
        }
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private final SpillerStats spillerStats = new SpillerStats();

        @Param("10000")
        private int rowsPerPage = 10000;

        @Param("10")
        private int pagesCount = 10;

        @Param("NONE")
        private CompressionCodec compressionCodec;

        @Param("true")
        private boolean encryptionEnabled;

        private List<Page> pages;
        private Spiller readSpiller;

        private FileSingleStreamSpillerFactory singleStreamSpillerFactory;
        private SpillerFactory spillerFactory;

        @Setup
        public void setup()
                throws ExecutionException, InterruptedException
        {
            singleStreamSpillerFactory = new FileSingleStreamSpillerFactory(
                    MoreExecutors.newDirectExecutorService(),
                    BLOCK_ENCODING_SERDE,
                    spillerStats,
                    ImmutableList.of(SPILL_PATH),
                    1.0,
                    compressionCodec,
                    encryptionEnabled);
            spillerFactory = new GenericSpillerFactory(singleStreamSpillerFactory);
            pages = createInputPages();
            readSpiller = spillerFactory.create(TYPES, bytes -> {}, newSimpleAggregatedMemoryContext());
            readSpiller.spill(pages.iterator()).get();
        }

        @TearDown
        public void tearDown()
        {
            readSpiller.close();
            singleStreamSpillerFactory.destroy();
        }

        private List<Page> createInputPages()
        {
            ImmutableList.Builder<Page> pages = ImmutableList.builder();

            PageBuilder pageBuilder = new PageBuilder(TYPES);
            LineItemGenerator lineItemGenerator = new LineItemGenerator(1, 1, 1);
            for (int j = 0; j < pagesCount; j++) {
                Iterator<LineItem> iterator = lineItemGenerator.iterator();
                for (int i = 0; i < rowsPerPage; i++) {
                    pageBuilder.declarePosition();

                    LineItem lineItem = iterator.next();
                    BIGINT.writeLong(pageBuilder.getBlockBuilder(0), lineItem.orderKey());
                    BIGINT.writeLong(pageBuilder.getBlockBuilder(1), lineItem.discountPercent());
                    DOUBLE.writeDouble(pageBuilder.getBlockBuilder(2), lineItem.discount());
                    VARCHAR.writeString(pageBuilder.getBlockBuilder(3), lineItem.returnFlag());
                    DOUBLE.writeDouble(pageBuilder.getBlockBuilder(4), lineItem.extendedPrice());
                }
                pages.add(pageBuilder.build());
                pageBuilder.reset();
            }

            return pages.build();
        }

        public List<Page> getPages()
        {
            return pages;
        }

        public Spiller getReadSpiller()
        {
            return readSpiller;
        }

        public Spiller createSpiller()
        {
            return spillerFactory.create(TYPES, bytes -> {}, newSimpleAggregatedMemoryContext());
        }
    }
}
