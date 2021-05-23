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

import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.SqlDecimal;
import org.joda.time.DateTimeZone;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.trino.orc.OrcTester.Format.ORC_12;
import static io.trino.orc.OrcTester.READER_OPTIONS;
import static io.trino.orc.OrcTester.writeOrcColumnHive;
import static io.trino.orc.metadata.CompressionKind.NONE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static java.util.UUID.randomUUID;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(3)
@Warmup(iterations = 20, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 20, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkOrcDecimalReader
{
    private static final DecimalType DECIMAL_TYPE = createDecimalType(30, 10);

    @Benchmark
    public Object readDecimal(BenchmarkData data)
            throws Exception
    {
        OrcRecordReader recordReader = data.createRecordReader();
        List<Block> blocks = new ArrayList<>();
        for (Page page = recordReader.nextPage(); page != null; page = recordReader.nextPage()) {
            blocks.add(page.getBlock(0).getLoadedBlock());
        }
        return blocks;
    }

    @Test
    public void testReadDecimal()
            throws Exception
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();
        readDecimal(data);
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private File temporary;
        private File dataPath;

        @Setup
        public void setup()
                throws Exception
        {
            temporary = createTempDir();
            dataPath = new File(temporary, randomUUID().toString());

            writeOrcColumnHive(dataPath, ORC_12, NONE, DECIMAL_TYPE, createDecimalValues().iterator());
        }

        @TearDown
        public void tearDown()
                throws IOException
        {
            deleteRecursively(temporary.toPath(), ALLOW_INSECURE);
        }

        private OrcRecordReader createRecordReader()
                throws IOException
        {
            OrcDataSource dataSource = new FileOrcDataSource(dataPath, READER_OPTIONS);
            OrcReader orcReader = OrcReader.createOrcReader(dataSource, READER_OPTIONS)
                    .orElseThrow(() -> new RuntimeException("File is empty"));
            return orcReader.createRecordReader(
                    orcReader.getRootColumn().getNestedColumns(),
                    ImmutableList.of(DECIMAL_TYPE),
                    OrcPredicate.TRUE,
                    DateTimeZone.UTC, // arbitrary
                    newSimpleAggregatedMemoryContext(),
                    INITIAL_BATCH_SIZE,
                    RuntimeException::new);
        }

        private List<SqlDecimal> createDecimalValues()
        {
            Random random = new Random();
            List<SqlDecimal> values = new ArrayList<>();
            for (int i = 0; i < 1000000; ++i) {
                values.add(new SqlDecimal(BigInteger.valueOf(random.nextLong() % 10000000000L), 10, 5));
            }
            return values;
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkOrcDecimalReader().readDecimal(data);

        benchmark(BenchmarkOrcDecimalReader.class).run();
    }
}
