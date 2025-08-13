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
package io.trino.lance.file;

import com.google.common.collect.ImmutableList;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Collections.nCopies;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(3)
@Warmup(iterations = 30, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 20, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@OperationsPerInvocation(BenchmarkColumnReaders.ROWS)
public class BenchmarkColumnReaders
{
    public static final int ROWS = 10_000_000;

    @Benchmark
    public Object readBigIntJNI(BigIntBenchmarkData data)
            throws Exception
    {
        try (LanceJNIReader reader = data.createJNIReader()) {
            return readColumnJNI(reader);
        }
    }

    @Benchmark
    public Object readBigInt(BigIntBenchmarkData data)
            throws Exception
    {
        try (LanceReader reader = data.createReader()) {
            return readColumn(reader);
        }
    }

    @Benchmark
    public Object readVarcharJNI(VarcharBenchmarkData data)
            throws Exception
    {
        try (LanceJNIReader reader = data.createJNIReader()) {
            return readColumnJNI(reader);
        }
    }

    @Benchmark
    public Object readVarchar(VarcharBenchmarkData data)
            throws Exception
    {
        try (LanceReader reader = data.createReader()) {
            return readColumn(reader);
        }
    }

    @Benchmark
    public Object readListJNI(ListBenchmarkData data)
            throws Exception
    {
        try (LanceJNIReader reader = data.createJNIReader()) {
            return readColumnJNI(reader);
        }
    }

    @Benchmark
    public Object readList(ListBenchmarkData data)
            throws Exception
    {
        try (LanceReader reader = data.createReader()) {
            return readColumn(reader);
        }
    }

    @Benchmark
    public Object readStructJNI(StructBenchmarkData data)
            throws Exception
    {
        try (LanceJNIReader reader = data.createJNIReader()) {
            return readColumnJNI(reader);
        }
    }

    @Benchmark
    public Object readStruct(StructBenchmarkData data)
            throws Exception
    {
        try (LanceReader reader = data.createReader()) {
            return readColumn(reader);
        }
    }

    private Object readColumn(LanceReader reader)
            throws IOException
    {
        List<Block> blocks = new ArrayList<>();
        for (SourcePage page = reader.nextSourcePage(); page != null; page = reader.nextSourcePage()) {
            blocks.add(page.getBlock(0));
        }
        return blocks;
    }

    private Object readColumnJNI(LanceJNIReader reader)
            throws IOException
    {
        List<Block> blocks = new ArrayList<>();
        for (SourcePage page = reader.nextSourcePage(); page != null; page = reader.nextSourcePage()) {
            blocks.add(page.getBlock(0));
        }
        return blocks;
    }

    @State(Thread)
    public static class ListBenchmarkData
            extends BenchmarkData
    {
        public static final Type LIST_TYPE = LanceTester.arrayType(BIGINT);
        public static final int MAX_LIST_SIZE = 32;

        @Setup
        public void setup()
                throws Exception
        {
            setup(LIST_TYPE, createValues(), true);
        }

        private Iterator<?> createValues()
        {
            List<List<Long>> values = new ArrayList<>();
            for (int i = 0; i < ROWS; i++) {
                if (i % 9 == 0) {
                    values.add(null);
                }
                else {
                    List<Long> row = new ArrayList<>();
                    for (int j = 0; j < i % MAX_LIST_SIZE; j++) {
                        if (j % 7 == 0) {
                            row.add(null);
                        }
                        else {
                            row.add(ThreadLocalRandom.current().nextLong());
                        }
                    }
                    values.add(row);
                }
            }
            return values.iterator();
        }
    }

    @State(Thread)
    public static class StructBenchmarkData
            extends BenchmarkData
    {
        public static final int NUM_FILEDS = 3;
        public static final Type STRUCT_TYPE = LanceTester.rowType(BIGINT, BIGINT, BIGINT);

        @Setup
        public void setup()
                throws Exception
        {
            setup(STRUCT_TYPE, createValues(), true);
        }

        private Iterator<?> createValues()
        {
            List<List<Long>> values = new ArrayList<>();
            for (int i = 0; i < ROWS; i++) {
                if (i % 9 == 0) {
                    values.add(null);
                }
                else {
                    values.add(nCopies(NUM_FILEDS, (long) i));
                }
            }
            return values.iterator();
        }
    }

    @State(Thread)
    public static class BigIntBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(BIGINT, createValues(), true);
        }

        private Iterator<?> createValues()
        {
            List<Long> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                long value = ThreadLocalRandom.current().nextLong();
                if (value % 7 == 0) {
                    values.add(null);
                }
                else {
                    values.add(value);
                }
            }
            return values.iterator();
        }
    }

    @State(Thread)
    public static class VarcharBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(VARCHAR, createValues(), true);
        }

        private Iterator<?> createValues()
        {
            List<String> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                long value = ThreadLocalRandom.current().nextLong();
                if (value % 7 == 0) {
                    values.add(null);
                }
                else {
                    values.add(Long.toString(value));
                }
            }
            return values.iterator();
        }
    }

    public abstract static class BenchmarkData
    {
        protected final Random random = new Random(0);
        private Type type;
        private Path temporaryDirectory;
        private File lanceFile;
        private LanceDataSource dataSource;

        public void setup(Type type, Iterator<?> values, boolean nullable)
                throws Exception
        {
            this.type = type;
            temporaryDirectory = createTempDirectory(null);
            lanceFile = temporaryDirectory.resolve(randomUUID().toString()).toFile();
            LanceTester.writeLanceColumnJNI(lanceFile, type, newArrayList(values), nullable);
            dataSource = new FileLanceDataSource(lanceFile);
        }

        @TearDown
        public void tearDown()
                throws IOException
        {
            deleteRecursively(temporaryDirectory, ALLOW_INSECURE);
        }

        LanceReader createReader()
                throws IOException
        {
            return new LanceReader(dataSource, ImmutableList.of(0), Optional.empty(), newSimpleAggregatedMemoryContext());
        }

        public LanceJNIReader createJNIReader()
                throws IOException
        {
            return new LanceJNIReader(lanceFile, ImmutableList.of(type.getDisplayName()), ImmutableList.of(type));
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        benchmark(BenchmarkColumnReaders.class).run();
    }
}
