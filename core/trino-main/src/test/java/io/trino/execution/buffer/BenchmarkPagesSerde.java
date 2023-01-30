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
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.TestingBlockEncodingSerde;
import io.trino.spi.type.Type;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.RunnerException;
import org.testng.annotations.Test;

import javax.crypto.SecretKey;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.operator.PageAssertions.assertPageEquals;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.util.Ciphers.createRandomAesEncryptionKey;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.util.concurrent.TimeUnit.SECONDS;

@State(Scope.Thread)
@OutputTimeUnit(SECONDS)
@Fork(1)
@Warmup(iterations = 12, time = 1, timeUnit = SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = SECONDS)
@BenchmarkMode(Mode.Throughput)
public class BenchmarkPagesSerde
{
    @Benchmark
    public void serialize(BenchmarkData data, Blackhole blackhole)
    {
        Page[] pages = data.dataPages;
        PageSerializer serializer = data.serializer;
        for (int i = 0; i < pages.length; i++) {
            blackhole.consume(serializer.serialize(pages[i]));
        }
    }

    @Benchmark
    public void deserialize(BenchmarkData data, Blackhole blackhole)
    {
        Slice[] serializedPages = data.serializedPages;
        PageDeserializer deserializer = data.deserializer;
        for (int i = 0; i < serializedPages.length; i++) {
            blackhole.consume(deserializer.deserialize(serializedPages[i]));
        }
    }

    @Test
    public void testBenchmarkData()
    {
        BenchmarkData data = new BenchmarkData();
        data.compressed = true;
        data.initialize();
        Slice[] serializedPages = data.serializedPages;
        PageDeserializer deserializer = data.deserializer;
        // Sanity test by deserializing and checking against the original pages
        for (int i = 0; i < serializedPages.length; i++) {
            assertPageEquals(BenchmarkData.TYPES, deserializer.deserialize(serializedPages[i]), data.dataPages[i]);
        }
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private static final int ROW_COUNT = 15000;
        private static final List<Type> TYPES = ImmutableList.of(VARCHAR);
        @Param({"true", "false"})
        private boolean encrypted;
        @Param({"true", "false"})
        private boolean compressed;
        @Param("1000")
        private int randomSeed = 1000;

        private PageSerializer serializer;
        private PageDeserializer deserializer;
        private Page[] dataPages;
        private Slice[] serializedPages;

        @Setup
        public void initialize()
        {
            PagesSerdeFactory serdeFactory = new PagesSerdeFactory(new TestingBlockEncodingSerde(), compressed);
            Optional<SecretKey> encryptionKey = encrypted ? Optional.of(createRandomAesEncryptionKey()) : Optional.empty();
            serializer = serdeFactory.createSerializer(encryptionKey);
            deserializer = serdeFactory.createDeserializer(encryptionKey);
            dataPages = createPages();
            serializedPages = createSerializedPages();
        }

        public Page[] getDataPages()
        {
            return dataPages;
        }

        private Slice[] createSerializedPages()
        {
            Slice[] result = new Slice[dataPages.length];
            for (int i = 0; i < result.length; i++) {
                result[i] = serializer.serialize(dataPages[i]);
            }
            return result;
        }

        private Page[] createPages()
        {
            Random random = new Random(randomSeed);
            List<Page> pages = new ArrayList<>();
            int remainingRows = ROW_COUNT;
            PageBuilder pageBuilder = new PageBuilder(TYPES);
            while (remainingRows > 0) {
                int rows = 100 + random.nextInt(900); // 100 - 1000 rows per pass
                List<Object>[] testRows = generateTestRows(random, TYPES, rows);
                remainingRows -= rows;
                for (int i = 0; i < testRows.length; i++) {
                    writeRow(testRows[i], pageBuilder.getBlockBuilder(0));
                }
                pageBuilder.declarePositions(rows);
                pages.add(pageBuilder.build());
                pageBuilder.reset();
            }
            return pages.toArray(Page[]::new);
        }

        private void writeRow(List<Object> testRow, BlockBuilder blockBuilder)
        {
            for (Object fieldValue : testRow) {
                if (fieldValue == null) {
                    blockBuilder.appendNull();
                }
                else if (fieldValue instanceof String) {
                    VARCHAR.writeSlice(blockBuilder, utf8Slice((String) fieldValue));
                }
                else {
                    throw new UnsupportedOperationException();
                }
            }
        }

        // copied & modifed from TestRowBlock
        private List<Object>[] generateTestRows(Random random, List<Type> fieldTypes, int numRows)
        {
            @SuppressWarnings("unchecked")
            List<Object>[] testRows = new List[numRows];
            for (int i = 0; i < numRows; i++) {
                List<Object> testRow = new ArrayList<>(fieldTypes.size());
                for (int j = 0; j < fieldTypes.size(); j++) {
                    if (fieldTypes.get(j) == VARCHAR) {
                        int mode = random.nextInt(4); // 25% null, 25% repeat previous value
                        if (mode == 0) {
                            testRow.add(null);
                        }
                        else if (i > 0 && mode == 1) {
                            // Repeat values to make compression more interesting
                            testRow.add(testRows[i - 1].get(j));
                        }
                        else {
                            byte[] data = new byte[random.nextInt(256)];
                            random.nextBytes(data);
                            testRow.add(new String(data, ISO_8859_1));
                        }
                    }
                    else {
                        throw new UnsupportedOperationException();
                    }
                }
                testRows[i] = testRow;
            }
            return testRows;
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        BenchmarkData data = new BenchmarkData();
        data.compressed = true; // Get usable stats on compressibility
        data.initialize();
        System.out.println("Page Size Avg: " + Arrays.stream(data.dataPages).mapToLong(Page::getSizeInBytes).average().getAsDouble());
        System.out.println("Page Size Min: " + Arrays.stream(data.dataPages).mapToLong(Page::getSizeInBytes).min().getAsLong());
        System.out.println("Page Size Max: " + Arrays.stream(data.dataPages).mapToLong(Page::getSizeInBytes).max().getAsLong());
        System.out.println("Page Size Sum: " + Arrays.stream(data.dataPages).mapToLong(Page::getSizeInBytes).sum());
        System.out.println("Page count: " + data.dataPages.length);
        System.out.println("Compressed: " + Arrays.stream(data.serializedPages).filter(PagesSerdeUtil::isSerializedPageCompressed).count());

        benchmark(BenchmarkPagesSerde.class)
                .withOptions(optionsBuilder -> optionsBuilder.jvmArgs("-Xms4g", "-Xmx4g"))
                .run();
    }
}
