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
package io.trino.parquet.reader;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.type.Type;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTestUtils.createParquetReader;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestByteStreamSplitEncoding
{
    @Test
    public void testReadFloatDouble()
            throws URISyntaxException, IOException
    {
        List<String> columnNames = ImmutableList.of("columnA", "columnB");
        List<Type> types = ImmutableList.of(REAL, DOUBLE);

        ParquetDataSource dataSource = new FileParquetDataSource(
                new File(Resources.getResource("byte_stream_split_float_and_double.parquet").toURI()),
                new ParquetReaderOptions());
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        ParquetReader reader = createParquetReader(dataSource, parquetMetadata, newSimpleAggregatedMemoryContext(), types, columnNames);

        readAndCompare(reader, getExpectedValues());
    }

    private static List<List<Double>> getExpectedValues()
    {
        ImmutableList.Builder<Double> floatsBuilder = ImmutableList.builder();
        for (int i = 0; i < 10; i++) {
            IntStream.range(0, 10)
                    .mapToDouble(j -> j * 1.3)
                    .forEach(floatsBuilder::add);
        }

        ImmutableList.Builder<Double> doublesBuilder = ImmutableList.builder();
        for (int i = 0; i < 10; i++) {
            IntStream.range(0, 10)
                    .mapToDouble(j -> j * 1.5)
                    .forEach(doublesBuilder::add);
        }
        return ImmutableList.of(floatsBuilder.build(), doublesBuilder.build());
    }

    private static void readAndCompare(ParquetReader reader, List<List<Double>> expected)
            throws IOException
    {
        int rowCount = 0;
        int pageCount = 0;
        Page page = reader.nextPage();
        while (page != null) {
            assertThat(page.getChannelCount()).isEqualTo(2);
            if (pageCount % 2 == 1) { // Skip loading every alternative page
                for (int channel = 0; channel < page.getChannelCount(); channel++) {
                    Block block = page.getBlock(channel).getLoadedBlock();
                    List<Double> expectedValues = expected.get(channel);
                    for (int postition = 0; postition < block.getPositionCount(); postition++) {
                        if (block instanceof IntArrayBlock) {
                            assertEquals(REAL.getObjectValue(SESSION, block, postition), expectedValues.get(rowCount + postition).floatValue());
                        }
                        else {
                            assertEquals(DOUBLE.getObjectValue(SESSION, block, postition), expectedValues.get(rowCount + postition));
                        }
                    }
                }
            }
            rowCount += page.getPositionCount();
            pageCount++;
            page = reader.nextPage();
        }
        assertThat(rowCount).isEqualTo(100);
    }
}
