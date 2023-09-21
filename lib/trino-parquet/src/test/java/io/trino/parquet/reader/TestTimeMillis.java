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
import io.trino.spi.type.SqlTime;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.Type;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTestUtils.createParquetReader;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimeType.TIME_NANOS;
import static io.trino.spi.type.TimeType.TIME_SECONDS;
import static io.trino.testing.DataProviders.toDataProvider;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTimeMillis
{
    @Test(dataProvider = "timeTypeProvider")
    public void testTimeMillsInt32(TimeType timeType)
            throws Exception
    {
        List<String> columnNames = ImmutableList.of("COLUMN1", "COLUMN2");
        List<Type> types = ImmutableList.of(timeType, timeType);
        int precision = timeType.getPrecision();

        ParquetDataSource dataSource = new FileParquetDataSource(
                new File(Resources.getResource("time_millis_int32.snappy.parquet").toURI()),
                new ParquetReaderOptions());
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        ParquetReader reader = createParquetReader(dataSource, parquetMetadata, newSimpleAggregatedMemoryContext(), types, columnNames);

        Page page = reader.nextPage();
        Block block = page.getBlock(0).getLoadedBlock();
        assertThat(block.getPositionCount()).isEqualTo(1);
        // TIME '15:03:00'
        assertThat(timeType.getObjectValue(SESSION, block, 0))
                .isEqualTo(SqlTime.newInstance(precision, 54180000000000000L));

        // TIME '23:59:59.999'
        block = page.getBlock(1).getLoadedBlock();
        assertThat(block.getPositionCount()).isEqualTo(1);
        // Rounded up to 0 if precision < 3
        assertThat(timeType.getObjectValue(SESSION, block, 0))
                .isEqualTo(SqlTime.newInstance(precision, timeType == TIME_SECONDS ? 0L : 86399999000000000L));
    }

    @DataProvider
    public static Object[][] timeTypeProvider()
    {
        return Stream.of(TIME_SECONDS, TIME_MILLIS, TIME_MICROS, TIME_NANOS)
                .collect(toDataProvider());
    }
}
