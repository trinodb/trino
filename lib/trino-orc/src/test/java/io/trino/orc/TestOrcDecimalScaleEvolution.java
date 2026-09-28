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
import io.trino.spi.block.Block;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.SqlDecimal;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.orc.OrcReader.MAX_BATCH_SIZE;
import static io.trino.orc.OrcTester.HIVE_STORAGE_TIME_ZONE;
import static io.trino.orc.OrcTester.READER_OPTIONS;
import static io.trino.orc.OrcTester.writeOrcColumnTrino;
import static io.trino.orc.metadata.CompressionKind.NONE;
import static io.trino.spi.type.Decimals.readBigDecimal;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOrcDecimalScaleEvolution
{
    private static final DecimalType SHORT_FILE_TYPE = DecimalType.createDecimalType(10, 3);
    private static final DecimalType SHORT_READ_TYPE = DecimalType.createDecimalType(10, 2);
    private static final DecimalType LONG_FILE_TYPE = DecimalType.createDecimalType(20, 3);
    private static final DecimalType LONG_READ_TYPE = DecimalType.createDecimalType(20, 2);

    private static final List<String> VALUES = ImmutableList.of("10.233", "10.235", "-10.235");
    private static final List<BigDecimal> EXPECTED = ImmutableList.of(
            new BigDecimal("10.23"),
            new BigDecimal("10.24"),
            new BigDecimal("-10.24"));

    @Test
    public void testShortDecimalReadWithLowerScale()
            throws Exception
    {
        assertScaleReduction(SHORT_FILE_TYPE, SHORT_READ_TYPE);
    }

    @Test
    public void testLongDecimalReadWithLowerScale()
            throws Exception
    {
        assertScaleReduction(LONG_FILE_TYPE, LONG_READ_TYPE);
    }

    /**
     * A file whose decimal scale is larger than the scale it is read as has to lose the extra
     * digits. Reading {@code 10.233} as {@code decimal(10, 2)} has to yield {@code 10.23},
     * rounding half up, exactly as a cast between the two types does.
     */
    private static void assertScaleReduction(DecimalType fileType, DecimalType readType)
            throws Exception
    {
        try (TempFile tempFile = new TempFile()) {
            writeOrcColumnTrino(
                    tempFile.getFile(),
                    NONE,
                    fileType,
                    VALUES.stream()
                            .map(value -> toSqlDecimal(value, fileType))
                            .iterator(),
                    new OrcWriterStats());

            assertThat(readDecimals(tempFile, readType)).containsExactlyElementsOf(EXPECTED);
        }
    }

    private static SqlDecimal toSqlDecimal(String value, DecimalType type)
    {
        BigDecimal decimal = new BigDecimal(value);
        return new SqlDecimal(
                decimal.unscaledValue().multiply(BigInteger.TEN.pow(type.getScale() - decimal.scale())),
                type.getPrecision(),
                type.getScale());
    }

    private static List<BigDecimal> readDecimals(TempFile tempFile, DecimalType readType)
            throws IOException
    {
        List<BigDecimal> result = new ArrayList<>();
        try (OrcDataSource orcDataSource = new FileOrcDataSource(tempFile.getFile(), READER_OPTIONS)) {
            OrcReader orcReader = OrcReader.createOrcReader(orcDataSource, READER_OPTIONS)
                    .orElseThrow(() -> new RuntimeException("File is empty"));
            try (OrcRecordReader recordReader = orcReader.createRecordReader(
                    orcReader.getRootColumn().getNestedColumns(),
                    ImmutableList.of(readType),
                    false,
                    OrcPredicate.TRUE,
                    HIVE_STORAGE_TIME_ZONE,
                    newSimpleAggregatedMemoryContext(),
                    MAX_BATCH_SIZE,
                    RuntimeException::new)) {
                while (true) {
                    SourcePage page = recordReader.nextPage();
                    if (page == null) {
                        break;
                    }
                    Block block = page.getBlock(0);
                    for (int i = 0; i < block.getPositionCount(); i++) {
                        result.add(readBigDecimal(readType, block, i));
                    }
                }
            }
        }
        return result;
    }
}
