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
package io.trino.plugin.hive.util;

import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.Type;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.plugin.hive.util.HiveWriteUtils.createPartitionValues;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.writeBigDecimal;
import static io.trino.spi.type.Decimals.writeShortDecimal;
import static io.trino.spi.type.SqlDecimal.decimal;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveWriteUtils
{
    @Test
    public void testCreatePartitionValuesDecimal()
    {
        assertCreatePartitionValuesDecimal(10, 0, "12345", "12345");
        assertCreatePartitionValuesDecimal(10, 2, "123.45", "123.45");
        assertCreatePartitionValuesDecimal(10, 2, "12345.00", "12345");
        assertCreatePartitionValuesDecimal(5, 0, "12345", "12345");
        assertCreatePartitionValuesDecimal(38, 2, "12345.00", "12345");
        assertCreatePartitionValuesDecimal(38, 20, "12345.00000000000000000000", "12345");
        assertCreatePartitionValuesDecimal(38, 20, "12345.67898000000000000000", "12345.67898");
    }

    private static void assertCreatePartitionValuesDecimal(int precision, int scale, String decimalValue, String expectedValue)
    {
        DecimalType decimalType = createDecimalType(precision, scale);
        List<Type> types = List.of(decimalType);
        SqlDecimal decimal = decimal(decimalValue, decimalType);

        // verify the test values are as expected
        assertThat(decimal.toString()).isEqualTo(decimalValue);
        assertThat(decimal.toBigDecimal().toString()).isEqualTo(decimalValue);

        PageBuilder pageBuilder = new PageBuilder(types);
        pageBuilder.declarePosition();
        writeDecimal(decimalType, decimal, pageBuilder.getBlockBuilder(0));
        Page page = pageBuilder.build();

        // verify the expected value against HiveDecimal
        assertThat(HiveDecimal.create(decimal.toBigDecimal()).toString())
                .isEqualTo(expectedValue);

        assertThat(createPartitionValues(types, page, 0))
                .isEqualTo(List.of(expectedValue));
    }

    private static void writeDecimal(DecimalType decimalType, SqlDecimal decimal, BlockBuilder blockBuilder)
    {
        if (decimalType.isShort()) {
            writeShortDecimal(blockBuilder, decimal.toBigDecimal().unscaledValue().longValue());
        }
        else {
            writeBigDecimal(decimalType, blockBuilder, decimal.toBigDecimal());
        }
    }
}
