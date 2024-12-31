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
package io.trino.plugin.paimon;

import io.airlift.slice.Slices;
import io.trino.spi.Page;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.encodeScaledValue;
import static io.trino.spi.type.Decimals.encodeShortScaledValue;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.DateTimes.MICROSECONDS_PER_MILLISECOND;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.paimon.data.Timestamp.fromLocalDateTime;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

final class TestPaimonRow
{
    @Test
    void testPaimonRow()
    {
        Page singlePage = new Page(
                1,
                writeNativeValue(BOOLEAN, null),
                writeNativeValue(BOOLEAN, false),
                writeNativeValue(VARBINARY, Slices.wrappedBuffer((byte) 22)),
                writeNativeValue(SMALLINT, 356L),
                writeNativeValue(INTEGER, 4L),
                writeNativeValue(BIGINT, 23567222L),
                writeNativeValue(REAL, (long) Float.floatToIntBits(1213.33f)),
                writeNativeValue(DOUBLE, 121.3d),
                writeNativeValue(VARCHAR, Slices.wrappedBuffer("rfyu".getBytes(UTF_8))),
                writeNativeValue(createDecimalType(2, 2), encodeShortScaledValue(BigDecimal.valueOf(0.21), 2)),
                writeNativeValue(createDecimalType(38, 2), encodeScaledValue(BigDecimal.valueOf(65782123123.01), 2)),
                writeNativeValue(createDecimalType(10, 1), encodeShortScaledValue(BigDecimal.valueOf(62123123.5), 1)),
                writeNativeValue(TIMESTAMP_MICROS, fromLocalDateTime(LocalDateTime.parse("2007-12-03T10:15:30")).getMillisecond() * MICROSECONDS_PER_MILLISECOND),
                writeNativeValue(VARBINARY, Slices.wrappedBuffer("varbinary_v".getBytes(UTF_8))));

        RowType rowType = RowType.builder().fields(
                DataTypes.BOOLEAN(),
                DataTypes.BOOLEAN(),
                DataTypes.SMALLINT(),
                DataTypes.INT(),
                DataTypes.BIGINT(),
                DataTypes.FLOAT(),
                DataTypes.DOUBLE(),
                DataTypes.STRING(),
                DataTypes.DECIMAL(2, 2),
                DataTypes.DECIMAL(38, 2),
                DataTypes.DECIMAL(10, 1),
                DataTypes.TIMESTAMP(6),
                DataTypes.VARBINARY(Integer.MAX_VALUE)).build();

        PaimonRow paimonRow = new PaimonRow(rowType, singlePage, RowKind.INSERT);

        assertThat(paimonRow.getRowKind()).isEqualTo(RowKind.INSERT);
        assertThat(paimonRow.isNullAt(0)).isEqualTo(true);
        assertThat(paimonRow.getBoolean(1)).isEqualTo(false);
        assertThat(paimonRow.getBinary(2)[0]).isEqualTo((byte) 22);
        assertThat(paimonRow.getShort(3)).isEqualTo((short) 356);
        assertThat(paimonRow.getInt(4)).isEqualTo(4);
        assertThat(paimonRow.getLong(5)).isEqualTo(23567222L);
        assertThat(paimonRow.getFloat(6)).isEqualTo(1213.33f);
        assertThat(paimonRow.getDouble(7)).isEqualTo(121.3d);
        assertThat(paimonRow.getString(8)).isEqualTo(BinaryString.fromString("rfyu"));
        assertThat(paimonRow.getDecimal(9, 2, 2))
                .isEqualTo(Decimal.fromBigDecimal(BigDecimal.valueOf(0.21), 2, 2));
        assertThat(paimonRow.getDecimal(10, 38, 2))
                .isEqualTo(Decimal.fromBigDecimal(BigDecimal.valueOf(65782123123.01), 38, 2));
        assertThat(paimonRow.getDecimal(11, 10, 1))
                .isEqualTo(Decimal.fromBigDecimal(BigDecimal.valueOf(62123123.5), 10, 1));
        assertThat(paimonRow.getTimestamp(12, 6))
                .isEqualTo(fromLocalDateTime(LocalDateTime.parse("2007-12-03T10:15:30")));
        assertThat(paimonRow.getBinary(13))
                .isEqualTo("varbinary_v".getBytes(UTF_8));
    }
}
