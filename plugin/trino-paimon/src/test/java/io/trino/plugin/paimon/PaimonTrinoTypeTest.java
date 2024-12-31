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

import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.VarCharType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for {@link PaimonTypeUtils}.
 */
public class PaimonTrinoTypeTest
{
    @Test
    public void testFromPaimonType()
    {
        Type charType = PaimonTypeUtils.fromPaimonType(DataTypes.CHAR(1));
        assertThat(requireNonNull(charType).getDisplayName()).isEqualTo("char(1)");

        Type varCharType = PaimonTypeUtils.fromPaimonType(DataTypes.VARCHAR(10));
        assertThat(requireNonNull(varCharType).getDisplayName()).isEqualTo("varchar(10)");

        Type booleanType = PaimonTypeUtils.fromPaimonType(DataTypes.BOOLEAN());
        assertThat(requireNonNull(booleanType).getDisplayName()).isEqualTo("boolean");

        Type binaryType = PaimonTypeUtils.fromPaimonType(DataTypes.BINARY(10));
        assertThat(requireNonNull(binaryType).getDisplayName()).isEqualTo("varbinary");

        Type varBinaryType = PaimonTypeUtils.fromPaimonType(DataTypes.VARBINARY(10));
        assertThat(requireNonNull(varBinaryType).getDisplayName()).isEqualTo("varbinary");

        assertThat(PaimonTypeUtils.fromPaimonType(DataTypes.DECIMAL(38, 0)).getDisplayName())
                .isEqualTo("decimal(38,0)");

        org.apache.paimon.types.DecimalType decimal = DataTypes.DECIMAL(2, 2);
        assertThat(PaimonTypeUtils.fromPaimonType(decimal).getDisplayName())
                .isEqualTo("decimal(2,2)");

        Type tinyIntType = PaimonTypeUtils.fromPaimonType(DataTypes.TINYINT());
        assertThat(requireNonNull(tinyIntType).getDisplayName()).isEqualTo("tinyint");

        Type smallIntType = PaimonTypeUtils.fromPaimonType(DataTypes.SMALLINT());
        assertThat(requireNonNull(smallIntType).getDisplayName()).isEqualTo("smallint");

        Type intType = PaimonTypeUtils.fromPaimonType(DataTypes.INT());
        assertThat(requireNonNull(intType).getDisplayName()).isEqualTo("integer");

        Type bigIntType = PaimonTypeUtils.fromPaimonType(DataTypes.BIGINT());
        assertThat(requireNonNull(bigIntType).getDisplayName()).isEqualTo("bigint");

        Type floatType = PaimonTypeUtils.fromPaimonType(DataTypes.FLOAT());
        assertThat(requireNonNull(floatType).getDisplayName()).isEqualTo("real");

        Type doubleType = PaimonTypeUtils.fromPaimonType(DataTypes.DOUBLE());
        assertThat(requireNonNull(doubleType).getDisplayName()).isEqualTo("double");

        Type dateType = PaimonTypeUtils.fromPaimonType(DataTypes.DATE());
        assertThat(requireNonNull(dateType).getDisplayName()).isEqualTo("date");

        Type timeType = PaimonTypeUtils.fromPaimonType(new TimeType());
        assertThat(requireNonNull(timeType).getDisplayName()).isEqualTo("time(3)");

        Type timestampType6 = PaimonTypeUtils.fromPaimonType(DataTypes.TIMESTAMP());
        assertThat(requireNonNull(timestampType6).getDisplayName())
                .isEqualTo("timestamp(6)");

        Type timestampType0 =
                PaimonTypeUtils.fromPaimonType(new org.apache.paimon.types.TimestampType(3));
        assertThat(requireNonNull(timestampType0).getDisplayName())
                .isEqualTo("timestamp(3)");

        Type localZonedTimestampType =
                PaimonTypeUtils.fromPaimonType(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
        assertThat(requireNonNull(localZonedTimestampType).getDisplayName())
                .isEqualTo("timestamp(6) with time zone");

        Type arrayType = PaimonTypeUtils.fromPaimonType(DataTypes.ARRAY(DataTypes.STRING()));
        assertThat(requireNonNull(arrayType).getDisplayName()).isEqualTo("array(varchar)");

        Type multisetType = PaimonTypeUtils.fromPaimonType(DataTypes.MULTISET(DataTypes.STRING()));
        assertThat(requireNonNull(multisetType).getDisplayName())
                .isEqualTo("map(varchar, integer)");

        Type mapType =
                PaimonTypeUtils.fromPaimonType(
                        DataTypes.MAP(DataTypes.BIGINT(), DataTypes.STRING()));
        assertThat(requireNonNull(mapType).getDisplayName())
                .isEqualTo("map(bigint, varchar)");

        Type row =
                PaimonTypeUtils.fromPaimonType(
                        DataTypes.ROW(
                                new DataField(0, "id", new IntType()),
                                new DataField(1, "name", new VarCharType(Integer.MAX_VALUE))));
        assertThat(requireNonNull(row).getDisplayName())
                .isEqualTo("row(id integer, name varchar)");
    }

    @Test
    public void testToPaimonType()
    {
        DataType charType = PaimonTypeUtils.toPaimonType(CharType.createCharType(1));
        assertThat(charType.asSQLString()).isEqualTo("CHAR(1)");

        DataType varCharType =
                PaimonTypeUtils.toPaimonType(VarcharType.createUnboundedVarcharType());
        assertThat(varCharType.asSQLString()).isEqualTo("VARCHAR(2147483646)");

        DataType booleanType = PaimonTypeUtils.toPaimonType(BooleanType.BOOLEAN);
        assertThat(booleanType.asSQLString()).isEqualTo("BOOLEAN");

        DataType varbinaryType = PaimonTypeUtils.toPaimonType(VarbinaryType.VARBINARY);
        assertThat(varbinaryType.asSQLString()).isEqualTo("BYTES");

        DataType decimalType = PaimonTypeUtils.toPaimonType(DecimalType.createDecimalType(2, 2));
        assertThat(decimalType.asSQLString()).isEqualTo("DECIMAL(2, 2)");

        DataType tinyintType = PaimonTypeUtils.toPaimonType(TinyintType.TINYINT);
        assertThat(tinyintType.asSQLString()).isEqualTo("TINYINT");

        DataType smallintType = PaimonTypeUtils.toPaimonType(SmallintType.SMALLINT);
        assertThat(smallintType.asSQLString()).isEqualTo("SMALLINT");

        DataType intType = PaimonTypeUtils.toPaimonType(IntegerType.INTEGER);
        assertThat(intType.asSQLString()).isEqualTo("INT");

        DataType bigintType = PaimonTypeUtils.toPaimonType(BigintType.BIGINT);
        assertThat(bigintType.asSQLString()).isEqualTo("BIGINT");

        DataType floatType = PaimonTypeUtils.toPaimonType(RealType.REAL);
        assertThat(floatType.asSQLString()).isEqualTo("FLOAT");

        DataType doubleType = PaimonTypeUtils.toPaimonType(DoubleType.DOUBLE);
        assertThat(doubleType.asSQLString()).isEqualTo("DOUBLE");

        DataType dateType = PaimonTypeUtils.toPaimonType(DateType.DATE);
        assertThat(dateType.asSQLString()).isEqualTo("DATE");

        DataType timeType = PaimonTypeUtils.toPaimonType(io.trino.spi.type.TimeType.TIME_MILLIS);
        assertThat(timeType.asSQLString()).isEqualTo("TIME(0)");

        DataType timestampType0 = PaimonTypeUtils.toPaimonType(TimestampType.TIMESTAMP_SECONDS);
        assertThat(timestampType0.asSQLString()).isEqualTo("TIMESTAMP(0)");

        DataType timestampType3 = PaimonTypeUtils.toPaimonType(TimestampType.TIMESTAMP_MILLIS);
        assertThat(timestampType3.asSQLString()).isEqualTo("TIMESTAMP(3)");

        DataType timestampType6 = PaimonTypeUtils.toPaimonType(TimestampType.TIMESTAMP_MICROS);
        assertThat(timestampType6.asSQLString()).isEqualTo("TIMESTAMP(6)");

        DataType timestampWithTimeZoneType =
                PaimonTypeUtils.toPaimonType(TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS);
        assertThat(timestampWithTimeZoneType.asSQLString())
                .isEqualTo("TIMESTAMP(6) WITH LOCAL TIME ZONE");

        DataType arrayType = PaimonTypeUtils.toPaimonType(new ArrayType(IntegerType.INTEGER));
        assertThat(arrayType.asSQLString()).isEqualTo("ARRAY<INT>");

        DataType mapType =
                PaimonTypeUtils.toPaimonType(
                        new MapType(
                                IntegerType.INTEGER,
                                VarcharType.createUnboundedVarcharType(),
                                new TypeOperators()));
        assertThat(mapType.asSQLString()).isEqualTo("MAP<INT, VARCHAR(2147483646)>");

        List<RowType.Field> fields = new ArrayList<>();
        fields.add(new RowType.Field(java.util.Optional.of("id"), IntegerType.INTEGER));
        fields.add(
                new RowType.Field(
                        java.util.Optional.of("name"), VarcharType.createUnboundedVarcharType()));
        Type type = RowType.from(fields);
        DataType rowType = PaimonTypeUtils.toPaimonType(type);
        assertThat(rowType.asSQLString()).isEqualTo("ROW<`id` INT, `name` VARCHAR(2147483646)>");
    }
}
