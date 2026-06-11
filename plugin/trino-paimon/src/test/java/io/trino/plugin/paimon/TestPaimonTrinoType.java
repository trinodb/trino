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
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.VarCharType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.paimon.PaimonTypeUtils.toPaimonType;
import static io.trino.plugin.paimon.PaimonTypeUtils.toTrinoType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

final class TestPaimonTrinoType
{
    @Test
    void testToTrinoType()
    {
        assertThat(toTrinoType(DataTypes.CHAR(1)).getDisplayName()).isEqualTo("char(1)");
        assertThat(toTrinoType(DataTypes.VARCHAR(10)).getDisplayName()).isEqualTo("varchar(10)");
        assertThat(toTrinoType(DataTypes.BOOLEAN()).getDisplayName()).isEqualTo("boolean");
        assertThat(toTrinoType(DataTypes.BINARY(10)).getDisplayName()).isEqualTo("varbinary");
        assertThat(toTrinoType(DataTypes.VARBINARY(10)).getDisplayName()).isEqualTo("varbinary");
        assertThat(toTrinoType(DataTypes.DECIMAL(38, 0)).getDisplayName()).isEqualTo("decimal(38,0)");
        assertThat(toTrinoType(DataTypes.DECIMAL(2, 2)).getDisplayName()).isEqualTo("decimal(2,2)");
        assertThat(toTrinoType(DataTypes.TINYINT()).getDisplayName()).isEqualTo("tinyint");
        assertThat(toTrinoType(DataTypes.SMALLINT()).getDisplayName()).isEqualTo("smallint");
        assertThat(toTrinoType(DataTypes.INT()).getDisplayName()).isEqualTo("integer");
        assertThat(toTrinoType(DataTypes.BIGINT()).getDisplayName()).isEqualTo("bigint");
        assertThat(toTrinoType(DataTypes.FLOAT()).getDisplayName()).isEqualTo("real");
        assertThat(toTrinoType(DataTypes.DOUBLE()).getDisplayName()).isEqualTo("double");
        assertThat(toTrinoType(DataTypes.DATE()).getDisplayName()).isEqualTo("date");
        assertThat(toTrinoType(new TimeType(0)).getDisplayName()).isEqualTo("time(0)");
        assertThat(toTrinoType(new TimeType(3)).getDisplayName()).isEqualTo("time(3)");
        assertThat(toTrinoType(new TimeType(6)).getDisplayName()).isEqualTo("time(6)");
        assertThat(toTrinoType(new TimeType(9)).getDisplayName()).isEqualTo("time(9)");
        assertThat(toTrinoType(DataTypes.TIMESTAMP()).getDisplayName()).isEqualTo("timestamp(6)");
        assertThat(toTrinoType(new org.apache.paimon.types.TimestampType(3)).getDisplayName()).isEqualTo("timestamp(3)");
        assertThat(toTrinoType(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE()).getDisplayName()).isEqualTo("timestamp(6) with time zone");
        assertThat(toTrinoType(DataTypes.ARRAY(DataTypes.STRING())).getDisplayName()).isEqualTo("array(varchar)");
        assertThat(toTrinoType(DataTypes.MULTISET(DataTypes.STRING())).getDisplayName()).isEqualTo("map(varchar, integer)");
        assertThat(toTrinoType(DataTypes.MAP(DataTypes.BIGINT(), DataTypes.STRING())).getDisplayName()).isEqualTo("map(bigint, varchar)");
        assertThat(toTrinoType(DataTypes.ROW(
                new DataField(0, "id", new IntType()),
                new DataField(1, "name", new VarCharType(Integer.MAX_VALUE)))).getDisplayName())
                .isEqualTo("row(\"id\" integer, \"name\" varchar)");
    }

    @Test
    void testToPaimonType()
    {
        assertThat(toPaimonType(CharType.createCharType(1)).asSQLString()).isEqualTo("CHAR(1)");
        assertThat(toPaimonType(VARCHAR).asSQLString()).isEqualTo("VARCHAR(2147483646)");
        assertThat(toPaimonType(BooleanType.BOOLEAN).asSQLString()).isEqualTo("BOOLEAN");
        assertThat(toPaimonType(VarbinaryType.VARBINARY).asSQLString()).isEqualTo("BYTES");
        assertThat(toPaimonType(DecimalType.createDecimalType(2, 2)).asSQLString()).isEqualTo("DECIMAL(2, 2)");
        assertThat(toPaimonType(TinyintType.TINYINT).asSQLString()).isEqualTo("TINYINT");
        assertThat(toPaimonType(SmallintType.SMALLINT).asSQLString()).isEqualTo("SMALLINT");
        assertThat(toPaimonType(IntegerType.INTEGER).asSQLString()).isEqualTo("INT");
        assertThat(toPaimonType(BigintType.BIGINT).asSQLString()).isEqualTo("BIGINT");
        assertThat(toPaimonType(RealType.REAL).asSQLString()).isEqualTo("FLOAT");
        assertThat(toPaimonType(DoubleType.DOUBLE).asSQLString()).isEqualTo("DOUBLE");
        assertThat(toPaimonType(DateType.DATE).asSQLString()).isEqualTo("DATE");
        assertThat(toPaimonType(io.trino.spi.type.TimeType.TIME_SECONDS).asSQLString()).isEqualTo("TIME(0)");
        assertThat(toPaimonType(io.trino.spi.type.TimeType.TIME_MILLIS).asSQLString()).isEqualTo("TIME(3)");
        assertThat(toPaimonType(TimestampType.TIMESTAMP_SECONDS).asSQLString()).isEqualTo("TIMESTAMP(0)");
        assertThat(toPaimonType(TimestampType.TIMESTAMP_MILLIS).asSQLString()).isEqualTo("TIMESTAMP(3)");
        assertThat(toPaimonType(TimestampType.TIMESTAMP_MICROS).asSQLString()).isEqualTo("TIMESTAMP(6)");
        assertThat(toPaimonType(TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS).asSQLString()).isEqualTo("TIMESTAMP(3) WITH LOCAL TIME ZONE");
        assertThat(toPaimonType(new ArrayType(IntegerType.INTEGER)).asSQLString()).isEqualTo("ARRAY<INT>");
        assertThat(toPaimonType(new MapType(
                IntegerType.INTEGER,
                VARCHAR,
                new TypeOperators())).asSQLString()).isEqualTo("MAP<INT, VARCHAR(2147483646)>");
        List<RowType.Field> fields = new ArrayList<>();
        fields.add(new RowType.Field(Optional.of("id"), IntegerType.INTEGER));
        fields.add(new RowType.Field(Optional.of("name"), VARCHAR));
        Type type = RowType.from(fields);
        DataType rowType = toPaimonType(type);
        assertThat(rowType.asSQLString()).isEqualTo("ROW<`id` INT, `name` VARCHAR(2147483646)>");
    }
}
