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
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeDescriptor;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VarCharType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_PICOS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PaimonTypeTest
{
    private static final Type JSON_TYPE = TESTING_TYPE_MANAGER.getType(new TypeDescriptor(JSON));

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

        Type blobType = PaimonTypeUtils.fromPaimonType(DataTypes.BLOB());
        assertThat(requireNonNull(blobType).getDisplayName()).isEqualTo("varbinary");

        Type variantType = PaimonTypeUtils.fromPaimonType(DataTypes.VARIANT(), TESTING_TYPE_MANAGER);
        assertThat(requireNonNull(variantType).getDisplayName()).isEqualTo("json");

        assertThat(PaimonTypeUtils.fromPaimonType(DataTypes.DECIMAL(38, 0)).getDisplayName()).isEqualTo("decimal(38,0)");

        org.apache.paimon.types.DecimalType decimal = DataTypes.DECIMAL(2, 2);
        assertThat(PaimonTypeUtils.fromPaimonType(decimal).getDisplayName()).isEqualTo("decimal(2,2)");

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
        assertThat(requireNonNull(timeType).getDisplayName()).isEqualTo("time(0)");

        Type timeType6 = PaimonTypeUtils.fromPaimonType(new TimeType(6));
        assertThat(requireNonNull(timeType6).getDisplayName()).isEqualTo("time(3)");

        Type timeType9 = PaimonTypeUtils.fromPaimonType(new TimeType(9));
        assertThat(requireNonNull(timeType9).getDisplayName()).isEqualTo("time(3)");

        Type timestampType6 = PaimonTypeUtils.fromPaimonType(DataTypes.TIMESTAMP());
        assertThat(requireNonNull(timestampType6).getDisplayName()).isEqualTo("timestamp(6)");

        Type timestampType0 = PaimonTypeUtils.fromPaimonType(new TimestampType(0));
        assertThat(requireNonNull(timestampType0).getDisplayName()).isEqualTo("timestamp(0)");

        Type timestampType2 = PaimonTypeUtils.fromPaimonType(new TimestampType(2));
        assertThat(requireNonNull(timestampType2).getDisplayName()).isEqualTo("timestamp(2)");

        Type timestampType3 = PaimonTypeUtils.fromPaimonType(new TimestampType(3));
        assertThat(requireNonNull(timestampType3).getDisplayName()).isEqualTo("timestamp(3)");

        Type timestampType9 = PaimonTypeUtils.fromPaimonType(new TimestampType(9));
        assertThat(requireNonNull(timestampType9).getDisplayName()).isEqualTo("timestamp(9)");

        Type localZonedTimestampType = PaimonTypeUtils.fromPaimonType(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
        assertThat(requireNonNull(localZonedTimestampType).getDisplayName()).isEqualTo("timestamp(6) with time zone");

        Type localZonedTimestampType1 = PaimonTypeUtils.fromPaimonType(
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(1));
        assertThat(requireNonNull(localZonedTimestampType1).getDisplayName()).isEqualTo("timestamp(1) with time zone");

        Type localZonedTimestampType2 = PaimonTypeUtils.fromPaimonType(
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(2));
        assertThat(requireNonNull(localZonedTimestampType2).getDisplayName()).isEqualTo("timestamp(2) with time zone");

        Type localZonedTimestampType9 = PaimonTypeUtils.fromPaimonType(
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(9));
        assertThat(requireNonNull(localZonedTimestampType9).getDisplayName()).isEqualTo("timestamp(9) with time zone");

        Type arrayType = PaimonTypeUtils.fromPaimonType(DataTypes.ARRAY(DataTypes.STRING()));
        assertThat(requireNonNull(arrayType).getDisplayName()).isEqualTo("array(varchar)");

        Type vectorType = PaimonTypeUtils.fromPaimonType(DataTypes.VECTOR(3, DataTypes.FLOAT()));
        assertThat(requireNonNull(vectorType).getDisplayName()).isEqualTo("array(real)");

        Type multisetType = PaimonTypeUtils.fromPaimonType(DataTypes.MULTISET(DataTypes.STRING()));
        assertThat(requireNonNull(multisetType).getDisplayName()).isEqualTo("map(varchar, integer)");

        Type mapType = PaimonTypeUtils.fromPaimonType(DataTypes.MAP(DataTypes.BIGINT(), DataTypes.STRING()));
        assertThat(requireNonNull(mapType).getDisplayName()).isEqualTo("map(bigint, varchar)");

        Type row = PaimonTypeUtils.fromPaimonType(DataTypes.ROW(
                new DataField(0, "id", new IntType()),
                new DataField(1, "name", new VarCharType(Integer.MAX_VALUE))));
        assertThat(requireNonNull(row).getDisplayName()).isEqualTo("row(\"id\" integer, \"name\" varchar)");
    }

    @Test
    public void testToPaimonType()
    {
        DataType charType = PaimonTypeUtils.toPaimonType(CharType.createCharType(1));
        assertThat(charType.asSQLString()).isEqualTo("CHAR(1)");

        DataType varCharType = PaimonTypeUtils.toPaimonType(VarcharType.createUnboundedVarcharType());
        assertThat(varCharType.asSQLString()).isEqualTo("STRING");
        assertThat(PaimonTypeUtils.fromPaimonType(varCharType)).isEqualTo(VarcharType.createUnboundedVarcharType());

        DataType booleanType = PaimonTypeUtils.toPaimonType(BooleanType.BOOLEAN);
        assertThat(booleanType.asSQLString()).isEqualTo("BOOLEAN");

        DataType varbinaryType = PaimonTypeUtils.toPaimonType(VarbinaryType.VARBINARY);
        assertThat(varbinaryType.asSQLString()).isEqualTo("BYTES");

        DataType variantType = PaimonTypeUtils.toPaimonType(TESTING_TYPE_MANAGER.getType(new TypeDescriptor(JSON)));
        assertThat(variantType.asSQLString()).isEqualTo("VARIANT");

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
        assertThat(timeType.asSQLString()).isEqualTo("TIME(3)");

        DataType timestampType0 = PaimonTypeUtils.toPaimonType(TIMESTAMP_SECONDS);
        assertThat(timestampType0.asSQLString()).isEqualTo("TIMESTAMP(0)");

        DataType timestampType3 = PaimonTypeUtils.toPaimonType(TIMESTAMP_MILLIS);
        assertThat(timestampType3.asSQLString()).isEqualTo("TIMESTAMP(3)");

        DataType timestampType6 = PaimonTypeUtils.toPaimonType(TIMESTAMP_MICROS);
        assertThat(timestampType6.asSQLString()).isEqualTo("TIMESTAMP(6)");

        DataType timestampType9 = PaimonTypeUtils.toPaimonType(TIMESTAMP_NANOS);
        assertThat(timestampType9.asSQLString()).isEqualTo("TIMESTAMP(9)");

        DataType timestampWithTimeZoneType = PaimonTypeUtils.toPaimonType(TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS);
        assertThat(timestampWithTimeZoneType.asSQLString()).isEqualTo("TIMESTAMP(3) WITH LOCAL TIME ZONE");

        DataType timestampWithTimeZoneType9 = PaimonTypeUtils.toPaimonType(
                TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS);
        assertThat(timestampWithTimeZoneType9.asSQLString()).isEqualTo("TIMESTAMP(9) WITH LOCAL TIME ZONE");

        DataType arrayType = PaimonTypeUtils.toPaimonType(new ArrayType(IntegerType.INTEGER));
        assertThat(arrayType.asSQLString()).isEqualTo("ARRAY<INT>");

        DataType mapType = PaimonTypeUtils.toPaimonType(
                new MapType(IntegerType.INTEGER, VarcharType.createUnboundedVarcharType(), new TypeOperators()));
        assertThat(mapType.asSQLString()).isEqualTo("MAP<INT, STRING>");

        List<RowType.Field> fields = new ArrayList<>();
        fields.add(new RowType.Field(Optional.of("id"), IntegerType.INTEGER));
        fields.add(new RowType.Field(Optional.of("name"), VarcharType.createUnboundedVarcharType()));
        Type type = RowType.from(fields);
        DataType rowType = PaimonTypeUtils.toPaimonType(type);
        assertThat(rowType.asSQLString()).isEqualTo("ROW<`id` INT, `name` STRING>");
        assertThat(PaimonTypeUtils.fromPaimonType(rowType)).isEqualTo(type);

        DataType nextRowType = PaimonTypeUtils.toPaimonType(type);
        assertThat(nextRowType).isEqualTo(rowType);

        DataType anonymousRowType = PaimonTypeUtils.toPaimonType(RowType.anonymous(List.of(
                IntegerType.INTEGER,
                VarcharType.createUnboundedVarcharType())));
        assertThat(anonymousRowType.asSQLString()).isEqualTo("ROW<`f0` INT, `f1` STRING>");
    }

    @Test
    public void testToPaimonTypeRejectsUnsupportedTemporalPrecision()
    {
        assertThatThrownBy(() -> PaimonTypeUtils.toPaimonType(io.trino.spi.type.TimeType.TIME_MICROS))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Paimon stores time values with millisecond precision, got time(6)");
        assertThatThrownBy(() -> PaimonTypeUtils.toPaimonType(io.trino.spi.type.TimeType.TIME_PICOS))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Paimon stores time values with millisecond precision, got time(12)");
        assertThatThrownBy(() -> PaimonTypeUtils.toPaimonType(TIMESTAMP_PICOS))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Paimon supports timestamp precision up to 9, got timestamp(12)");
        assertThatThrownBy(() -> PaimonTypeUtils.toPaimonType(TimestampWithTimeZoneType.TIMESTAMP_TZ_PICOS))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Paimon supports timestamp with time zone precision up to 9, got timestamp(12) with time zone");
        assertThatThrownBy(() -> PaimonTypeUtils.toPaimonType(RowType.from(List.of(
                RowType.field("event_time", io.trino.spi.type.TimeType.TIME_MICROS)))))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Paimon stores time values with millisecond precision, got time(6)");
        assertThatThrownBy(() -> PaimonTypeUtils.toPaimonType(RowType.from(List.of(
                RowType.field("event_time", TIMESTAMP_PICOS)))))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Paimon supports timestamp precision up to 9, got timestamp(12)");
    }

    @Test
    public void testToPaimonTypeRejectsUnsupportedStringLength()
    {
        assertThatThrownBy(() -> PaimonTypeUtils.toPaimonType(CharType.createCharType(0)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Paimon supports char length between 1 and 2147483647, got char(0)");
        assertThatThrownBy(() -> PaimonTypeUtils.toPaimonType(VarcharType.createVarcharType(0)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Paimon supports varchar length between 1 and 2147483647, got varchar(0)");
        assertThatThrownBy(() -> PaimonTypeUtils.toPaimonType(RowType.from(List.of(
                RowType.field("code", CharType.createCharType(0))))))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Paimon supports char length between 1 and 2147483647, got char(0)");
    }

    @Test
    public void testFromPaimonTypeRejectsCharLengthUnsupportedByTrino()
    {
        assertThatThrownBy(() -> PaimonTypeUtils.fromPaimonType(
                new org.apache.paimon.types.CharType(CharType.MAX_LENGTH + 1)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Trino supports char length up to 65536, got Paimon char(65537)");
    }

    @Test
    public void testNestedRowFieldIdsAreGloballyUnique()
    {
        Type rowType = RowType.from(List.of(
                RowType.field("id", IntegerType.INTEGER),
                RowType.field("payload", RowType.from(List.of(
                        RowType.field("name", VarcharType.createUnboundedVarcharType()),
                        RowType.field("attributes", RowType.from(List.of(
                                RowType.field("score", DoubleType.DOUBLE),
                                RowType.field("tags", new ArrayType(VarcharType.createUnboundedVarcharType()))))))))));

        org.apache.paimon.types.RowType paimonRowType = (org.apache.paimon.types.RowType) PaimonTypeUtils.toPaimonType(rowType);

        assertThat(paimonRowType.getFields()).extracting(DataField::id)
                .containsExactly(0, 1);
        org.apache.paimon.types.RowType payloadType = (org.apache.paimon.types.RowType) paimonRowType.getFields().get(1).type();
        assertThat(payloadType.getFields()).extracting(DataField::id)
                .containsExactly(2, 3);
        org.apache.paimon.types.RowType attributesType = (org.apache.paimon.types.RowType) payloadType.getFields().get(1).type();
        assertThat(attributesType.getFields()).extracting(DataField::id)
                .containsExactly(4, 5);

        Set<Integer> fieldIds = new HashSet<>();
        paimonRowType.collectFieldIds(fieldIds);
        assertThat(fieldIds).containsExactlyInAnyOrder(0, 1, 2, 3, 4, 5);
    }

    @Test
    public void testNestedVariantRequiresTypeManager()
    {
        assertThatThrownBy(() -> PaimonTypeUtils.fromPaimonType(DataTypes.ARRAY(DataTypes.VARIANT())))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Paimon VARIANT requires TypeManager for Trino JSON type");
        assertThatThrownBy(() -> PaimonTypeUtils.fromPaimonType(DataTypes.MULTISET(DataTypes.VARIANT())))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Paimon VARIANT requires TypeManager for Trino JSON type");
    }

    @Test
    public void testNestedVariantFromPaimonType()
    {
        assertThat(PaimonTypeUtils.fromPaimonType(DataTypes.ARRAY(DataTypes.VARIANT()), TESTING_TYPE_MANAGER))
                .isEqualTo(new ArrayType(JSON_TYPE));
        assertThat(PaimonTypeUtils.fromPaimonType(
                DataTypes.MAP(DataTypes.STRING(), DataTypes.VARIANT()), TESTING_TYPE_MANAGER))
                .isEqualTo(new MapType(VARCHAR, JSON_TYPE, new TypeOperators()));
        assertThat(PaimonTypeUtils.fromPaimonType(
                DataTypes.ROW(DataTypes.FIELD(0, "payload", DataTypes.VARIANT())), TESTING_TYPE_MANAGER))
                .isEqualTo(RowType.from(List.of(RowType.field("payload", JSON_TYPE))));
        assertThat(PaimonTypeUtils.fromPaimonType(DataTypes.MULTISET(DataTypes.VARIANT()), TESTING_TYPE_MANAGER))
                .isEqualTo(new MapType(JSON_TYPE, IntegerType.INTEGER, new TypeOperators()));
    }

    @Test
    public void testNestedJsonToPaimonVariant()
    {
        assertThat(PaimonTypeUtils.toPaimonType(new ArrayType(JSON_TYPE)).asSQLString())
                .isEqualTo("ARRAY<VARIANT>");
        assertThat(PaimonTypeUtils.toPaimonType(new MapType(IntegerType.INTEGER, JSON_TYPE, new TypeOperators()))
                .asSQLString())
                .isEqualTo("MAP<INT, VARIANT>");
        assertThat(PaimonTypeUtils.toPaimonType(RowType.from(List.of(RowType.field("payload", JSON_TYPE))))
                .asSQLString())
                .isEqualTo("ROW<`payload` VARIANT>");
        assertThat(PaimonTypeUtils.toPaimonType(new MapType(JSON_TYPE, IntegerType.INTEGER, new TypeOperators()))
                .asSQLString())
                .isEqualTo("MAP<VARIANT, INT>");
    }

    @Test
    public void testTypeMappingRejectsNullInputs()
    {
        assertThatThrownBy(() -> PaimonTypeUtils.fromPaimonType(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("type is null");
        assertThatThrownBy(() -> PaimonTypeUtils.fromPaimonType(null, TESTING_TYPE_MANAGER))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("type is null");
        assertThatThrownBy(() -> PaimonTypeUtils.fromPaimonType(DataTypes.INT(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("typeManager is null");
        assertThatThrownBy(() -> PaimonTypeUtils.toPaimonType(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("trinoType is null");
    }
}
