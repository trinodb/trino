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
package io.trino.server.dataframe;

import com.starburstdata.dataframe.type.AbstractDataType;
import com.starburstdata.dataframe.type.BinaryType;
import com.starburstdata.dataframe.type.ByteType;
import com.starburstdata.dataframe.type.DayTimeIntervalType;
import com.starburstdata.dataframe.type.FloatType;
import com.starburstdata.dataframe.type.LongType;
import com.starburstdata.dataframe.type.NullType;
import com.starburstdata.dataframe.type.ShortType;
import com.starburstdata.dataframe.type.StringType;
import com.starburstdata.dataframe.type.StructType;
import com.starburstdata.dataframe.type.TimestampNTZType;
import com.starburstdata.dataframe.type.YearMonthIntervalType;
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
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import io.trino.type.IntervalDayTimeType;
import io.trino.type.IntervalYearMonthType;
import io.trino.type.JsonType;
import io.trino.type.UnknownType;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class DataTypeMapper
{
    public AbstractDataType fromTrinoType(Type type)
    {
        if (type instanceof UnknownType) {
            return new NullType();
        }
        if (type instanceof BooleanType) {
            return new com.starburstdata.dataframe.type.BooleanType();
        }
        if (type instanceof CharType charType) {
            return new com.starburstdata.dataframe.type.CharType(Optional.of(charType.getLength()));
        }
        if (type instanceof VarcharType varcharType) {
            return new StringType(varcharType.getLength());
        }
        if (type instanceof VarbinaryType) {
            return new BinaryType();
        }
        if (type instanceof TinyintType) {
            return new ByteType();
        }
        if (type instanceof IntegerType) {
            return new com.starburstdata.dataframe.type.IntegerType();
        }
        if (type instanceof JsonType) {
            return new com.starburstdata.dataframe.type.JsonType();
        }
        if (type instanceof SmallintType) {
            return new ShortType();
        }
        if (type instanceof BigintType) {
            return new LongType();
        }
        if (type instanceof RealType) {
            return new FloatType();
        }
        if (type instanceof DoubleType) {
            return new com.starburstdata.dataframe.type.DoubleType();
        }
        if (type instanceof DecimalType decimalType) {
            return new com.starburstdata.dataframe.type.DecimalType(decimalType.getPrecision(), decimalType.getScale());
        }
        if (type instanceof DateType) {
            return new com.starburstdata.dataframe.type.DateType();
        }
        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            return new com.starburstdata.dataframe.type.TimestampType(Optional.of(timestampWithTimeZoneType.getPrecision()));
        }
        if (type instanceof TimestampType timestampType) {
            return new TimestampNTZType(Optional.of(timestampType.getPrecision()));
        }
        if (type instanceof TimeWithTimeZoneType timeWithTimeZoneType) {
            return new com.starburstdata.dataframe.type.TimeType(Optional.of(timeWithTimeZoneType.getPrecision()));
        }
        if (type instanceof TimeType timeType) {
            return new com.starburstdata.dataframe.type.TimeNTZType(Optional.of(timeType.getPrecision()));
        }
        if (type instanceof IntervalDayTimeType) {
            return new DayTimeIntervalType();
        }
        if (type instanceof IntervalYearMonthType) {
            return new YearMonthIntervalType();
        }
        if (type instanceof MapType mapType) {
            AbstractDataType keyType = fromTrinoType(mapType.getKeyType());
            AbstractDataType valueType = fromTrinoType(mapType.getValueType());
            return new com.starburstdata.dataframe.type.MapType(keyType, valueType);
        }
        if (type instanceof ArrayType arrayType) {
            AbstractDataType elementType = fromTrinoType(arrayType.getElementType());
            return new com.starburstdata.dataframe.type.ArrayType(elementType);
        }
        if (type instanceof RowType rowType) {
            List<StructType.StructField> fields = rowType.getFields().stream()
                    .map(field -> new StructType.StructField(field.getName().map(StructType.ColumnIdentifier::new), fromTrinoType(field.getType()), true)).collect(toImmutableList());
            return new StructType(fields);
        }
        if (type instanceof UuidType) {
            return new com.starburstdata.dataframe.type.UuidType();
        }
        throw new IllegalArgumentException("Can't map type: " + type);
    }
}
