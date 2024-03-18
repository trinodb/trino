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
package io.trino.plugin.varada.type;

import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.FixedWidthType;
import io.trino.spi.type.HyperLogLogType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.util.Set;

import static io.trino.plugin.varada.VaradaErrorCode.VARADA_CONTROL;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimeType.TIME_SECONDS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_SECONDS;
import static io.trino.spi.type.TinyintType.TINYINT;

public class TypeUtils
{
    private static final Set<Type> LONG_TYPES = Set.of(BIGINT, TIME_SECONDS, TIME_MILLIS, TIME_MICROS, TIMESTAMP_SECONDS, TIMESTAMP_MILLIS, TIMESTAMP_MICROS, TIMESTAMP_TZ_SECONDS, TIMESTAMP_TZ_MILLIS, TIMESTAMP_TZ_MICROS);

    private TypeUtils() {}

    // functions for groups of types
    public static boolean isStrType(Type type)
    {
        return isCharType(type) || isVarcharType(type) || isArrayType(type);
    }

    public static boolean isWarmBasicSupported(Type type)
    {
        if (type == null) {
            return false;
        }

        if (isWarmTimeOrTimestampNotSupported(type)) {
            return false;
        }

        return !TypeUtils.isRowType(type) && !TypeUtils.isArrayType(type) && !TypeUtils.isJsonType(type) && !TypeUtils.isVarbinaryType(type) && !TypeUtils.isHyperLogLogType(type);
    }

    public static boolean isWarmDataSupported(Type type)
    {
        if (type == null) {
            return false;
        }

        if (isWarmTimeOrTimestampNotSupported(type)) {
            return false;
        }

        if (TypeUtils.isRowType(type) || TypeUtils.isMapType(type)) {
            return false;
        }
        if (TypeUtils.isArrayType(type)) {
            ArrayType arrayType = (ArrayType) type;
            return !TypeUtils.isRowType(arrayType.getElementType());
        }
        return true;
    }

    public static boolean isWarmLuceneSupported(Type type)
    {
        boolean ret = isCharType(type) || isVarcharType(type);

        if (!ret && isArrayType(type)) {
            Type elementType = ((ArrayType) type).getElementType();
            ret = isCharType(elementType) || isVarcharType(elementType);
        }
        return ret;
    }

    public static boolean isLongTimestampType(Type type)
    {
        return (type instanceof TimestampType timestampType) && !timestampType.isShort();
    }

    public static boolean isLongTimestampTypeWithTimeZoneType(Type type)
    {
        return (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) && !timestampWithTimeZoneType.isShort();
    }

    private static boolean isWarmTimeOrTimestampNotSupported(Type type)
    {
        return isLongTimestampType(type) || isLongTimestampTypeWithTimeZoneType(type) || isLongTimeWithTimeZoneType(type);
    }

    public static boolean isLongTimeWithTimeZoneType(Type type)
    {
        return (type instanceof TimeWithTimeZoneType timeWithTimeZoneType) && !timeWithTimeZoneType.isShort();
    }

    public static boolean isWarmBloomSupported(Type type)
    {
        return isWarmBasicSupported(type);
    }

    public static boolean isLongType(Type type)
    {
        return LONG_TYPES.contains(type);
    }

    public static boolean isIntType(Type type)
    {
        return type.equals(DATE) || type.equals(INTEGER);
    }

    // functions for specific types
    public static boolean isIntegerType(Type type)
    {
        return type.equals(INTEGER);
    }

    public static boolean isDateType(Type type)
    {
        return type.equals(DATE);
    }

    public static boolean isRealType(Type type)
    {
        return type.equals(REAL);
    }

    public static boolean isBigIntegerType(Type type)
    {
        return type.equals(BIGINT);
    }

    public static boolean isTimeType(Type type)
    {
        return type instanceof TimeType || type instanceof TimeWithTimeZoneType;
    }

    public static boolean isTimestampType(Type type)
    {
        return type instanceof TimestampWithTimeZoneType || type instanceof TimestampType;
    }

    public static boolean isBooleanType(Type type)
    {
        return type.equals(BOOLEAN);
    }

    public static boolean isCharType(Type type)
    {
        return (type instanceof CharType);
    }

    public static boolean isVarcharType(Type type)
    {
        return (type instanceof VarcharType);
    }

    public static boolean isVarbinaryType(Type type)
    {
        return (type instanceof VarbinaryType);
    }

    public static boolean isHyperLogLogType(Type type)
    {
        return (type instanceof HyperLogLogType);
    }

    public static boolean isArrayType(Type type)
    {
        return (type instanceof ArrayType);
    }

    public static boolean isMapType(Type type)
    {
        return (type instanceof MapType);
    }

    public static boolean isJsonType(Type type)
    {
        return type.getTypeSignature().getBase().equalsIgnoreCase("json");
    }

    public static boolean isRowType(Type type)
    {
        return (type instanceof RowType);
    }

    public static boolean isLongDecimalType(Type type)
    {
        return (type instanceof DecimalType) && !((DecimalType) type).isShort();
    }

    public static boolean isShortDecimalType(Type type)
    {
        return (type instanceof DecimalType) && ((DecimalType) type).isShort();
    }

    public static boolean isDecimalType(Type type)
    {
        return isLongDecimalType(type) || isShortDecimalType(type);
    }

    public static boolean isSmallIntType(Type type)
    {
        return type.equals(SMALLINT);
    }

    public static boolean isTinyIntType(Type type)
    {
        return type.equals(TINYINT);
    }

    public static boolean isDoubleType(Type type)
    {
        return type.equals(DOUBLE);
    }

    public static boolean isStr(RecTypeCode code)
    {
        // the assumption that all string types are consecutive exists in native engine as well
        // NOTE that arrays are also considered as strings here since they are held as varchar in storage engine
        return (code.ordinal() >= RecTypeCode.REC_TYPE_CHAR.ordinal());
    }

    public static boolean isVarlenStr(RecTypeCode code)
    {
        // the assumption that all string types are consecutive exists in native engine as well
        return (code.ordinal() >= RecTypeCode.REC_TYPE_VARCHAR.ordinal());
    }

    public static int nativeRecTypeCode(RecTypeCode recTypeCode)
    {
        // native supports arrays as varchar, here we switch the code for native in case of arrays
        return isVarlenStr(recTypeCode) ? RecTypeCode.REC_TYPE_VARCHAR.ordinal() : recTypeCode.ordinal();
    }

    public static int getTypeLength(Type type, int maxVarlenLength)
    {
        int ret;
        if (type instanceof CharType) {
            return ((CharType) type).getLength();
        }
        else if (type instanceof VarcharType) {
            if (((VarcharType) type).isUnbounded()) {
                ret = 0;
            }
            else {
                ret = ((VarcharType) type).getBoundedLength();
            }
        }
        else if (type instanceof FixedWidthType) {
            return ((FixedWidthType) type).getFixedSize();
        }
        else if (type instanceof ArrayType) {
            ret = maxVarlenLength;
        }
        else if (type instanceof MapType mapType) {
            return getTypeLength(mapType.getValueType(), maxVarlenLength);
        }
        else {
            throw new TrinoException(VARADA_CONTROL, "Unsupported record type " + type.toString());
        }
        return ret != 0 ? ret : maxVarlenLength;
    }

    public static int getIndexTypeLength(Type type, int strIndexLength)
    {
        int recTypeLength = getTypeLength(type, strIndexLength);

        return getIndexTypeLength(convertToRecTypeCode(type, recTypeLength, strIndexLength), recTypeLength, strIndexLength);
    }

    public static int getIndexTypeLength(RecTypeCode recTypeCode, int recTypeLength, int strIndexLength)
    {
        return (isStr(recTypeCode) && !isStrOptimizedLength(recTypeLength)) ? strIndexLength : recTypeLength;
    }

    private static boolean isStrOptimizedLength(int length)
    {
        return (length == 1) || (length == 2) || (length == 4);
    }

    public static RecTypeCode convertToRecTypeCode(Type type, int recTypeLength, int fixedLengthStringLimit)
    {
        RecTypeCode ret;
        if (isShortDecimalType(type)) {
            ret = RecTypeCode.REC_TYPE_DECIMAL_SHORT;
        }
        else if (isLongDecimalType(type)) {
            int length = ((FixedWidthType) type).getFixedSize();
            if (length == 8) {
                ret = RecTypeCode.REC_TYPE_DECIMAL_SHORT;
            }
            else {
                ret = RecTypeCode.REC_TYPE_DECIMAL_LONG;
            }
        }
        else if (isBooleanType(type)) {
            ret = RecTypeCode.REC_TYPE_BOOLEAN;
        }
        else if (isIntegerType(type)) {
            ret = RecTypeCode.REC_TYPE_INTEGER;
        }
        else if (isTimestampType(type)) {
            ret = RecTypeCode.REC_TYPE_TIMESTAMP;
        }
        else if (isTimeType(type)) {
            ret = RecTypeCode.REC_TYPE_TIME;
        }
        else if (isDateType(type)) {
            ret = RecTypeCode.REC_TYPE_DATE;
        }
        else if (isTinyIntType(type)) {
            ret = RecTypeCode.REC_TYPE_TINYINT;
        }
        else if (isSmallIntType(type)) {
            ret = RecTypeCode.REC_TYPE_SMALLINT;
        }
        else if (isRealType(type)) {
            ret = RecTypeCode.REC_TYPE_REAL;
        }
        else if (isDoubleType(type)) {
            ret = RecTypeCode.REC_TYPE_DOUBLE;
        }
        else if (isBigIntegerType(type)) {
            ret = RecTypeCode.REC_TYPE_BIGINT;
        }
        else if (isCharType(type) || isVarcharType(type)) {
            ret = (isStrType(type) && (recTypeLength <= fixedLengthStringLimit)) ? RecTypeCode.REC_TYPE_CHAR : RecTypeCode.REC_TYPE_VARCHAR;
        }
        else if (isArrayType(type)) {
            Type elementType = ((ArrayType) type).getElementType();
            if (isIntType(elementType) || isRealType(elementType)) {
                ret = RecTypeCode.REC_TYPE_ARRAY_INT;
            }
            else if (isLongType(elementType)) {
                ret = RecTypeCode.REC_TYPE_ARRAY_BIGINT;
            }
            else if (isVarcharType(elementType)) {
                ret = RecTypeCode.REC_TYPE_ARRAY_VARCHAR;
            }
            else if (isCharType(elementType)) {
                ret = RecTypeCode.REC_TYPE_ARRAY_CHAR;
            }
            else if (isBooleanType(elementType)) {
                ret = RecTypeCode.REC_TYPE_ARRAY_BOOLEAN;
            }
            else if (isDoubleType(elementType)) {
                ret = RecTypeCode.REC_TYPE_ARRAY_DOUBLE;
            }
            else {
                throw new UnsupportedOperationException(String.format("unknown record array type %s", elementType));
            }
        }
        else if (isMapType(type)) {
            return convertToRecTypeCode(((MapType) type).getValueType(), recTypeLength, fixedLengthStringLimit);
        }
        else {
            throw new UnsupportedOperationException(String.format("unknown record type %s", type));
        }
        return ret;
    }

    public static boolean isMappedMatchCollectSupportedTypes(Type type)
    {
        if (type == null) {
            return false;
        }

        return isIntegerType(type) || isBigIntegerType(type) || isSmallIntType(type) || isRealType(type) ||
                isDoubleType(type) || isLongType(type) || isShortDecimalType(type) || isLongDecimalType(type);
    }
}
