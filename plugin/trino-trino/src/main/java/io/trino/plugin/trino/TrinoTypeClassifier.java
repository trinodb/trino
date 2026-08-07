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
package io.trino.plugin.trino;

import io.trino.plugin.jdbc.PredicatePushdownController;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.NumberType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;

final class TrinoTypeClassifier
{
    enum TransportKind
    {
        NATIVE,
        VARCHAR_CAST,
        VARBINARY_CAST,
        JSON_CAST,
    }

    enum SketchTransportKind
    {
        NONE,
        SLICE_BACKED,
        TDIGEST,
    }

    private TrinoTypeClassifier() {}

    static boolean supportsComplexReadType(Type type)
    {
        if (type instanceof ArrayType arrayType) {
            return supportsComplexReadType(arrayType.getElementType());
        }
        if (type instanceof MapType mapType) {
            return supportsComplexReadType(mapType.getKeyType()) && supportsComplexReadType(mapType.getValueType());
        }
        if (type instanceof RowType rowType) {
            return rowType.getFields().stream()
                    .map(RowType.Field::getType)
                    .allMatch(TrinoTypeClassifier::supportsComplexReadType);
        }
        return isNativeComplexLeafType(type);
    }

    static boolean requiresVarcharTransport(Type type)
    {
        if (type instanceof ArrayType || type instanceof MapType || type instanceof RowType) {
            return false;
        }
        if (type instanceof TimeType timeType) {
            return timeType.getPrecision() > 9;
        }
        if (type instanceof TimestampType timestampType) {
            return timestampType.getPrecision() > 9;
        }
        if (type instanceof TimestampWithTimeZoneType) {
            return true;
        }
        if (type instanceof TimeWithTimeZoneType) {
            return true;
        }
        return isIntervalYearToMonthType(type) || isIntervalDayToSecondType(type);
    }

    static boolean requiresVarbinaryTransport(Type type)
    {
        if (type instanceof ArrayType || type instanceof MapType || type instanceof RowType) {
            return false;
        }
        return sketchTransportKind(type) != SketchTransportKind.NONE;
    }

    static boolean requiresJsonTransport(Type type)
    {
        return (type instanceof ArrayType || type instanceof MapType || type instanceof RowType) &&
                !supportsComplexReadType(type) &&
                supportsJsonTransportType(type);
    }

    static PredicatePushdownController transportPredicatePushdownController(Type logicalType)
    {
        if (logicalType instanceof TimeType || logicalType instanceof TimestampType || logicalType instanceof TimestampWithTimeZoneType || logicalType instanceof TimeWithTimeZoneType) {
            return FULL_PUSHDOWN;
        }
        if (isIntervalYearToMonthType(logicalType) || isIntervalDayToSecondType(logicalType)) {
            return FULL_PUSHDOWN;
        }
        return DISABLE_PUSHDOWN;
    }

    static boolean isSliceBackedSketchType(Type type)
    {
        return sketchTransportKind(type) == SketchTransportKind.SLICE_BACKED;
    }

    static boolean isTDigestType(Type type)
    {
        return sketchTransportKind(type) == SketchTransportKind.TDIGEST;
    }

    static boolean isIntervalYearToMonthType(Type type)
    {
        return type.getDisplayName().startsWith("interval year to month");
    }

    static boolean isIntervalDayToSecondType(Type type)
    {
        return type.getDisplayName().startsWith("interval day to second");
    }

    static boolean isJsonType(Type type)
    {
        return type.getBaseName().equals("json");
    }

    static boolean isIpAddressType(Type type)
    {
        return type.getBaseName().equals("ipaddress");
    }

    static boolean isNumberType(Type type)
    {
        return type.getBaseName().equals(NumberType.NAME);
    }

    static TransportKind transportKind(Type type)
    {
        if (requiresVarbinaryTransport(type)) {
            return TransportKind.VARBINARY_CAST;
        }
        if (requiresJsonTransport(type)) {
            return TransportKind.JSON_CAST;
        }
        if (requiresVarcharTransport(type)) {
            return TransportKind.VARCHAR_CAST;
        }
        return TransportKind.NATIVE;
    }

    private static SketchTransportKind sketchTransportKind(Type type)
    {
        String typeBase = type.getBaseName();
        if (typeBase.equalsIgnoreCase("HyperLogLog") ||
                typeBase.equalsIgnoreCase("P4HyperLogLog") ||
                typeBase.equalsIgnoreCase("qdigest") ||
                typeBase.equalsIgnoreCase("setdigest")) {
            return SketchTransportKind.SLICE_BACKED;
        }
        if (typeBase.equalsIgnoreCase("tdigest")) {
            return SketchTransportKind.TDIGEST;
        }
        return SketchTransportKind.NONE;
    }

    private static boolean supportsJsonTransportType(Type type)
    {
        if (type instanceof ArrayType arrayType) {
            return supportsJsonTransportType(arrayType.getElementType());
        }
        if (type instanceof MapType mapType) {
            return supportsJsonTransportType(mapType.getKeyType()) && supportsJsonTransportType(mapType.getValueType());
        }
        if (type instanceof RowType rowType) {
            return rowType.getFields().stream()
                    .map(RowType.Field::getType)
                    .allMatch(TrinoTypeClassifier::supportsJsonTransportType);
        }
        return supportsStringSurrogateLeafType(type);
    }

    private static boolean isCommonLeafType(Type type)
    {
        if (type == BOOLEAN || type == TINYINT || type == SMALLINT || type == INTEGER || type == BIGINT || type == REAL || type == DOUBLE) {
            return true;
        }
        if (type instanceof CharType || type instanceof VarcharType || type instanceof VarbinaryType || type instanceof DateType || type instanceof DecimalType || type instanceof UuidType) {
            return true;
        }
        return isJsonType(type) || isIpAddressType(type) || isNumberType(type);
    }

    private static boolean supportsStringSurrogateLeafType(Type type)
    {
        if (isCommonLeafType(type)) {
            return true;
        }
        if (type instanceof TimeType || type instanceof TimestampType || type instanceof TimestampWithTimeZoneType || type instanceof TimeWithTimeZoneType) {
            return true;
        }
        return isIntervalYearToMonthType(type) || isIntervalDayToSecondType(type);
    }

    private static boolean isNativeComplexLeafType(Type type)
    {
        // Trino JDBC converts nested dates through java.sql.Date, which changes
        // proleptic Gregorian dates before the Gregorian cutover and BCE dates.
        if (type instanceof DateType) {
            return false;
        }
        if (isCommonLeafType(type)) {
            return true;
        }
        if (type instanceof TimeType timeType) {
            // Trino JDBC exposes nested times as java.sql.Time, which does not
            // preserve fractional seconds.
            return timeType.getPrecision() == 0;
        }
        // JDBC ARRAY / MAP / ROW decoding does not preserve timestamp semantics exactly,
        // so timestamps inside complex values must use transport rewriting instead.
        return false;
    }
}
