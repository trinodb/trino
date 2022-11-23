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
package io.trino.type;

import com.google.common.base.Joiner;
import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.FixedWidthType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.StandardTypes.ARRAY;
import static io.trino.spi.type.StandardTypes.MAP;
import static io.trino.spi.type.StandardTypes.ROW;
import static java.lang.String.format;

public final class TypeUtils
{
    public static final int NULL_HASH_CODE = 0;

    private TypeUtils()
    {
    }

    public static int expectedValueSize(Type type, int defaultSize)
    {
        if (type instanceof FixedWidthType) {
            return ((FixedWidthType) type).getFixedSize();
        }
        // If bound on length of varchar or char is smaller than defaultSize, use that as expected size
        // The data can take up to 4 bytes per character due to UTF-8 encoding, but we assume it is ASCII and only needs one byte.
        if (type instanceof VarcharType) {
            return ((VarcharType) type).getLength()
                    .map(length -> Math.min(length, defaultSize))
                    .orElse(defaultSize);
        }
        if (type instanceof CharType) {
            return Math.min(((CharType) type).getLength(), defaultSize);
        }
        return defaultSize;
    }

    public static void checkElementNotNull(boolean isNull, String errorMsg)
    {
        if (isNull) {
            throw new TrinoException(NOT_SUPPORTED, errorMsg);
        }
    }

    public static String getDisplayLabel(Type type, boolean legacy)
    {
        if (legacy) {
            return getDisplayLabelForLegacyClients(type);
        }
        return type.getDisplayName();
    }

    private static String getDisplayLabelForLegacyClients(Type type)
    {
        if (type instanceof TimestampType && ((TimestampType) type).getPrecision() == TimestampType.DEFAULT_PRECISION) {
            return StandardTypes.TIMESTAMP;
        }
        if (type instanceof TimestampWithTimeZoneType && ((TimestampWithTimeZoneType) type).getPrecision() == TimestampWithTimeZoneType.DEFAULT_PRECISION) {
            return StandardTypes.TIMESTAMP_WITH_TIME_ZONE;
        }
        if (type instanceof TimeType && ((TimeType) type).getPrecision() == TimeType.DEFAULT_PRECISION) {
            return StandardTypes.TIME;
        }
        if (type instanceof TimeWithTimeZoneType && ((TimeWithTimeZoneType) type).getPrecision() == TimeWithTimeZoneType.DEFAULT_PRECISION) {
            return StandardTypes.TIME_WITH_TIME_ZONE;
        }
        if (type instanceof ArrayType) {
            return ARRAY + "(" + getDisplayLabelForLegacyClients(((ArrayType) type).getElementType()) + ")";
        }
        if (type instanceof MapType) {
            return MAP + "(" + getDisplayLabelForLegacyClients(((MapType) type).getKeyType()) + ", " + getDisplayLabelForLegacyClients(((MapType) type).getValueType()) + ")";
        }
        if (type instanceof RowType) {
            return getRowDisplayLabelForLegacyClients((RowType) type);
        }

        return type.getDisplayName();
    }

    private static String getRowDisplayLabelForLegacyClients(RowType type)
    {
        List<String> fields = type.getFields().stream()
                .map(field -> {
                    String typeDisplayName = getDisplayLabelForLegacyClients(field.getType());
                    if (field.getName().isPresent()) {
                        return field.getName().get() + ' ' + typeDisplayName;
                    }
                    return typeDisplayName;
                })
                .collect(toImmutableList());

        return format("%s(%s)", ROW, Joiner.on(", ").join(fields));
    }
}
