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

import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;

import static io.trino.spi.type.StandardTypes.ARRAY;
import static io.trino.spi.type.StandardTypes.MAP;
import static io.trino.spi.type.StandardTypes.ROW;
import static java.util.stream.Collectors.joining;

public final class TypeUtils
{
    public static final int NULL_HASH_CODE = 0;

    private TypeUtils() {}

    public static String getDisplayLabel(Type type, boolean legacy)
    {
        if (legacy) {
            return getDisplayLabelForLegacyClients(type);
        }
        return type.getDisplayName();
    }

    private static String getDisplayLabelForLegacyClients(Type type)
    {
        if (type instanceof TimestampType timestampType && timestampType.getPrecision() == TimestampType.DEFAULT_PRECISION) {
            return StandardTypes.TIMESTAMP;
        }
        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType && timestampWithTimeZoneType.getPrecision() == TimestampWithTimeZoneType.DEFAULT_PRECISION) {
            return StandardTypes.TIMESTAMP_WITH_TIME_ZONE;
        }
        if (type instanceof TimeType timeType && timeType.getPrecision() == TimeType.DEFAULT_PRECISION) {
            return StandardTypes.TIME;
        }
        if (type instanceof TimeWithTimeZoneType timeWithTimeZoneType && timeWithTimeZoneType.getPrecision() == TimeWithTimeZoneType.DEFAULT_PRECISION) {
            return StandardTypes.TIME_WITH_TIME_ZONE;
        }
        if (type instanceof ArrayType arrayType) {
            return ARRAY + "(" + getDisplayLabelForLegacyClients(arrayType.getElementType()) + ")";
        }
        if (type instanceof MapType mapType) {
            return MAP + "(" + getDisplayLabelForLegacyClients(mapType.getKeyType()) + ", " + getDisplayLabelForLegacyClients(mapType.getValueType()) + ")";
        }
        if (type instanceof RowType rowType) {
            return getRowDisplayLabelForLegacyClients(rowType);
        }

        return type.getDisplayName();
    }

    private static String getRowDisplayLabelForLegacyClients(RowType type)
    {
        return type.getFields().stream()
                .map(field -> {
                    String typeDisplayName = getDisplayLabelForLegacyClients(field.getType());
                    if (field.getName().isPresent()) {
                        return field.getName().get() + ' ' + typeDisplayName;
                    }
                    return typeDisplayName;
                })
                .collect(joining(", ", ROW + "(", ")"));
    }
}
