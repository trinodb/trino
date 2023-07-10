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
package io.trino.plugin.deltalake;

import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;

public final class DeltaLakeTypes
{
    private DeltaLakeTypes() {}

    public static Type toParquetType(TypeOperators typeOperators, Type type)
    {
        if (type instanceof TimestampWithTimeZoneType timestamp) {
            verify(timestamp.getPrecision() == 3, "Unsupported type: %s", type);
            return TIMESTAMP_MILLIS;
        }
        if (type instanceof ArrayType arrayType) {
            return new ArrayType(toParquetType(typeOperators, arrayType.getElementType()));
        }
        if (type instanceof MapType mapType) {
            return new MapType(toParquetType(typeOperators, mapType.getKeyType()), toParquetType(typeOperators, mapType.getValueType()), typeOperators);
        }
        if (type instanceof RowType rowType) {
            return RowType.from(rowType.getFields().stream()
                    .map(field -> RowType.field(field.getName().orElseThrow(), toParquetType(typeOperators, field.getType())))
                    .collect(toImmutableList()));
        }
        return type;
    }
}
