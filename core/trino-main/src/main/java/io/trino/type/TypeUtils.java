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

import io.trino.spi.type.BigintType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;

public final class TypeUtils
{
    public static final int NULL_HASH_CODE = 0;

    private TypeUtils() {}

    public static boolean isDirectLongComparisonValidForContinuousValues(Type type)
    {
        // Types for which we can safely use equality and comparison on the stored long value
        // instead of going through type specific methods and where the next consecutive value
        // can be obtained by incrementing the stored long value by 1
        return type instanceof TinyintType ||
                type instanceof SmallintType ||
                type instanceof IntegerType ||
                type instanceof BigintType ||
                (type instanceof TimeType timeType && timeType.getPrecision() == 12) ||
                type instanceof DateType ||
                (type instanceof TimestampType timestampType && timestampType.getPrecision() == 6) ||
                (type instanceof DecimalType decimalType && decimalType.isShort());
    }
}
