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
package io.prestosql.type.coercions;

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;

import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static java.lang.String.format;

public class TypeCoercers
{
    private TypeCoercers() {}

    public static TypeCoercer<?, ?> get(Type sourceType, Type targetType)
    {
        if (sourceType.equals(targetType)) {
            return new NoopCoercer(sourceType, targetType);
        }

        if (sourceType instanceof DecimalType) {
            return DecimalCoercers.createCoercer((DecimalType) sourceType, targetType);
        }

        if (sourceType instanceof DoubleType) {
            return DoubleCoercers.createCoercer((DoubleType) sourceType, targetType);
        }

        if (sourceType instanceof RealType) {
            return RealCoercers.createCoercer((RealType) sourceType, targetType);
        }

        if (sourceType instanceof VarbinaryType) {
            return VarbinaryCoercers.createCoercer((VarbinaryType) sourceType, targetType);
        }

        if (sourceType instanceof VarcharType) {
            return VarcharCoercers.createCoercer((VarcharType) sourceType, targetType);
        }

        if (sourceType instanceof TimestampType) {
            return TimestampCoercers.createCoercer((TimestampType) sourceType, targetType);
        }

        if (isNumber(sourceType)) {
            return NumberCoercers.createCoercer(sourceType, targetType);
        }

        throw new IllegalStateException(format("Could not coerce from %s to %s", sourceType, targetType));
    }

    public static boolean isNumber(Type type)
    {
        return type == TinyintType.TINYINT || type == SmallintType.SMALLINT || type == IntegerType.INTEGER || type == BigintType.BIGINT;
    }

    public static boolean longCanBeCasted(long value, Type sourceType, Type targetType)
    {
        long minValue;
        long maxValue;

        if (targetType.equals(TINYINT)) {
            minValue = Byte.MIN_VALUE;
            maxValue = Byte.MAX_VALUE;
        }
        else if (targetType.equals(SMALLINT)) {
            minValue = Short.MIN_VALUE;
            maxValue = Short.MAX_VALUE;
        }
        else if (targetType.equals(INTEGER)) {
            minValue = Integer.MIN_VALUE;
            maxValue = Integer.MAX_VALUE;
        }
        else if (targetType.equals(BIGINT)) {
            minValue = Long.MIN_VALUE;
            maxValue = Long.MAX_VALUE;
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, format("Could not coerce from %s to %s", sourceType, targetType));
        }

        return (minValue <= value && value <= maxValue);
    }
}
