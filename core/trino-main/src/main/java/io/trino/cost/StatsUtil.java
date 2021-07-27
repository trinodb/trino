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
package io.trino.cost;

import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.sql.InterpretedFunctionInvoker;

import java.util.OptionalDouble;

import static io.trino.spi.type.DoubleType.DOUBLE;
import static java.util.Collections.singletonList;

final class StatsUtil
{
    private StatsUtil() {}

    static OptionalDouble toStatsRepresentation(Metadata metadata, Session session, Type type, Object value)
    {
        if (convertibleToDoubleWithCast(type)) {
            InterpretedFunctionInvoker functionInvoker = new InterpretedFunctionInvoker(metadata);
            ResolvedFunction castFunction = metadata.getCoercion(type, DOUBLE);
            return OptionalDouble.of((double) functionInvoker.invoke(castFunction, session.toConnectorSession(), singletonList(value)));
        }

        if (DateType.DATE.equals(type)) {
            return OptionalDouble.of((long) value);
        }

        if (type instanceof TimestampType) {
            if (((TimestampType) type).isShort()) {
                return OptionalDouble.of((long) value);
            }
            return OptionalDouble.of(((LongTimestamp) value).getEpochMicros());
        }

        return OptionalDouble.empty();
    }

    private static boolean convertibleToDoubleWithCast(Type type)
    {
        return type instanceof DecimalType
                || type instanceof DoubleType
                || type instanceof RealType
                || type instanceof BigintType
                || type instanceof IntegerType
                || type instanceof SmallintType
                || type instanceof TinyintType
                || type instanceof BooleanType;
    }
}
