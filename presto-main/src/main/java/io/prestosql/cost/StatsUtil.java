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
package io.prestosql.cost;

import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.ResolvedFunction;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.InterpretedFunctionInvoker;

import java.util.OptionalDouble;

import static io.prestosql.spi.type.DoubleType.DOUBLE;
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
            return OptionalDouble.of(((Long) value).doubleValue());
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
