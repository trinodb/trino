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
package io.prestosql.spi.type;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.function.InvocationConvention;
import io.prestosql.spi.function.OperatorType;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.TestingIdType.ID;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;

public class TestingTypeManager
        implements TypeManager
{
    private static final List<Type> TYPES = ImmutableList.of(BOOLEAN, BIGINT, DOUBLE, INTEGER, VARCHAR, VARBINARY, TIMESTAMP_MILLIS, TIMESTAMP_WITH_TIME_ZONE, DATE, ID, HYPER_LOG_LOG);

    @Override
    public Type getType(TypeSignature signature)
    {
        for (Type type : TYPES) {
            if (signature.getBase().equals(type.getTypeSignature().getBase())) {
                return type;
            }
        }
        throw new TypeNotFoundException(signature);
    }

    @Override
    public Type fromSqlType(String type)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Type getType(TypeId id)
    {
        for (Type type : TYPES) {
            if (type.getTypeId().equals(id)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Type not found: " + id);
    }

    @Override
    public MethodHandle resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes, InvocationConvention invocationConvention)
    {
        throw new UnsupportedOperationException();
    }
}
