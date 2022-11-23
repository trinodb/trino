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
package io.trino.spi.type;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TestingIdType.ID;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class TestingTypeManager
        implements TypeManager
{
    private static final List<Type> TYPES = ImmutableList.of(BOOLEAN, BIGINT, DOUBLE, INTEGER, VARCHAR, VARBINARY, TIMESTAMP_MILLIS, TIMESTAMP_TZ_MILLIS, DATE, ID, HYPER_LOG_LOG);

    private final TypeOperators typeOperators = new TypeOperators();

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
    public TypeOperators getTypeOperators()
    {
        return typeOperators;
    }
}
