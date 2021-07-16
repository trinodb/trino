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
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.MAX_SHORT_PRECISION;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TestingIdType.ID;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class TestingTypeManager
        implements TypeManager
{
    private static final List<Type> TYPES = ImmutableList.of(BOOLEAN, BIGINT, DOUBLE, INTEGER, VARCHAR, VARBINARY, TIMESTAMP_MILLIS, TIMESTAMP_WITH_TIME_ZONE, DATE, ID,
            HYPER_LOG_LOG,
            createMapType(BIGINT, BIGINT),
            createMapType(createDecimalType(MAX_SHORT_PRECISION + 1), createDecimalType(MAX_SHORT_PRECISION + 1)),
            createMapType(INTEGER, INTEGER),
            createMapType(SMALLINT, SMALLINT),
            createMapType(BOOLEAN, BOOLEAN),
            createMapType(VARCHAR, VARCHAR),
            createMapType(BIGINT, createMapType(BIGINT, BIGINT)),
            createMapType(createDecimalType(MAX_SHORT_PRECISION + 1), createMapType(createDecimalType(MAX_SHORT_PRECISION + 1), createDecimalType(MAX_SHORT_PRECISION + 1))),
            createMapType(INTEGER, createMapType(INTEGER, INTEGER)),
            createMapType(SMALLINT, createMapType(SMALLINT, SMALLINT)),
            createMapType(BOOLEAN, createMapType(BOOLEAN, BOOLEAN)),
            createMapType(VARCHAR, createMapType(VARCHAR, VARCHAR)),
            createMapType(BIGINT, new ArrayType(BIGINT)),
            createMapType(createDecimalType(MAX_SHORT_PRECISION + 1), new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1))),
            createMapType(INTEGER, new ArrayType(INTEGER)),
            createMapType(SMALLINT, new ArrayType(SMALLINT)),
            createMapType(BOOLEAN, new ArrayType(BOOLEAN)),
            createMapType(VARCHAR, new ArrayType(VARCHAR)),
            createMapType(BIGINT, createDecimalType(MAX_SHORT_PRECISION + 1)));

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

    public static MapType createMapType(Type keyType, Type valueType)
    {
        return new MapType(
                keyType,
                valueType,
                new TypeOperators());
    }
}
