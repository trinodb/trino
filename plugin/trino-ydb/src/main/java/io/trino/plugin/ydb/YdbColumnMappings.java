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
package io.trino.plugin.ydb;

import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static io.trino.plugin.jdbc.StandardColumnMappings.dateReadFunctionUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalReadFunction;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static tech.ydb.jdbc.YdbConst.SQL_KIND_DECIMAL;
import static tech.ydb.jdbc.YdbConst.SQL_KIND_PRIMITIVE;

final class YdbColumnMappings
{
    // https://github.com/ydb-platform/ydb-jdbc-driver/blob/v2.4.1/jdbc/src/main/java/tech/ydb/jdbc/common/YdbTypes.java#L64-L77
    static final int YDB_DATE32_SQL_TYPE = SQL_KIND_PRIMITIVE + 25;
    private static final BigInteger MAX_UINT64 = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);

    private YdbColumnMappings() {}

    // Date and Date32 parameters are bound as Date32, which YQL compares with both column types, so predicates
    // such as d > DATE '1960-01-01' on a Date column push down correctly
    // https://ydb.tech/docs/en/yql/reference/types/primitive#datetime
    static ColumnMapping dateColumnMapping()
    {
        return ColumnMapping.longMapping(
                DATE,
                dateReadFunctionUsingLocalDate(),
                LongWriteFunction.of(YDB_DATE32_SQL_TYPE, (statement, index, value) -> statement.setObject(index, value, YDB_DATE32_SQL_TYPE)));
    }

    static ColumnMapping unsignedBigintColumnMapping()
    {
        DecimalType type = createDecimalType(20, 0);
        int sqlType = SQL_KIND_PRIMITIVE + 8;
        return ColumnMapping.objectMapping(
                type,
                longDecimalReadFunction(type),
                objectWriteFunction(sqlType, (statement, index, value) -> {
                    // The driver binds a long as Uint64 without a range check, so u > -1 would silently become
                    // u > 18446744073709551615. Reject such values eagerly, like the ClickHouse connector does.
                    BigInteger unsigned = value.toBigInteger();
                    if (unsigned.signum() < 0 || unsigned.compareTo(MAX_UINT64) > 0) {
                        throw new TrinoException(INVALID_ARGUMENTS, "Value must be between 0 and %s in YDB: %s".formatted(MAX_UINT64, unsigned));
                    }
                    statement.setObject(index, unsigned.longValue(), sqlType);
                }));
    }

    static ColumnMapping decimalColumnMapping(DecimalType type)
    {
        int sqlType = SQL_KIND_DECIMAL + (type.getPrecision() << 6) + type.getScale();
        if (type.isShort()) {
            return ColumnMapping.longMapping(
                    type,
                    shortDecimalReadFunction(type),
                    LongWriteFunction.of(sqlType, (statement, index, value) ->
                            statement.setObject(index, BigDecimal.valueOf(value, type.getScale()), sqlType)));
        }
        return ColumnMapping.objectMapping(
                type,
                longDecimalReadFunction(type),
                objectWriteFunction(sqlType, (statement, index, value) ->
                        statement.setObject(index, new BigDecimal(value.toBigInteger(), type.getScale()), sqlType)));
    }

    private static ObjectWriteFunction objectWriteFunction(int sqlType, ObjectWriteFunction.ObjectWriteFunctionImplementation<Int128> implementation)
    {
        return new ObjectWriteFunction()
        {
            @Override
            public Class<?> getJavaType()
            {
                return Int128.class;
            }

            @Override
            public void set(PreparedStatement statement, int index, Object value)
                    throws SQLException
            {
                implementation.set(statement, index, (Int128) value);
            }

            @Override
            public void setNull(PreparedStatement statement, int index)
                    throws SQLException
            {
                statement.setNull(index, sqlType);
            }
        };
    }
}
