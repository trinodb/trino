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

import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;

import java.sql.Types;
import java.util.Optional;

public final class YdbTypeUtils
{
    private YdbTypeUtils() {}

    public static Optional<JdbcTypeHandle> toTypeHandle(Type type)
    {
        return switch (type) {
            case io.trino.spi.type.BooleanType _ -> Optional.of(new JdbcTypeHandle(Types.BOOLEAN, Optional.of("Bool"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
            case io.trino.spi.type.TinyintType _ -> Optional.of(new JdbcTypeHandle(Types.TINYINT, Optional.of("Int8"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
            case io.trino.spi.type.SmallintType _ -> Optional.of(new JdbcTypeHandle(Types.SMALLINT, Optional.of("Int16"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
            case io.trino.spi.type.IntegerType _ -> Optional.of(new JdbcTypeHandle(Types.INTEGER, Optional.of("Int32"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
            case io.trino.spi.type.BigintType _ -> Optional.of(new JdbcTypeHandle(Types.BIGINT, Optional.of("Int64"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
            case io.trino.spi.type.RealType _ -> Optional.of(new JdbcTypeHandle(Types.REAL, Optional.of("Float"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
            case io.trino.spi.type.DoubleType _ -> Optional.of(new JdbcTypeHandle(Types.DOUBLE, Optional.of("Double"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
            case io.trino.spi.type.DateType _ -> Optional.of(new JdbcTypeHandle(Types.DATE, Optional.of("Date32"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
            case io.trino.spi.type.TimestampType timestampType when timestampType.getPrecision() <= 6 -> Optional.of(new JdbcTypeHandle(Types.TIMESTAMP, Optional.of("Timestamp64"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
            case DecimalType decimalType -> Optional.of(new JdbcTypeHandle(Types.DECIMAL, Optional.of("Decimal"), Optional.of(decimalType.getPrecision()), Optional.of(decimalType.getScale()), Optional.empty(), Optional.empty()));
            case io.trino.spi.type.VarcharType _ -> Optional.of(new JdbcTypeHandle(Types.VARCHAR, Optional.of("Text"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
            case io.trino.spi.type.VarbinaryType _ -> Optional.of(new JdbcTypeHandle(Types.BINARY, Optional.of("Bytes"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
            default -> Optional.empty();
        };
    }
}
