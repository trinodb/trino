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
package io.trino.plugin.redshift;

import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.expression.AbstractRewriteCast;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;

import java.sql.Types;
import java.util.Optional;
import java.util.function.BiFunction;

public class RewriteCast
        extends AbstractRewriteCast
{
    public RewriteCast(BiFunction<ConnectorSession, Type, String> toTargetType)
    {
        super(toTargetType);
    }

    @Override
    protected boolean eligibleForPushdown(Type sourceType, Type targetType)
    {
        if (targetType instanceof TinyintType) {
            return supportedSourceTypeToCastInIntegralType(sourceType);
        }
        if (targetType instanceof SmallintType) {
            return supportedSourceTypeToCastInIntegralType(sourceType);
        }
        if (targetType instanceof IntegerType) {
            return supportedSourceTypeToCastInIntegralType(sourceType);
        }
        if (targetType instanceof BigintType) {
            return supportedSourceTypeToCastInIntegralType(sourceType);
        }
        return false;
    }

    @Override
    protected Optional<JdbcTypeHandle> toJdbcTypeHandle(Type type)
    {
        if (type instanceof TinyintType) {
            return Optional.of(new JdbcTypeHandle(Types.TINYINT, Optional.of("tinyint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
        }
        if (type instanceof SmallintType) {
            return Optional.of(new JdbcTypeHandle(Types.SMALLINT, Optional.of("smallint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
        }
        if (type instanceof IntegerType integerType) {
            return Optional.of(new JdbcTypeHandle(Types.INTEGER, Optional.of(integerType.getBaseName()), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
        }
        if (type instanceof BigintType bigintType) {
            return Optional.of(new JdbcTypeHandle(Types.BIGINT, Optional.of(bigintType.getBaseName()), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
        }
        return Optional.empty();
    }

    @Override
    protected String buildCast(Type sourceType, Type targetType, String expression, String castType)
    {
        if (isDecimalType(sourceType) && isIntegralType(targetType)) {
            return "CAST(ROUND(%s) AS %s)".formatted(expression, castType);
        }
        return "CAST(%s AS %s)".formatted(expression, castType);
    }

    private static boolean supportedSourceTypeToCastInIntegralType(Type sourceType)
    {
        return sourceType instanceof BooleanType
                || sourceType instanceof TinyintType
                || sourceType instanceof SmallintType
                || sourceType instanceof IntegerType
                || sourceType instanceof BigintType
                || sourceType instanceof RealType
                || sourceType instanceof DoubleType
                || sourceType instanceof DecimalType;
    }

    private boolean isDecimalType(Type type)
    {
        return type instanceof RealType
                || type instanceof DoubleType
                || type instanceof DecimalType;
    }

    private boolean isIntegralType(Type type)
    {
        return type instanceof TinyintType
                || type instanceof SmallintType
                || type instanceof IntegerType
                || type instanceof BigintType;
    }
}
