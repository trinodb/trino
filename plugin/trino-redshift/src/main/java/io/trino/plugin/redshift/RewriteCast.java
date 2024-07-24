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
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.IntegerType;
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
    protected Optional<JdbcTypeHandle> toJdbcTypeHandle(JdbcTypeHandle sourceType, Type targetType)
    {
        if (!pushdownSupported(sourceType, targetType)) {
            return Optional.empty();
        }

        if (targetType instanceof TinyintType) {
            return Optional.of(new JdbcTypeHandle(Types.TINYINT, Optional.of("tinyint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
        }
        if (targetType instanceof SmallintType) {
            return Optional.of(new JdbcTypeHandle(Types.SMALLINT, Optional.of("smallint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
        }
        if (targetType instanceof IntegerType integerType) {
            return Optional.of(new JdbcTypeHandle(Types.INTEGER, Optional.of(integerType.getBaseName()), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
        }
        if (targetType instanceof BigintType bigintType) {
            return Optional.of(new JdbcTypeHandle(Types.BIGINT, Optional.of(bigintType.getBaseName()), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
        }
        return Optional.empty();
    }

    private boolean pushdownSupported(JdbcTypeHandle sourceType, Type targetType)
    {
        if (targetType instanceof TinyintType) {
            return supportedSourceTypeToCastToIntegralType(sourceType);
        }
        if (targetType instanceof SmallintType) {
            return supportedSourceTypeToCastToIntegralType(sourceType);
        }
        if (targetType instanceof IntegerType) {
            return supportedSourceTypeToCastToIntegralType(sourceType);
        }
        if (targetType instanceof BigintType) {
            return supportedSourceTypeToCastToIntegralType(sourceType);
        }
        return false;
    }

    @Override
    protected String buildCast(Type sourceType, Type targetType, String expression, String castType)
    {
        if (sourceType instanceof DecimalType && isIntegralType(targetType)) {
            // Trino rounds up to nearest integral value, whereas Redshift does not.
            // So using ROUND() to make pushdown same as the trino behavior
            return "CAST(ROUND(%s) AS %s)".formatted(expression, castType);
        }
        return "CAST(%s AS %s)".formatted(expression, castType);
    }

    private static boolean supportedSourceTypeToCastToIntegralType(JdbcTypeHandle sourceType)
    {
        return switch (sourceType.jdbcType()) {
            case Types.BIT,
                    Types.TINYINT,
                    Types.SMALLINT,
                    Types.INTEGER,
                    Types.BIGINT,
                    Types.NUMERIC -> true;
            default -> false;
        };
    }

    private boolean isIntegralType(Type type)
    {
        return type instanceof TinyintType
                || type instanceof SmallintType
                || type instanceof IntegerType
                || type instanceof BigintType;
    }
}
