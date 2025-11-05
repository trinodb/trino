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
package io.trino.plugin.postgresql.rule;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.expression.AbstractRewriteCast;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

import static java.sql.Types.BIGINT;
import static java.sql.Types.BIT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.SMALLINT;

public class RewriteCast
        extends AbstractRewriteCast
{
    private static final List<Integer> SUPPORTED_SOURCE_TYPE_FOR_INTEGRAL_CAST = ImmutableList.of(BIT, SMALLINT, INTEGER, BIGINT, NUMERIC);

    public RewriteCast(BiFunction<ConnectorSession, Type, String> jdbcTypeProvider)
    {
        super(jdbcTypeProvider);
    }

    @Override
    protected Optional<JdbcTypeHandle> toJdbcTypeHandle(JdbcTypeHandle sourceType, Type targetType)
    {
        if (!pushdownSupported(sourceType, targetType)) {
            return Optional.empty();
        }

        return switch (targetType) {
            case SmallintType smallintType ->
                    Optional.of(new JdbcTypeHandle(SMALLINT, Optional.of(smallintType.getBaseName()), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
            case IntegerType integerType ->
                    Optional.of(new JdbcTypeHandle(INTEGER, Optional.of(integerType.getBaseName()), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
            case BigintType bigintType ->
                    Optional.of(new JdbcTypeHandle(BIGINT, Optional.of(bigintType.getBaseName()), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
            default -> Optional.empty();
        };
    }

    private static boolean pushdownSupported(JdbcTypeHandle sourceType, Type targetType)
    {
        return switch (targetType) {
            case SmallintType _, IntegerType _, BigintType _ -> SUPPORTED_SOURCE_TYPE_FOR_INTEGRAL_CAST.contains(sourceType.jdbcType());
            default -> false;
        };
    }

    @Override
    protected String buildCast(Type sourceType, Type targetType, String expression, String castType)
    {
        if (sourceType instanceof DecimalType && isIntegralType(targetType)) {
            // Trino rounds up to nearest integral value, whereas Postgresql does not.
            // So using ROUND() to make pushdown same as the trino behavior
            return "CAST(ROUND(%s) AS %s)".formatted(expression, castType);
        }
        if (sourceType instanceof BooleanType && isIntegralType(targetType)) {
            // Cast boolean to int first
            return "CAST(%s::INT AS %s)".formatted(expression, castType);
        }
        return "CAST(%s AS %s)".formatted(expression, castType);
    }

    private static boolean isIntegralType(Type type)
    {
        return type instanceof SmallintType
                || type instanceof IntegerType
                || type instanceof BigintType;
    }
}
