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
package io.trino.plugin.mysql.rule;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.expression.AbstractRewriteCast;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

import static java.sql.Types.BIGINT;
import static java.sql.Types.BIT;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.REAL;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TINYINT;

public class RewriteCast
        extends AbstractRewriteCast
{
    private static final List<Integer> SUPPORTED_SOURCE_TYPE_FOR_INTEGRAL_CAST = ImmutableList.of(BIT, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, REAL, DOUBLE, DECIMAL, NUMERIC);

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
            case RealType realType ->
                    Optional.of(new JdbcTypeHandle(REAL, Optional.of(realType.getBaseName()), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
            case DoubleType doubleType ->
                    Optional.of(new JdbcTypeHandle(DOUBLE, Optional.of(doubleType.getBaseName()), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
            case DecimalType decimalType ->
                    Optional.of(new JdbcTypeHandle(DECIMAL, Optional.of(decimalType.getBaseName()), Optional.of(decimalType.getPrecision()), Optional.of(decimalType.getScale()), Optional.empty(), Optional.empty()));
            default -> Optional.empty();
        };
    }

    private static boolean pushdownSupported(JdbcTypeHandle sourceType, Type targetType)
    {
        return switch (targetType) {
            case RealType _, DoubleType _, DecimalType _ -> SUPPORTED_SOURCE_TYPE_FOR_INTEGRAL_CAST.contains(sourceType.jdbcType());
            default -> false;
        };
    }
}
