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
package io.trino.plugin.jdbc.expression;

import com.google.common.collect.ImmutableList;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.spi.expression.Constant;
import io.trino.spi.type.Type;

import java.util.Optional;

import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.constant;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;

public class RewriteApproximateNumericConstant
        implements ConnectorExpressionRule<Constant, ParameterizedExpression>
{
    private static final Pattern<Constant> PATTERN = constant().with(type().matching(type ->
            type == REAL || type == DOUBLE));

    @Override
    public Pattern<Constant> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<ParameterizedExpression> rewrite(Constant constant, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        Type type = constant.getType();
        Object value = constant.getValue();
        if (value == null) {
            // TODO we could handle NULL values too
            return Optional.empty();
        }
        if (type == DOUBLE) {
            double doubleValue = (double) value;
            if (Double.isNaN(doubleValue) || Double.isInfinite(doubleValue)) {
                return Optional.empty();
            }
            return Optional.of(new ParameterizedExpression("?", ImmutableList.of(new QueryParameter(type, Optional.of(value)))));
        }

        if (type == REAL) {
            float floatValue = intBitsToFloat(toIntExact((long) value));
            if (Float.isNaN(floatValue) || Float.isInfinite(floatValue)) {
                return Optional.empty();
            }
            return Optional.of(new ParameterizedExpression("?", ImmutableList.of(new QueryParameter(type, Optional.of(value)))));
        }

        throw new UnsupportedOperationException("Unsupported type: " + type);
    }
}
