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
package io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada;

import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionPatterns;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.varada.expression.VaradaCall;
import io.trino.plugin.varada.expression.VaradaExpression;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.DateType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.expression.StandardFunctions.CAST_FUNCTION_NAME;

class CastRewriter
        implements ConnectorExpressionRule<Call, VaradaExpression>
{
    private static final Set<Class<? extends Type>> supportedCastTypes = Set.of(VarcharType.class, RealType.class, DateType.class);
    private static final Pattern<Call> PATTERN = ConnectorExpressionPatterns.call()
            .with(ConnectorExpressionPatterns.argumentCount().equalTo(1))
            .with(ConnectorExpressionPatterns.argument(0).matching(x -> x instanceof Variable))
            .with(ConnectorExpressionPatterns.type().matching(CastRewriter::isSupportedCastType));

    CastRewriter() {}

    private static boolean isSupportedCastType(Type type)
    {
        return supportedCastTypes.contains(type.getClass());
    }

    @Override
    public Pattern<Call> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<VaradaExpression> rewrite(Call expression, Captures captures, RewriteContext<VaradaExpression> context)
    {
        Optional<VaradaExpression> varadaExpression = context.defaultRewrite(expression.getChildren().get(0));
        return varadaExpression.map(value -> new VaradaCall(CAST_FUNCTION_NAME.getName(), List.of(value), expression.getType()));
    }
}
