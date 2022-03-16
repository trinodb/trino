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

import org.antlr.v4.runtime.ParserRuleContext;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;

public class ExpressionPatternBuilder
        extends ConnectorExpressionPatternBaseVisitor<Object>
{
    @Override
    public Object visitStandaloneExpression(ConnectorExpressionPatternParser.StandaloneExpressionContext context)
    {
        return visit(context.expression());
    }

    @Override
    public Object visitStandaloneType(ConnectorExpressionPatternParser.StandaloneTypeContext context)
    {
        return visit(context.type());
    }

    @Override
    public Object visitCall(ConnectorExpressionPatternParser.CallContext context)
    {
        return new CallPattern(
                visit(context.identifier(), String.class),
                visit(context.expression(), ExpressionPattern.class),
                visitIfPresent(context.type(), TypePattern.class));
    }

    @Override
    public ExpressionPattern visitExpressionCapture(ConnectorExpressionPatternParser.ExpressionCaptureContext context)
    {
        return new ExpressionCapture(
                visit(context.identifier(), String.class),
                visitIfPresent(context.type(), TypePattern.class));
    }

    @Override
    public Object visitType(ConnectorExpressionPatternParser.TypeContext context)
    {
        return new TypePattern(
                visit(context.identifier(), String.class),
                context.typeParameter().stream()
                        .map(parameter -> {
                            Object result = visit(parameter, Object.class);
                            if (result instanceof String) {
                                return new TypeParameterCapture((String) result);
                            }
                            if (result instanceof Long) {
                                return new LongTypeParameter((Long) result);
                            }
                            throw new UnsupportedOperationException(format("Unsupported parameter %s (%s) from %s", result, result.getClass(), parameter));
                        })
                        .collect(toImmutableList()));
    }

    @Override
    public Object visitNumber(ConnectorExpressionPatternParser.NumberContext context)
    {
        return Long.parseLong(context.INTEGER_VALUE().getText());
    }

    @Override
    public Object visitIdentifier(ConnectorExpressionPatternParser.IdentifierContext context)
    {
        return context.getText();
    }

    private <T> List<T> visit(List<? extends ParserRuleContext> contexts, Class<T> expected)
    {
        return contexts.stream()
                .map(context -> this.visit(context, expected))
                .collect(toImmutableList());
    }

    private <T> Optional<T> visitIfPresent(@Nullable ParserRuleContext context, Class<T> expected)
    {
        if (context == null) {
            return Optional.empty();
        }
        return Optional.of(visit(context, expected));
    }

    private <T> T visit(ParserRuleContext context, Class<T> expected)
    {
        return expected.cast(super.visit(context));
    }

    // default implementation is error-prone
    @Override
    protected Object aggregateResult(Object aggregate, Object nextResult)
    {
        if (nextResult == null) {
            throw new UnsupportedOperationException("not yet implemented");
        }
        if (aggregate == null) {
            return nextResult;
        }
        throw new UnsupportedOperationException(format("Cannot combine %s and %s", aggregate, nextResult));
    }
}
