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

import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;

import java.util.Objects;
import java.util.Optional;

import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.arguments;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionUnqualifiedName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static java.lang.String.format;

public class VariableLengthCallPattern
        extends ExpressionPattern
{
    private final String functionName;
    private final ExpressionListPattern variableLengthParameters;
    private final Optional<TypePattern> resultType;
    private final Pattern<Call> pattern;

    public VariableLengthCallPattern(String functionName,
                                     ExpressionListPattern variableLengthParameters,
                                     Optional<TypePattern> resultType)
    {
        this.functionName = functionName;
        this.variableLengthParameters = variableLengthParameters;
        this.resultType = resultType;

        Pattern<Call> pattern = call().with(functionUnqualifiedName().equalTo(functionName));
        if (resultType.isPresent()) {
            pattern = pattern.with(type().matching(resultType.get().getPattern()));
        }
        this.pattern = pattern.with(arguments().matching(variableLengthParameters.getPattern()));
    }

    @Override
    public Pattern<? extends ConnectorExpression> getPattern()
    {
        return pattern;
    }

    @Override
    public void resolve(Captures captures, MatchContext matchContext)
    {
        variableLengthParameters.resolve(captures, matchContext);
        resultType.ifPresent(resultType -> resultType.resolve(captures, matchContext));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VariableLengthCallPattern that = (VariableLengthCallPattern) o;
        return Objects.equals(functionName, that.functionName) &&
                Objects.equals(variableLengthParameters, that.variableLengthParameters) &&
                Objects.equals(resultType, that.resultType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(functionName, variableLengthParameters, resultType);
    }

    @Override
    public String toString()
    {
        return format(
                "%s(...arguments)%s",
                functionName,
                resultType.map(resultType -> ": " + resultType).orElse(""));
    }
}
