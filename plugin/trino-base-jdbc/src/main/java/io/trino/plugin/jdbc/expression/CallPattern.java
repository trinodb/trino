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
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionUnqualifiedName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class CallPattern
        extends ExpressionPattern
{
    private final String functionName;
    private final List<ExpressionPattern> parameters;
    private final Optional<SimpleTypePattern> resultType;
    private final Pattern<Call> pattern;

    public CallPattern(String functionName, List<ExpressionPattern> parameters, Optional<SimpleTypePattern> resultType)
    {
        this.functionName = requireNonNull(functionName, "functionName is null");
        this.parameters = ImmutableList.copyOf(requireNonNull(parameters, "parameters is null"));
        this.resultType = requireNonNull(resultType, "resultType is null");

        Pattern<Call> pattern = call().with(functionUnqualifiedName().equalTo(functionName));
        if (resultType.isPresent()) {
            pattern = pattern.with(type().matching(resultType.get().getPattern()));
        }
        pattern = pattern.with(argumentCount().equalTo(parameters.size()));
        for (int i = 0; i < parameters.size(); i++) {
            pattern = pattern.with(argument(i).matching(parameters.get(i).getPattern()));
        }
        this.pattern = pattern;
    }

    @Override
    public Pattern<? extends ConnectorExpression> getPattern()
    {
        return pattern;
    }

    @Override
    public void resolve(Captures captures, MatchContext matchContext)
    {
        for (ExpressionPattern parameter : parameters) {
            parameter.resolve(captures, matchContext);
        }
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
        CallPattern that = (CallPattern) o;
        return Objects.equals(functionName, that.functionName) &&
                Objects.equals(parameters, that.parameters) &&
                Objects.equals(resultType, that.resultType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(functionName, parameters, resultType);
    }

    @Override
    public String toString()
    {
        return format(
                "%s(%s)%s",
                functionName,
                parameters.stream()
                        .map(Object::toString)
                        .collect(joining(", ")),
                resultType.map(resultType -> ": " + resultType).orElse(""));
    }
}
