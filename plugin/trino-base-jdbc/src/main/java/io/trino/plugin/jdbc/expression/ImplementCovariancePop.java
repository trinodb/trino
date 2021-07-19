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

import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.AggregateFunctionRule;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.expression.Variable;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.AggregateFunctionPatterns.basicAggregation;
import static io.trino.plugin.base.expression.AggregateFunctionPatterns.expressionTypes;
import static io.trino.plugin.base.expression.AggregateFunctionPatterns.functionName;
import static io.trino.plugin.base.expression.AggregateFunctionPatterns.inputs;
import static io.trino.plugin.base.expression.AggregateFunctionPatterns.variables;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static java.lang.String.format;

public class ImplementCovariancePop
        implements AggregateFunctionRule<JdbcExpression>
{
    private static final Capture<List<Variable>> INPUTS = newCapture();

    @Override
    public Pattern<AggregateFunction> getPattern()
    {
        return basicAggregation()
                .with(functionName().equalTo("covar_pop"))
                .with(inputs().matching(
                        variables()
                                .matching(expressionTypes(REAL, REAL).or(expressionTypes(DOUBLE, DOUBLE)))
                                .capturedAs(INPUTS)));
    }

    @Override
    public Optional<JdbcExpression> rewrite(AggregateFunction aggregateFunction, Captures captures, RewriteContext context)
    {
        List<Variable> inputs = captures.get(INPUTS);
        verify(inputs.size() == 2);

        JdbcColumnHandle columnHandle1 = (JdbcColumnHandle) context.getAssignment(inputs.get(0).getName());
        JdbcColumnHandle columnHandle2 = (JdbcColumnHandle) context.getAssignment(inputs.get(1).getName());
        verify(aggregateFunction.getOutputType().equals(columnHandle1.getColumnType()));

        return Optional.of(new JdbcExpression(
                format("covar_pop(%s, %s)", context.getIdentifierQuote().apply(columnHandle1.getColumnName()), context.getIdentifierQuote().apply(columnHandle2.getColumnName())),
                columnHandle1.getJdbcTypeHandle()));
    }
}
