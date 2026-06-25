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
package io.trino.sql.planner;

import com.google.inject.Inject;
import io.trino.FullConnectorSession;
import io.trino.Session;
import io.trino.operator.scalar.TryFunction;
import io.trino.spi.connector.ConnectorExpressionEvaluator;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.NullableValue;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.optimizer.IrExpressionOptimizer;

import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.base.expression.ConnectorExpressions.extractVariables;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class InternalConnectorExpressionEvaluator
        implements ConnectorExpressionEvaluator
{
    private final PlannerContext plannerContext;
    private final IrExpressionOptimizer optimizer;

    @Inject
    public InternalConnectorExpressionEvaluator(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.optimizer = plannerContext.getPartialEvaluator();
    }

    @Override
    public Prepared prepare(ConnectorSession session, ConnectorExpression expression)
    {
        return new PreparedExpression(session, expression);
    }

    private final class PreparedExpression
            implements Prepared
    {
        private final Session session;
        private final Expression irExpression;
        private final Set<Symbol> columnSymbols;

        PreparedExpression(ConnectorSession connectorSession, ConnectorExpression connectorExpression)
        {
            session = ((FullConnectorSession) requireNonNull(connectorSession, "connectorSession is null")).getSession();

            Map<String, Symbol> variableMappings = extractVariables(connectorExpression).stream()
                    .collect(toImmutableMap(
                            Variable::getName,
                            variable -> new Symbol(variable.getType(), variable.getName()),
                            // otherwise it has duplicate element issue
                            (existing, _) -> existing));

            // $engine_expression calls are translated by deserializing their payload, so the
            // resulting IR contains the wrapped predicate inlined
            irExpression = ConnectorExpressionTranslator.translate(
                    session, connectorExpression, plannerContext, variableMappings, new SymbolAllocator());

            columnSymbols = SymbolsExtractor.extractUnique(irExpression);
        }

        @Override
        public Set<String> getArguments()
        {
            return columnSymbols.stream().map(Symbol::name).collect(toImmutableSet());
        }

        @Override
        public EvaluationResult tryEvaluate(Map<String, NullableValue> bindings)
        {
            Map<Symbol, Expression> columnBindings = columnSymbols.stream()
                    .filter(symbol -> bindings.containsKey(symbol.name()))
                    .collect(toImmutableMap(
                            identity(),
                            symbol -> new Constant(symbol.type(), bindings.get(symbol.name()).getValue())));

            // Skip pruning if evaluation fails in a recoverable way. Failing here can cause
            // spurious query failures for partitions that would otherwise be filtered out.
            return TryFunction.evaluate(
                    () -> {
                        // optimizer.process returns Optional.empty() when no change was made; use original as fallback
                        Expression result = optimizer.process(irExpression, session, new SymbolAllocator(columnBindings.keySet()), columnBindings)
                                .orElse(irExpression);
                        if (!(result instanceof Constant constant)) {
                            return new EvaluationResult.NoResult();
                        }
                        return new EvaluationResult.Value(constant.value());
                    },
                    new EvaluationResult.NoResult());
        }
    }
}
