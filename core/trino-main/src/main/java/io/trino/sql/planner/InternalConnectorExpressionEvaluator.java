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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.trino.FullConnectorSession;
import io.trino.Session;
import io.trino.operator.scalar.TryFunction;
import io.trino.spi.connector.ConnectorExpressionEvaluator;
import io.trino.spi.connector.ConnectorExpressionEvaluator.EvaluationResult;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.NullableValue;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.optimizer.IrExpressionOptimizer;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.base.expression.ConnectorExpressions.extractVariables;
import static io.trino.sql.planner.EngineExpressions.ENGINE_EXPRESSION_FUNCTION_NAME;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class InternalConnectorExpressionEvaluator
        implements ConnectorExpressionEvaluator
{
    private final PlannerContext plannerContext;
    private final IrExpressionOptimizer optimizer;
    private final JsonCodec<Expression> serializer;

    @Inject
    public InternalConnectorExpressionEvaluator(PlannerContext plannerContext, JsonCodec<Expression> serializer)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.optimizer = plannerContext.getPartialEvaluator();
        this.serializer = requireNonNull(serializer, "serializer is null");
    }

    @Override
    public Prepared prepare(ConnectorExpression expression, ConnectorSession session)
    {
        return new PreparedExpression(expression, session);
    }

    private record RewriteResult(
            ConnectorExpression expression,
            Map<Symbol, Expression> engineExpressionBindings) {}

    /**
     * Walks {@code expression} recursively. When a {@code $engine_expression(payload, ...)} call
     * is found, replaces it with a synthetic {@link Variable} and deserializes the payload into an
     * IR {@link Expression}. Returns the rewritten expression together with a map from each
     * synthetic symbol to its deserialized IR expression.
     */
    private RewriteResult rewriteEngineExpressions(ConnectorExpression expression, AtomicInteger engineExpressionId)
    {
        if (expression instanceof Call call
                && call.getFunctionName().equals(ENGINE_EXPRESSION_FUNCTION_NAME)) {
            Slice payload = (Slice) requireNonNull(
                    ((io.trino.spi.expression.Constant) call.getArguments().getFirst()).getValue(),
                    "engine expression payload is null");
            Expression engineExpression = serializer.fromJson(payload.toStringUtf8());
            Symbol synthetic = new Symbol(call.getType(), "$engine_expression_" + engineExpressionId.getAndIncrement());
            return new RewriteResult(
                    new Variable(synthetic.name(), synthetic.type()),
                    ImmutableMap.of(synthetic, engineExpression));
        }
        if (expression instanceof Call call) {
            Map<Symbol, Expression> engineExpressionBindings = new LinkedHashMap<>();
            List<ConnectorExpression> rewrittenArguments = call.getArguments().stream()
                    .map(argument -> {
                        RewriteResult result = rewriteEngineExpressions(argument, engineExpressionId);
                        engineExpressionBindings.putAll(result.engineExpressionBindings());
                        return result.expression();
                    })
                    .collect(toImmutableList());
            return new RewriteResult(
                    new Call(call.getType(), call.getFunctionName(), rewrittenArguments),
                    ImmutableMap.copyOf(engineExpressionBindings));
        }
        return new RewriteResult(expression, ImmutableMap.of());
    }

    private final class PreparedExpression
            implements Prepared
    {
        private final Session session;
        private final Expression irExpression;
        private final Map<Symbol, Expression> engineExpressionBindings;
        private final Set<Symbol> columnSymbols;

        PreparedExpression(ConnectorExpression connectorExpression, ConnectorSession connectorSession)
        {
            session = ((FullConnectorSession) requireNonNull(connectorSession, "connectorSession is null")).getSession();

            RewriteResult rewritten = rewriteEngineExpressions(connectorExpression, new AtomicInteger());
            engineExpressionBindings = rewritten.engineExpressionBindings();

            Map<String, Symbol> variableMappings = extractVariables(rewritten.expression()).stream()
                    .collect(toImmutableMap(
                            Variable::getName,
                            variable -> new Symbol(variable.getType(), variable.getName()),
                            // otherwise it has duplicate element issue
                            (existing, _) -> existing));

            irExpression = ConnectorExpressionTranslator.translate(
                    session, rewritten.expression(), plannerContext, variableMappings);

            Set<Symbol> syntheticSymbols = engineExpressionBindings.keySet();
            columnSymbols = ImmutableSet.<Symbol>builder()
                    .addAll(Sets.difference(SymbolsExtractor.extractUnique(irExpression), syntheticSymbols))
                    .addAll(engineExpressionBindings.values().stream()
                            .flatMap(engineExpression -> SymbolsExtractor.extractUnique(engineExpression).stream())
                            .iterator())
                    .build();
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

            Map<Symbol, Expression> allBindings = ImmutableMap.<Symbol, Expression>builder()
                    .putAll(columnBindings)
                    .putAll(engineExpressionBindings)
                    .buildOrThrow();

            // Skip pruning if evaluation fails in a recoverable way. Failing here can cause
            // spurious query failures for partitions that would otherwise be filtered out.
            return TryFunction.evaluate(
                    () -> {
                        // optimizer.process returns Optional.empty() when no change was made; use original as fallback
                        Expression result = optimizer.process(irExpression, session, allBindings)
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
