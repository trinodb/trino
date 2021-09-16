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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.spi.type.Type;
import io.trino.sql.planner.ExpressionInterpreter;
import io.trino.sql.planner.LiteralEncoder;
import io.trino.sql.planner.NoOpSymbolResolver;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.SymbolReference;

import java.util.Map;
import java.util.Set;

import static io.trino.sql.planner.iterative.rule.ExtractCommonPredicatesExpressionRewriter.extractCommonPredicates;
import static io.trino.sql.planner.iterative.rule.PushDownNegationsExpressionRewriter.pushDownNegations;
import static java.util.Objects.requireNonNull;

public class SimplifyExpressions
        extends ExpressionRewriteRuleSet
{
    public static Expression rewrite(Expression expression, Session session, SymbolAllocator symbolAllocator, Metadata metadata, LiteralEncoder literalEncoder, TypeAnalyzer typeAnalyzer)
    {
        requireNonNull(metadata, "metadata is null");
        requireNonNull(typeAnalyzer, "typeAnalyzer is null");
        if (expression instanceof SymbolReference) {
            return expression;
        }
        Map<NodeRef<Expression>, Type> expressionTypes = typeAnalyzer.getTypes(session, symbolAllocator.getTypes(), expression);
        expression = pushDownNegations(metadata, expression, expressionTypes);
        expression = extractCommonPredicates(metadata, expression);
        expressionTypes = typeAnalyzer.getTypes(session, symbolAllocator.getTypes(), expression);
        ExpressionInterpreter interpreter = new ExpressionInterpreter(expression, metadata, session, expressionTypes);
        Object optimized = interpreter.optimize(NoOpSymbolResolver.INSTANCE);
        return literalEncoder.toExpression(optimized, expressionTypes.get(NodeRef.of(expression)));
    }

    public SimplifyExpressions(Metadata metadata, TypeAnalyzer typeAnalyzer)
    {
        super(createRewrite(metadata, typeAnalyzer));
    }

    @Override
    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                projectExpressionRewrite(),
                filterExpressionRewrite(),
                joinExpressionRewrite(),
                valuesExpressionRewrite()); // ApplyNode and AggregationNode are not supported, because ExpressionInterpreter doesn't support them
    }

    private static ExpressionRewriter createRewrite(Metadata metadata, TypeAnalyzer typeAnalyzer)
    {
        requireNonNull(metadata, "metadata is null");
        requireNonNull(typeAnalyzer, "typeAnalyzer is null");
        LiteralEncoder literalEncoder = new LiteralEncoder(metadata);

        return (expression, context) -> rewrite(expression, context.getSession(), context.getSymbolAllocator(), metadata, literalEncoder, typeAnalyzer);
    }
}
