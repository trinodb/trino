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

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.TableFunctionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.ConnectorExpressionTranslator;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.ExpressionInterpreter;
import io.trino.sql.planner.LiteralEncoder;
import io.trino.sql.planner.NoOpSymbolResolver;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableFunctionProcessorNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.SystemSessionProperties.isAllowPushdownIntoConnectors;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.ExpressionUtils.combineConjuncts;
import static io.trino.sql.planner.iterative.rule.PushPredicateIntoTableScan.computeEnforced;
import static io.trino.sql.planner.iterative.rule.PushPredicateIntoTableScan.createResultingPredicate;
import static io.trino.sql.planner.iterative.rule.PushPredicateIntoTableScan.splitExpression;
import static io.trino.sql.planner.plan.Patterns.filter;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.Patterns.tableFunctionProcessor;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class PushFilterIntoTableFunction
        implements Rule<FilterNode>
{
    private static final Capture<TableFunctionProcessorNode> TABLE_FUNCTION_PROCESSOR_NODE = newCapture();
    private static final Pattern<FilterNode> PATTERN = filter().with(source().matching(
            tableFunctionProcessor().capturedAs(TABLE_FUNCTION_PROCESSOR_NODE)));

    private final PlannerContext plannerContext;
    private final TypeAnalyzer typeAnalyzer;

    public PushFilterIntoTableFunction(PlannerContext plannerContext, TypeAnalyzer typeAnalyzer)
    {
        this.plannerContext = plannerContext;
        this.typeAnalyzer = typeAnalyzer;
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(FilterNode filterNode, Captures captures, Context context)
    {
        TableFunctionProcessorNode tableFunctionProcessorNode = captures.get(TABLE_FUNCTION_PROCESSOR_NODE);

        Optional<PlanNode> rewritten = pushFilterIntoTableFunctionProcessorNode(
                filterNode,
                tableFunctionProcessorNode,
                context.getSession(),
                context.getSymbolAllocator(),
                plannerContext,
                new DomainTranslator(plannerContext),
                typeAnalyzer);

        if (rewritten.isEmpty() || arePlansSame(filterNode, tableFunctionProcessorNode, rewritten.get())) {
            return Result.empty();
        }

        return Result.ofPlanNode(rewritten.get());
    }

    public static Optional<PlanNode> pushFilterIntoTableFunctionProcessorNode(
            FilterNode filterNode,
            TableFunctionProcessorNode node,
            Session session,
            SymbolAllocator symbolAllocator,
            PlannerContext plannerContext,
            DomainTranslator domainTranslator,
            TypeAnalyzer typeAnalyzer)
    {
        if (!isAllowPushdownIntoConnectors(session)) {
            return Optional.empty();
        }

        PushPredicateIntoTableScan.SplitExpression splitExpression = splitExpression(plannerContext, filterNode.getPredicate());
        DomainTranslator.ExtractionResult decomposedPredicate = DomainTranslator.getExtractionResult(
                plannerContext,
                session,
                splitExpression.getDeterministicPredicate(),
                symbolAllocator.getTypes());

        List<Symbol> outputSymbols = node.getOutputSymbols();

        BiMap<Integer, Symbol> assignments = HashBiMap.create(IntStream.range(0, outputSymbols.size()).boxed()
                .collect(toImmutableMap(identity(), outputSymbols::get)));

        TupleDomain<Integer> newDomain = decomposedPredicate.getTupleDomain()
                .transformKeys(assignments.inverse()::get)
                .intersect(node.getEnforcedConstraint());

        ConnectorExpressionTranslator.ConnectorExpressionTranslation expressionTranslation = ConnectorExpressionTranslator.translateConjuncts(
                session,
                decomposedPredicate.getRemainingExpression(),
                symbolAllocator.getTypes(),
                plannerContext,
                typeAnalyzer);
        ImmutableMap<String, Integer> nameToPosition = assignments.inverse().entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getKey().getName(), Map.Entry::getValue));
        Constraint<Integer> constraint = new Constraint<>(newDomain, expressionTranslation.connectorExpression(), nameToPosition);

        Optional<ConstraintApplicationResult<ConnectorTableFunctionHandle, Integer>> result = plannerContext.getMetadata().applyFilter(session, node.getHandle(), constraint);
        if (result.isEmpty()) {
            return Optional.empty();
        }

        TupleDomain<Integer> remainingFilter = result.get().getRemainingFilter();
        Optional<ConnectorExpression> remainingConnectorExpression = result.get().getRemainingExpression();

        TableFunctionProcessorNode tableFunctionProcessorNode = new TableFunctionProcessorNode(
                node.getId(),
                node.getName(),
                node.getProperOutputs(),
                node.getSource(),
                node.isPruneWhenEmpty(),
                node.getPassThroughSpecifications(),
                node.getRequiredSymbols(),
                node.getMarkerSymbols(),
                node.getSpecification(),
                node.getPrePartitioned(),
                node.getPreSorted(),
                node.getHashSymbol(),
                new TableFunctionHandle(node.getHandle().getCatalogHandle(), result.get().getHandle(), node.getHandle().getTransactionHandle()),
                computeEnforced(newDomain, remainingFilter));

        Expression remainingDecomposedPredicate;
        if (remainingConnectorExpression.isEmpty() || remainingConnectorExpression.get().equals(expressionTranslation.connectorExpression())) {
            remainingDecomposedPredicate = decomposedPredicate.getRemainingExpression();
        }
        else {
            Map<String, Symbol> variableMappings = node.getOutputSymbols().stream().collect(toMap(Symbol::getName, identity()));
            LiteralEncoder literalEncoder = new LiteralEncoder(plannerContext);
            Expression translatedExpression = ConnectorExpressionTranslator.translate(session, remainingConnectorExpression.get(), plannerContext, variableMappings, literalEncoder);
            Map<NodeRef<Expression>, Type> translatedExpressionTypes = typeAnalyzer.getTypes(session, symbolAllocator.getTypes(), translatedExpression);
            translatedExpression = literalEncoder.toExpression(
                    session,
                    new ExpressionInterpreter(translatedExpression, plannerContext, session, translatedExpressionTypes)
                            .optimize(NoOpSymbolResolver.INSTANCE),
                    translatedExpressionTypes.get(NodeRef.of(translatedExpression)));
            remainingDecomposedPredicate = combineConjuncts(plannerContext.getMetadata(), translatedExpression, expressionTranslation.remainingExpression());
        }
        Expression resultingPredicate = createResultingPredicate(
                plannerContext,
                session,
                symbolAllocator,
                typeAnalyzer,
                splitExpression.getDynamicFilter(),
                domainTranslator.toPredicate(session, remainingFilter.transformKeys(assignments::get)),
                splitExpression.getNonDeterministicPredicate(),
                remainingDecomposedPredicate);

        if (!TRUE_LITERAL.equals(resultingPredicate)) {
            return Optional.of(new FilterNode(filterNode.getId(), tableFunctionProcessorNode, resultingPredicate));
        }
        return Optional.of(tableFunctionProcessorNode);
    }

    private boolean arePlansSame(FilterNode filter, TableFunctionProcessorNode tableFunctionProcessorNode, PlanNode rewritten)
    {
        if (!(rewritten instanceof FilterNode rewrittenFilter)) {
            return false;
        }

        if (!Objects.equals(filter.getPredicate(), rewrittenFilter.getPredicate())) {
            return false;
        }

        if (!(rewrittenFilter.getSource() instanceof TableFunctionProcessorNode rewrittenTableFunctionProcessorNode)) {
            return false;
        }

        return Objects.equals(tableFunctionProcessorNode.getEnforcedConstraint(), rewrittenTableFunctionProcessorNode.getEnforcedConstraint()) &&
                Objects.equals(tableFunctionProcessorNode.getHandle(), rewrittenTableFunctionProcessorNode.getHandle());
    }
}
