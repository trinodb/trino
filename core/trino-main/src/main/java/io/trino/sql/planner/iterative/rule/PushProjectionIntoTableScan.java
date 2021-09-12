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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.ScalarStatsCalculator;
import io.trino.cost.SymbolStatsEstimate;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableProperties.TablePartitioning;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.ConnectorExpressionTranslator;
import io.trino.sql.planner.LiteralEncoder;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.SystemSessionProperties.isAllowPushdownIntoConnectors;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.PartialTranslator.extractPartialTranslations;
import static io.trino.sql.planner.ReferenceAwareExpressionNodeInliner.replaceExpression;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.Patterns.tableScan;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class PushProjectionIntoTableScan
        implements Rule<ProjectNode>
{
    private static final Capture<TableScanNode> TABLE_SCAN = newCapture();
    private static final Pattern<ProjectNode> PATTERN = project().with(source().matching(
            tableScan().capturedAs(TABLE_SCAN)));

    private final Metadata metadata;
    private final TypeAnalyzer typeAnalyzer;
    private final ScalarStatsCalculator scalarStatsCalculator;

    public PushProjectionIntoTableScan(Metadata metadata, TypeAnalyzer typeAnalyzer, ScalarStatsCalculator scalarStatsCalculator)
    {
        this.metadata = metadata;
        this.typeAnalyzer = typeAnalyzer;
        this.scalarStatsCalculator = requireNonNull(scalarStatsCalculator, "scalarStatsCalculator is null");
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isAllowPushdownIntoConnectors(session);
    }

    @Override
    public Result apply(ProjectNode project, Captures captures, Context context)
    {
        TableScanNode tableScan = captures.get(TABLE_SCAN);

        // Extract translatable components from projection expressions. Prepare a mapping from these internal
        // expression nodes to corresponding ConnectorExpression translations.
        Map<NodeRef<Expression>, ConnectorExpression> partialTranslations = project.getAssignments().getMap().entrySet().stream()
                .flatMap(expression ->
                        extractPartialTranslations(
                                expression.getValue(),
                                context.getSession(),
                                typeAnalyzer,
                                context.getSymbolAllocator().getTypes()).entrySet().stream())
                // Avoid duplicates
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue, (first, ignore) -> first));

        List<NodeRef<Expression>> nodesForPartialProjections = ImmutableList.copyOf(partialTranslations.keySet());
        List<ConnectorExpression> connectorPartialProjections = ImmutableList.copyOf(partialTranslations.values());

        Map<String, Symbol> inputVariableMappings = tableScan.getAssignments().keySet().stream()
                .collect(toImmutableMap(Symbol::getName, identity()));
        Map<String, ColumnHandle> assignments = inputVariableMappings.entrySet().stream()
                .collect(toImmutableMap(Entry::getKey, entry -> tableScan.getAssignments().get(entry.getValue())));

        Optional<ProjectionApplicationResult<TableHandle>> result = metadata.applyProjection(context.getSession(), tableScan.getTable(), connectorPartialProjections, assignments);

        if (result.isEmpty()) {
            return Result.empty();
        }

        List<ConnectorExpression> newConnectorPartialProjections = result.get().getProjections();
        checkState(newConnectorPartialProjections.size() == connectorPartialProjections.size(),
                "Mismatch between input and output projections from the connector: expected %s but got %s",
                connectorPartialProjections.size(),
                newConnectorPartialProjections.size());

        List<Symbol> newScanOutputs = new ArrayList<>();
        Map<Symbol, ColumnHandle> newScanAssignments = new HashMap<>();
        Map<String, Symbol> variableMappings = new HashMap<>();
        for (Assignment assignment : result.get().getAssignments()) {
            Symbol symbol = context.getSymbolAllocator().newSymbol(assignment.getVariable(), assignment.getType());

            newScanOutputs.add(symbol);
            newScanAssignments.put(symbol, assignment.getColumn());
            variableMappings.put(assignment.getVariable(), symbol);
        }

        // Translate partial connector projections back to new partial projections
        List<Expression> newPartialProjections = newConnectorPartialProjections.stream()
                .map(expression -> ConnectorExpressionTranslator.translate(expression, variableMappings, new LiteralEncoder(metadata)))
                .collect(toImmutableList());

        // Map internal node references to new partial projections
        ImmutableMap.Builder<NodeRef<Expression>, Expression> nodesToNewPartialProjectionsBuilder = ImmutableMap.builder();
        for (int i = 0; i < nodesForPartialProjections.size(); i++) {
            nodesToNewPartialProjectionsBuilder.put(nodesForPartialProjections.get(i), newPartialProjections.get(i));
        }
        Map<NodeRef<Expression>, Expression> nodesToNewPartialProjections = nodesToNewPartialProjectionsBuilder.build();

        // Stitch partial translations to form new complete projections
        Assignments.Builder newProjectionAssignments = Assignments.builder();
        project.getAssignments().entrySet().forEach(entry -> {
            newProjectionAssignments.put(entry.getKey(), replaceExpression(entry.getValue(), nodesToNewPartialProjections));
        });

        Optional<PlanNodeStatsEstimate> newStatistics = tableScan.getStatistics().map(statistics -> {
            PlanNodeStatsEstimate.Builder builder = PlanNodeStatsEstimate.builder();
            builder.setOutputRowCount(statistics.getOutputRowCount());

            for (int i = 0; i < connectorPartialProjections.size(); i++) {
                ConnectorExpression inputConnectorExpression = connectorPartialProjections.get(i);
                ConnectorExpression resultConnectorExpression = newConnectorPartialProjections.get(i);
                if (!(resultConnectorExpression instanceof Variable)) {
                    continue;
                }
                String resultVariableName = ((Variable) resultConnectorExpression).getName();
                Expression inputExpression = ConnectorExpressionTranslator.translate(inputConnectorExpression, inputVariableMappings, new LiteralEncoder(metadata));
                SymbolStatsEstimate symbolStatistics = scalarStatsCalculator.calculate(inputExpression, statistics, context.getSession(), context.getSymbolAllocator().getTypes());
                builder.addSymbolStatistics(variableMappings.get(resultVariableName), symbolStatistics);
            }
            return builder.build();
        });

        verifyTablePartitioning(context, tableScan, result.get().getHandle());
        return Result.ofPlanNode(
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        new TableScanNode(
                                tableScan.getId(),
                                result.get().getHandle(),
                                newScanOutputs,
                                newScanAssignments,
                                TupleDomain.all(),
                                newStatistics,
                                tableScan.isUpdateTarget(),
                                tableScan.getUseConnectorNodePartitioning()),
                        newProjectionAssignments.build()));
    }

    // PushProjectionIntoTableScan might be executed after AddExchanges and DetermineTableScanNodePartitioning.
    // In that case, table scan node partitioning (if present) was used to fragment plan with ExchangeNodes.
    // Therefore table scan node partitioning should not change after AddExchanges is executed since it would
    // make plan with ExchangeNodes invalid.
    private void verifyTablePartitioning(
            Context context,
            TableScanNode oldTableScan,
            TableHandle newTable)
    {
        if (oldTableScan.getUseConnectorNodePartitioning().isEmpty()) {
            return;
        }

        Optional<TablePartitioning> oldTablePartitioning = metadata.getTableProperties(context.getSession(), oldTableScan.getTable()).getTablePartitioning();
        Optional<TablePartitioning> newTablePartitioning = metadata.getTableProperties(context.getSession(), newTable).getTablePartitioning();
        verify(newTablePartitioning.equals(oldTablePartitioning), "Partitioning must not change after projection is pushed down");
    }
}
