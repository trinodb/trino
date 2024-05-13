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
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.expression.ArrayFieldDereference;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.NodeRef;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.SystemSessionProperties.isAllowPushdownIntoConnectors;
import static io.trino.SystemSessionProperties.isPushFieldDereferenceLambdaIntoScanEnabled;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.PartialTranslator.extractPartialTranslations;
import static io.trino.sql.planner.iterative.rule.DereferencePushdown.extractSubscriptLambdas;
import static io.trino.sql.planner.iterative.rule.DereferencePushdown.getReferences;
import static io.trino.sql.planner.plan.Patterns.filter;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.Patterns.tableScan;
import static java.util.function.Function.identity;

/**
 * This rule is similar as PushSubscriptLambdaIntoTableScan, but handles the case where filter node
 * is above table scan after predicate pushdown rules
 *
 * TODO: Remove lambda expression after subfields are pushed down
 */
public class PushFieldReferenceLambdaThroughFilterIntoTableScan
        implements Rule<ProjectNode>
{
    private static final Logger LOG = Logger.get(PushFieldReferenceLambdaThroughFilterIntoTableScan.class);
    private static final Capture<FilterNode> filter = newCapture();
    private static final Capture<TableScanNode> tablescan = newCapture();

    private final PlannerContext plannerContext;

    public PushFieldReferenceLambdaThroughFilterIntoTableScan(PlannerContext plannerContext)
    {
        this.plannerContext = plannerContext;
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return project()
                .with(source().matching(filter().capturedAs(filter)
                        .with(source().matching((tableScan().capturedAs(tablescan))))));
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isAllowPushdownIntoConnectors(session)
                && isPushFieldDereferenceLambdaIntoScanEnabled(session);
    }

    @Override
    public Result apply(ProjectNode project, Captures captures, Context context)
    {
        FilterNode filterNode = captures.get(filter);
        TableScanNode tableScanNode = captures.get(tablescan);

        Map<Call, Reference> subscriptLambdas = extractSubscriptLambdas(project.getAssignments().getExpressions());

        if (subscriptLambdas.isEmpty()) {
            return Result.empty();
        }

        // If filter has same reference as subscript input, skip for safe for now
        List<Reference> filterSymbolReferences = getReferences(filterNode.getPredicate());
        subscriptLambdas = subscriptLambdas.entrySet().stream()
                .filter(e -> !filterSymbolReferences.contains(e.getValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (subscriptLambdas.isEmpty()) {
            return Result.empty();
        }

        Session session = context.getSession();
        // Extract only ArrayFieldDereference expressions from projection expressions, other expressions have been applied
        Map<NodeRef<Expression>, ConnectorExpression> partialTranslations = subscriptLambdas.entrySet().stream()
                .flatMap(expression ->
                        extractPartialTranslations(
                                expression.getKey(),
                                session,
                                true
                        ).entrySet().stream().filter(entry -> (entry.getValue() instanceof ArrayFieldDereference)))
                .filter(entry -> !(entry.getValue() instanceof io.trino.spi.expression.Constant))
                // Avoid duplicates
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue, (first, ignore) -> first));

        if (partialTranslations.isEmpty()) {
            return Result.empty();
        }

        Map<String, Symbol> inputVariableMappings = tableScanNode.getAssignments().keySet().stream()
                .collect(toImmutableMap(Symbol::name, identity()));
        Map<String, ColumnHandle> assignments = inputVariableMappings.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> tableScanNode.getAssignments().get(entry.getValue())));

        // Apply projections handled by connectors
        Optional<ProjectionApplicationResult<TableHandle>> result =
                plannerContext.getMetadata().applyProjection(session,
                        tableScanNode.getTable(),
                        ImmutableList.copyOf(partialTranslations.values()),
                        assignments);

        if (result.isEmpty()) {
            return Result.empty();
        }

        Map<Symbol, ColumnHandle> newTableAssignments = new HashMap<>();
        for (Assignment assignment : result.get().getAssignments()) {
            newTableAssignments.put(inputVariableMappings.get(assignment.getVariable()), assignment.getColumn());
        }

        verify(assignments.size() == newTableAssignments.size(),
                "Assignments size mis-match after PushSubscriptLambdaThroughFilterIntoTableScan: %d instead of %d",
                newTableAssignments.size(),
                assignments.size());

        LOG.info("PushSubscriptLambdaThroughFilterIntoTableScan is effectively triggered on %d expressions", partialTranslations.size());

        // Only update tableHandle and TableScan assignments which have new columnHandles
        return Result.ofPlanNode(
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        new FilterNode(
                                context.getIdAllocator().getNextId(),
                                new TableScanNode(
                                        tableScanNode.getId(),
                                        result.get().getHandle(),
                                        tableScanNode.getOutputSymbols(),
                                        newTableAssignments,
                                        tableScanNode.getEnforcedConstraint(),
                                        tableScanNode.getStatistics(),
                                        tableScanNode.isUpdateTarget(),
                                        tableScanNode.getUseConnectorNodePartitioning()),
                                filterNode.getPredicate()),
                        project.getAssignments()));
    }
}
