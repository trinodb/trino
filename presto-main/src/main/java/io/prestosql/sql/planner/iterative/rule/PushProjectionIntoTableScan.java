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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TableHandle;
import io.prestosql.spi.connector.Assignment;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ProjectionApplicationResult;
import io.prestosql.spi.expression.ConnectorExpression;
import io.prestosql.sql.planner.ConnectorExpressionTranslator;
import io.prestosql.sql.planner.LiteralEncoder;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.NodeRef;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.SystemSessionProperties.isAllowPushdownIntoConnectors;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.PartialTranslator.extractPartialTranslations;
import static io.prestosql.sql.planner.ReferenceAwareExpressionNodeInliner.replaceExpression;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.planner.plan.Patterns.tableScan;

public class PushProjectionIntoTableScan
        implements Rule<ProjectNode>
{
    private static final Capture<TableScanNode> TABLE_SCAN = newCapture();
    private static final Pattern<ProjectNode> PATTERN = project().with(source().matching(
            tableScan().capturedAs(TABLE_SCAN)));

    private final Metadata metadata;
    private final TypeAnalyzer typeAnalyzer;

    public PushProjectionIntoTableScan(Metadata metadata, TypeAnalyzer typeAnalyzer)
    {
        this.metadata = metadata;
        this.typeAnalyzer = typeAnalyzer;
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

        Map<Symbol, Expression> inputExpressions = project.getAssignments().getMap();

        ImmutableList.Builder<NodeRef<Expression>> nodeReferencesBuilder = ImmutableList.builder();
        ImmutableList.Builder<ConnectorExpression> partialProjectionsBuilder = ImmutableList.builder();

        // Extract translatable components from projection expressions. Prepare a mapping from these internal
        // expression nodes to corresponding ConnectorExpression translations.
        for (Map.Entry<Symbol, Expression> expression : inputExpressions.entrySet()) {
            Map<NodeRef<Expression>, ConnectorExpression> partialTranslations = extractPartialTranslations(
                    expression.getValue(),
                    context.getSession(),
                    typeAnalyzer,
                    context.getSymbolAllocator().getTypes());

            partialTranslations.forEach((nodeRef, expr) -> {
                nodeReferencesBuilder.add(nodeRef);
                partialProjectionsBuilder.add(expr);
            });
        }

        List<NodeRef<Expression>> nodesForPartialProjections = nodeReferencesBuilder.build();
        List<ConnectorExpression> connectorPartialProjections = partialProjectionsBuilder.build();

        Map<String, ColumnHandle> assignments = tableScan.getAssignments()
                .entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getKey().getName(), Map.Entry::getValue));

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

        return Result.ofPlanNode(
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        TableScanNode.newInstance(
                                tableScan.getId(),
                                result.get().getHandle(),
                                newScanOutputs,
                                newScanAssignments),
                        newProjectionAssignments.build()));
    }
}
