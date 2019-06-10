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

import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TableHandle;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ProjectionApplicationResult;
import io.prestosql.spi.expression.ConnectorExpression;
import io.prestosql.spi.expression.ConnectorExpressionTranslator;
import io.prestosql.sql.planner.LiteralEncoder;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.tree.Expression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.matching.Capture.newCapture;
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
    public Result apply(ProjectNode project, Captures captures, Context context)
    {
        TableScanNode tableScan = captures.get(TABLE_SCAN);

        List<ConnectorExpression> projections;
        try {
            projections = project.getAssignments()
                    .getExpressions().stream()
                    .map(expression -> ConnectorExpressionTranslator.translate(
                            context.getSession(),
                            expression,
                            typeAnalyzer,
                            context.getSymbolAllocator().getTypes()))
                    .collect(toImmutableList());
        }
        catch (UnsupportedOperationException e) {
            // some expression not supported by translator, skip
            // TODO: Support pushing down the expressions that could be translated
            // TODO: A possible approach might be:
            //    1. For expressions that could not be translated, extract column references
            //    2. Provide those column references as part of the call to applyProjection
            //    3. Re-assemble a projection based on the new projections + un-translateble projections
            //       rewritten in terms of the new assignments for the columns passed in #2
            return Result.empty();
        }

        Map<String, ColumnHandle> assignments = tableScan.getAssignments()
                .entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getKey().getName(), Map.Entry::getValue));

        Optional<ProjectionApplicationResult<TableHandle>> result = metadata.applyProjection(context.getSession(), tableScan.getTable(), projections, assignments);
        if (!result.isPresent()) {
            return Result.empty();
        }

        List<Symbol> newScanOutputs = new ArrayList<>();
        Map<Symbol, ColumnHandle> newScanAssignments = new HashMap<>();
        Map<String, Symbol> variableMappings = new HashMap<>();
        for (ProjectionApplicationResult.Assignment assignment : result.get().getAssignments()) {
            Symbol symbol = context.getSymbolAllocator().newSymbol(assignment.getVariable(), assignment.getType());

            newScanOutputs.add(symbol);
            newScanAssignments.put(symbol, assignment.getColumn());
            variableMappings.put(assignment.getVariable(), symbol);
        }

        // TODO: ensure newProjections.size == original projections.size

        List<Expression> newProjections = result.get().getProjections().stream()
                .map(expression -> ConnectorExpressionTranslator.translate(expression, variableMappings, new LiteralEncoder(metadata.getBlockEncodingSerde())))
                .collect(toImmutableList());

        Assignments.Builder newProjectionAssignments = Assignments.builder();
        for (int i = 0; i < project.getOutputSymbols().size(); i++) {
            newProjectionAssignments.put(project.getOutputSymbols().get(i), newProjections.get(i));
        }

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
