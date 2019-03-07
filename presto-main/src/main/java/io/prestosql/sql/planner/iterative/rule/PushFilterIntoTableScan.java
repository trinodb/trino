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
import io.prestosql.metadata.FilterApplicationResult;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.spi.expression.ConnectorExpression;
import io.prestosql.spi.expression.ConnectorExpressionTranslator;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.TableScanNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.plan.Patterns.filter;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.planner.plan.Patterns.tableScan;

public class PushFilterIntoTableScan
        implements Rule<FilterNode>
{
    private static final Capture<TableScanNode> TABLE_SCAN = newCapture();
    private static final Pattern<FilterNode> PATTERN = filter().with(source().matching(
            tableScan().capturedAs(TABLE_SCAN)));

    private final Metadata metadata;
    private final TypeAnalyzer typeAnalyzer;

    public PushFilterIntoTableScan(Metadata metadata, TypeAnalyzer typeAnalyzer)
    {
        this.metadata = metadata;
        this.typeAnalyzer = typeAnalyzer;
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(FilterNode filter, Captures captures, Context context)
    {
        TableScanNode tableScan = captures.get(TABLE_SCAN);

        ConnectorExpression expression = ConnectorExpressionTranslator.translate(
                context.getSession(),
                filter.getPredicate(),
                tableScan.getAssignments(),
                typeAnalyzer,
                context.getSymbolAllocator().getTypes(),
                metadata);

        Optional<FilterApplicationResult> result = metadata.applyFilter(tableScan.getTable(), expression);
        if (!result.isPresent()) {
            return Result.empty();
        }

        Map<ColumnHandle, Symbol> mappings = new HashMap<>();
        for (Map.Entry<Symbol, ColumnHandle> assignment : tableScan.getAssignments().entrySet()) {
            mappings.put(assignment.getValue(), assignment.getKey());
        }

        List<Symbol> newOutputs = new ArrayList<>();
        Map<Symbol, ColumnHandle> newAssignments = new HashMap<>();

        newOutputs.addAll(tableScan.getOutputSymbols());
        newAssignments.putAll(tableScan.getAssignments());
        for (FilterApplicationResult.Column newProjection : result.get().getNewProjections()) {
            Symbol symbol = context.getSymbolAllocator().newSymbol("column", newProjection.getType());

            mappings.put(newProjection.getColumn(), symbol);
            newOutputs.add(symbol);
            newAssignments.put(symbol, newProjection.getColumn());
        }

        return Result.ofPlanNode(
                new ProjectNode( // to preserve the schema of the transformed output
                        context.getIdAllocator().getNextId(),
                        new FilterNode(
                                filter.getId(),
                                TableScanNode.newInstance(
                                        tableScan.getId(),
                                        result.get().getTable(),
                                        newOutputs,
                                        newAssignments),
                                ConnectorExpressionTranslator.translate(result.get().getRemainingFilter(), mappings, metadata)),
                        Assignments.identity(filter.getOutputSymbols())));
    }
}
