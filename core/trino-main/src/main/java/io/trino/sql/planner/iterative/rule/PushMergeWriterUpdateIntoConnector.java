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

import com.google.common.collect.ImmutableMap;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.ConnectorExpressionTranslator;
import io.trino.sql.planner.IrTypeAnalyzer;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.MergeProcessorNode;
import io.trino.sql.planner.plan.MergeWriterNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableFinishNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TableUpdateNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.SymbolReference;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.plan.Patterns.mergeProcessor;
import static io.trino.sql.planner.plan.Patterns.mergeWriter;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.Patterns.tableFinish;
import static io.trino.sql.planner.plan.Patterns.tableScan;
import static java.util.Objects.requireNonNull;

/**
 * This version support only constant updates
 * and fall back to default behaviour in all other cases
 */
public class PushMergeWriterUpdateIntoConnector
        implements Rule<TableFinishNode>
{
    private static final Capture<MergeWriterNode> MERGE_WRITER_NODE_CAPTURE = newCapture();
    private static final Capture<MergeProcessorNode> MERGE_PROCESSOR_NODE_CAPTURE = newCapture();
    private static final Capture<TableScanNode> TABLE_SCAN = newCapture();
    private static final Capture<ProjectNode> PROJECT_NODE_CAPTURE = newCapture();

    private static final Pattern<TableFinishNode> PATTERN =
            tableFinish().with(source().matching(
                    mergeWriter().capturedAs(MERGE_WRITER_NODE_CAPTURE).with(source().matching(
                            mergeProcessor().capturedAs(MERGE_PROCESSOR_NODE_CAPTURE).with(source().matching(
                                    project().capturedAs(PROJECT_NODE_CAPTURE).with(source().matching(
                                            tableScan().capturedAs(TABLE_SCAN)))))))));

    public PushMergeWriterUpdateIntoConnector(PlannerContext plannerContext, IrTypeAnalyzer typeAnalyzer, Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    }

    private final Metadata metadata;
    private final PlannerContext plannerContext;
    private final IrTypeAnalyzer typeAnalyzer;

    @Override
    public Pattern<TableFinishNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(TableFinishNode node, Captures captures, Context context)
    {
        MergeWriterNode mergeWriter = captures.get(MERGE_WRITER_NODE_CAPTURE);
        MergeProcessorNode mergeProcessor = captures.get(MERGE_PROCESSOR_NODE_CAPTURE);
        ProjectNode project = captures.get(PROJECT_NODE_CAPTURE);
        TableScanNode tableScan = captures.get(TABLE_SCAN);

        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(context.getSession(), mergeWriter.getTarget().getHandle());
        List<String> orderedColumnNames = mergeWriter.getTarget().getMergeParadigmAndTypes().getColumnNames();
        List<? extends Node> mergeAssignments = project.getAssignments().get(mergeProcessor.getMergeRowSymbol()).getChildren();

        Map<ColumnHandle, Constant> assignments = buildAssignments(orderedColumnNames, mergeAssignments, columnHandles, context);
        if (assignments.isEmpty()) {
            return Result.empty();
        }

        return metadata.applyUpdate(context.getSession(), tableScan.getTable(), assignments)
                .map(newHandle -> new TableUpdateNode(
                        context.getIdAllocator().getNextId(),
                        newHandle,
                        getOnlyElement(node.getOutputSymbols())))
                .map(Result::ofPlanNode)
                .orElseGet(Result::empty);
    }

    private Map<ColumnHandle, Constant> buildAssignments(
            List<String> orderedColumnNames,
            List<? extends Node> mergeAssignments,
            Map<String, ColumnHandle> columnHandles,
            Context context)
    {
        ImmutableMap.Builder<ColumnHandle, Constant> assignmentsBuilder = ImmutableMap.builder();
        for (int i = 0; i < orderedColumnNames.size(); i++) {
            String columnName = orderedColumnNames.get(i);
            Node assigmentNode = mergeAssignments.get(i);
            if (assigmentNode instanceof SymbolReference) {
                // the column is not updated
                continue;
            }

            Optional<ConnectorExpression> connectorExpression = ConnectorExpressionTranslator.translate(
                    context.getSession(),
                    ((Expression) assigmentNode),
                    context.getSymbolAllocator().getTypes(),
                    plannerContext,
                    typeAnalyzer);

            // we don't support any expressions in update statements yet, only constants
            if (connectorExpression.isEmpty() || !(connectorExpression.get() instanceof Constant)) {
                return ImmutableMap.of();
            }
            assignmentsBuilder.put(columnHandles.get(columnName), (Constant) connectorExpression.get());
        }

        return assignmentsBuilder.buildOrThrow();
    }
}
