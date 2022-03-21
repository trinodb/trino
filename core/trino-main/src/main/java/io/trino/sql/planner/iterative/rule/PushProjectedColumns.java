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

import io.trino.Session;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.trino.SystemSessionProperties.isAllowPushdownIntoConnectors;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.plan.Patterns.filter;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.Patterns.tableScan;
import static java.util.Objects.requireNonNull;

/**
 *  If the query has a filter, push the final (after prune) projected columns into connector
 */
public class PushProjectedColumns
        implements Rule<ProjectNode>
{
    private static final Capture<TableScanNode> TABLE_SCAN = newCapture();
    private static final Capture<FilterNode> FILTER = newCapture();
    private static final Pattern<ProjectNode> PATTERN = project()
            .with(source().matching(filter().capturedAs(FILTER)
                    .with(source().matching(tableScan().capturedAs(TABLE_SCAN)))));

    private final Metadata metadata;

    public PushProjectedColumns(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
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
    public Result apply(ProjectNode projectNode, Captures captures, Context context)
    {
        TableScanNode tableScan = captures.get(TABLE_SCAN);
        FilterNode filter = captures.get(FILTER);

        Set<ColumnHandle> columns = projectNode.getAssignments().getExpressions().stream()
                .flatMap(expression -> SymbolsExtractor.extractAll(expression).stream())
                .map(symbol -> tableScan.getAssignments().get(symbol))
                .collect(Collectors.toSet());

        Optional<TableHandle> result = metadata.applyProjectedColumns(context.getSession(), tableScan.getTable(), columns);

        if (result.isEmpty()) {
            return Result.empty();
        }

        TableScanNode newTableScan = new TableScanNode(
                tableScan.getId(),
                result.get(),
                tableScan.getOutputSymbols(),
                tableScan.getAssignments(),
                tableScan.getEnforcedConstraint(),
                tableScan.getStatistics(),
                tableScan.isUpdateTarget(),
                tableScan.getUseConnectorNodePartitioning());
        FilterNode newFilterNode = new FilterNode(filter.getId(), newTableScan, filter.getPredicate());
        ProjectNode newProjectNode = new ProjectNode(projectNode.getId(), newFilterNode, projectNode.getAssignments());
        return Result.ofPlanNode(newProjectNode);
    }
}
