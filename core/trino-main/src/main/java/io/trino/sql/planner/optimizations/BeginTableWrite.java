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
package io.trino.sql.planner.optimizations;

import io.trino.Session;
import io.trino.cost.StatsAndCosts;
import io.trino.cost.TableStatsProvider;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.MergeHandle;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableExecuteHandle;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.BeginTableExecuteResult;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.MergeWriterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.SimplePlanRewriter;
import io.trino.sql.planner.plan.SimplePlanRewriter.RewriteContext;
import io.trino.sql.planner.plan.StatisticsWriterNode;
import io.trino.sql.planner.plan.TableExecuteNode;
import io.trino.sql.planner.plan.TableFinishNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.sql.planner.plan.TableWriterNode.CreateReference;
import io.trino.sql.planner.plan.TableWriterNode.CreateTarget;
import io.trino.sql.planner.plan.TableWriterNode.InsertReference;
import io.trino.sql.planner.plan.TableWriterNode.InsertTarget;
import io.trino.sql.planner.plan.TableWriterNode.MergeTarget;
import io.trino.sql.planner.plan.TableWriterNode.TableExecuteTarget;
import io.trino.sql.planner.plan.TableWriterNode.WriterTarget;
import io.trino.sql.planner.plan.UnionNode;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.sql.planner.planprinter.PlanPrinter.textLogicalPlan;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

/*
 * Major HACK alert!!!
 *
 * This logic should be invoked on query start, not during planning. At that point, the token
 * returned by beginCreate/beginInsert should be handed down to tasks in a mapping separate
 * from the plan that links plan nodes to the corresponding token.
 */
public class BeginTableWrite
        implements PlanOptimizer
{
    private final Metadata metadata;
    private final FunctionManager functionManager;

    public BeginTableWrite(Metadata metadata, FunctionManager functionManager)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector, TableStatsProvider tableStatsProvider)
    {
        try {
            return SimplePlanRewriter.rewriteWith(new Rewriter(session), plan, Optional.empty());
        }
        catch (RuntimeException e) {
            try {
                int nestLevel = 4; // so that it renders reasonably within exception stacktrace
                String explain = textLogicalPlan(plan, types, metadata, functionManager, StatsAndCosts.empty(), session, nestLevel, false);
                e.addSuppressed(new Exception("Current plan:\n" + explain));
            }
            catch (RuntimeException ignore) {
                // ignored
            }
            throw e;
        }
    }

    private class Rewriter
            extends SimplePlanRewriter<Optional<WriterTarget>>
    {
        private final Session session;

        public Rewriter(Session session)
        {
            this.session = session;
        }

        @Override
        public PlanNode visitTableWriter(TableWriterNode node, RewriteContext<Optional<WriterTarget>> context)
        {
            // Part of the plan should be an Optional<StateChangeListener<QueryState>> and this
            // callback can create the table and abort the table creation if the query fails.

            WriterTarget writerTarget = getContextTarget(context);
            return new TableWriterNode(
                    node.getId(),
                    context.rewrite(node.getSource(), context.get()),
                    writerTarget,
                    node.getRowCountSymbol(),
                    node.getFragmentSymbol(),
                    node.getColumns(),
                    node.getColumnNames(),
                    node.getPartitioningScheme(),
                    node.getPreferredPartitioningScheme(),
                    node.getStatisticsAggregation(),
                    node.getStatisticsAggregationDescriptor());
        }

        @Override
        public PlanNode visitTableExecute(TableExecuteNode node, RewriteContext<Optional<WriterTarget>> context)
        {
            TableExecuteTarget tableExecuteTarget = (TableExecuteTarget) getContextTarget(context);
            return new TableExecuteNode(
                    node.getId(),
                    rewriteModifyTableScan(node.getSource(), tableExecuteTarget.getSourceHandle().orElseThrow(), false),
                    tableExecuteTarget,
                    node.getRowCountSymbol(),
                    node.getFragmentSymbol(),
                    node.getColumns(),
                    node.getColumnNames(),
                    node.getPartitioningScheme(),
                    node.getPreferredPartitioningScheme());
        }

        @Override
        public PlanNode visitMergeWriter(MergeWriterNode mergeNode, RewriteContext<Optional<WriterTarget>> context)
        {
            MergeTarget mergeTarget = (MergeTarget) getContextTarget(context);
            return new MergeWriterNode(
                    mergeNode.getId(),
                    rewriteModifyTableScan(mergeNode.getSource(), mergeTarget.getHandle(), true),
                    mergeTarget,
                    mergeNode.getProjectedSymbols(),
                    mergeNode.getPartitioningScheme(),
                    mergeNode.getOutputSymbols());
        }

        @Override
        public PlanNode visitStatisticsWriterNode(StatisticsWriterNode node, RewriteContext<Optional<WriterTarget>> context)
        {
            PlanNode child = node.getSource();
            child = context.rewrite(child, context.get());

            StatisticsWriterNode.WriteStatisticsHandle analyzeHandle =
                    new StatisticsWriterNode.WriteStatisticsHandle(metadata.beginStatisticsCollection(session, ((StatisticsWriterNode.WriteStatisticsReference) node.getTarget()).getHandle()));

            return new StatisticsWriterNode(
                    node.getId(),
                    child,
                    analyzeHandle,
                    node.getRowCountSymbol(),
                    node.isRowCountEnabled(),
                    node.getDescriptor());
        }

        @Override
        public PlanNode visitTableFinish(TableFinishNode node, RewriteContext<Optional<WriterTarget>> context)
        {
            PlanNode child = node.getSource();

            WriterTarget originalTarget = getWriterTarget(child);
            WriterTarget newTarget = createWriterTarget(originalTarget);

            child = context.rewrite(child, Optional.of(newTarget));

            return new TableFinishNode(
                    node.getId(),
                    child,
                    newTarget,
                    node.getRowCountSymbol(),
                    node.getStatisticsAggregation(),
                    node.getStatisticsAggregationDescriptor());
        }

        public WriterTarget getWriterTarget(PlanNode node)
        {
            if (node instanceof TableWriterNode) {
                return ((TableWriterNode) node).getTarget();
            }
            if (node instanceof TableExecuteNode) {
                TableExecuteTarget target = ((TableExecuteNode) node).getTarget();
                return new TableExecuteTarget(
                        target.getExecuteHandle(),
                        findTableScanHandleForTableExecute(((TableExecuteNode) node).getSource()),
                        target.getSchemaTableName(),
                        target.isReportingWrittenBytesSupported());
            }

            if (node instanceof MergeWriterNode mergeWriterNode) {
                MergeTarget mergeTarget = mergeWriterNode.getTarget();
                Optional<TableHandle> tableHandle = findTableScanHandleForMergeWriter(mergeWriterNode.getSource());
                if (tableHandle.isEmpty()) {
                    // The table scan was eliminated by constant folding.  But since it was eliminated, we
                    // won't ever need to access the table, so returning the existing mergeTarget works.
                    return mergeTarget;
                }
                return new MergeTarget(
                        tableHandle.get(),
                        mergeTarget.getMergeHandle(),
                        mergeTarget.getSchemaTableName(),
                        mergeTarget.getMergeParadigmAndTypes());
            }

            if (node instanceof ExchangeNode || node instanceof UnionNode) {
                Set<WriterTarget> writerTargets = node.getSources().stream()
                        .map(this::getWriterTarget)
                        .collect(toSet());
                return getOnlyElement(writerTargets);
            }
            throw new IllegalArgumentException("Invalid child for TableCommitNode: " + node.getClass().getSimpleName());
        }

        private WriterTarget createWriterTarget(WriterTarget target)
        {
            // TODO: begin these operations in pre-execution step, not here
            // TODO: we shouldn't need to store the schemaTableName in the handles, but there isn't a good way to pass this around with the current architecture
            if (target instanceof CreateReference create) {
                return new CreateTarget(
                        metadata.beginCreateTable(session, create.getCatalog(), create.getTableMetadata(), create.getLayout()),
                        create.getTableMetadata().getTable(),
                        target.supportsReportingWrittenBytes(metadata, session),
                        target.supportsMultipleWritersPerPartition(metadata, session),
                        target.getMaxWriterTasks(metadata, session));
            }
            if (target instanceof InsertReference insert) {
                return new InsertTarget(
                        metadata.beginInsert(session, insert.getHandle(), insert.getColumns()),
                        metadata.getTableMetadata(session, insert.getHandle()).getTable(),
                        target.supportsReportingWrittenBytes(metadata, session),
                        target.supportsMultipleWritersPerPartition(metadata, session),
                        target.getMaxWriterTasks(metadata, session));
            }
            if (target instanceof MergeTarget merge) {
                MergeHandle mergeHandle = metadata.beginMerge(session, merge.getHandle());
                return new MergeTarget(
                        mergeHandle.getTableHandle(),
                        Optional.of(mergeHandle),
                        merge.getSchemaTableName(),
                        merge.getMergeParadigmAndTypes());
            }
            if (target instanceof TableWriterNode.RefreshMaterializedViewReference refreshMV) {
                return new TableWriterNode.RefreshMaterializedViewTarget(
                        refreshMV.getStorageTableHandle(),
                        metadata.beginRefreshMaterializedView(session, refreshMV.getStorageTableHandle(), refreshMV.getSourceTableHandles()),
                        metadata.getTableMetadata(session, refreshMV.getStorageTableHandle()).getTable(),
                        refreshMV.getSourceTableHandles());
            }
            if (target instanceof TableExecuteTarget tableExecute) {
                BeginTableExecuteResult<TableExecuteHandle, TableHandle> result = metadata.beginTableExecute(session, tableExecute.getExecuteHandle(), tableExecute.getMandatorySourceHandle());
                return new TableExecuteTarget(result.getTableExecuteHandle(), Optional.of(result.getSourceHandle()), tableExecute.getSchemaTableName(), tableExecute.isReportingWrittenBytesSupported());
            }
            throw new IllegalArgumentException("Unhandled target type: " + target.getClass().getSimpleName());
        }

        private Optional<TableHandle> findTableScanHandleForTableExecute(PlanNode startNode)
        {
            List<PlanNode> tableScanNodes = PlanNodeSearcher.searchFrom(startNode)
                    .where(node -> node instanceof TableScanNode && ((TableScanNode) node).isUpdateTarget())
                    .findAll();

            if (tableScanNodes.size() == 1) {
                return Optional.of(((TableScanNode) tableScanNodes.get(0)).getTable());
            }
            throw new IllegalArgumentException("Expected to find exactly one update target TableScanNode in plan but found: " + tableScanNodes);
        }

        private Optional<TableHandle> findTableScanHandleForMergeWriter(PlanNode startNode)
        {
            List<PlanNode> tableScanNodes = PlanNodeSearcher.searchFrom(startNode)
                    .where(node -> node instanceof TableScanNode scanNode && scanNode.isUpdateTarget())
                    .findAll();

            if (tableScanNodes.isEmpty()) {
                return Optional.empty();
            }
            if (tableScanNodes.size() == 1) {
                return Optional.of(((TableScanNode) tableScanNodes.get(0)).getTable());
            }
            throw new IllegalArgumentException("Expected to find zero or one update target TableScanNode in plan but found: " + tableScanNodes);
        }

        private PlanNode rewriteModifyTableScan(PlanNode node, TableHandle handle, boolean tableScanNotFoundIsOk)
        {
            AtomicInteger modifyCount = new AtomicInteger(0);
            PlanNode rewrittenNode = SimplePlanRewriter.rewriteWith(
                    new SimplePlanRewriter<Void>()
                    {
                        @Override
                        public PlanNode visitTableScan(TableScanNode scan, RewriteContext<Void> context)
                        {
                            if (!scan.isUpdateTarget()) {
                                return scan;
                            }
                            modifyCount.incrementAndGet();
                            return new TableScanNode(
                                    scan.getId(),
                                    handle,
                                    scan.getOutputSymbols(),
                                    scan.getAssignments(),
                                    scan.getEnforcedConstraint(),
                                    scan.getStatistics(),
                                    scan.isUpdateTarget(),
                                    // partitioning should not change with write table handle
                                    scan.getUseConnectorNodePartitioning());
                        }
                    },
                    node,
                    null);

            int countFound = modifyCount.get();
            if (tableScanNotFoundIsOk) {
                verify(countFound == 0 || countFound == 1, "Expected to find zero or one update target TableScanNodes but found %s", countFound);
            }
            else {
                verify(countFound == 1, "Expected to find exactly one update target TableScanNode but found %s", countFound);
            }
            return rewrittenNode;
        }
    }

    private static WriterTarget getContextTarget(RewriteContext<Optional<WriterTarget>> context)
    {
        return context.get().orElseThrow(() -> new IllegalStateException("WriterTarget not present"));
    }
}
