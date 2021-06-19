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

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.cost.StatsAndCosts;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.plan.DeleteNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.SimplePlanRewriter;
import io.trino.sql.planner.plan.SimplePlanRewriter.RewriteContext;
import io.trino.sql.planner.plan.StatisticsWriterNode;
import io.trino.sql.planner.plan.TableFinishNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.sql.planner.plan.TableWriterNode.CreateReference;
import io.trino.sql.planner.plan.TableWriterNode.CreateTarget;
import io.trino.sql.planner.plan.TableWriterNode.DeleteTarget;
import io.trino.sql.planner.plan.TableWriterNode.InsertReference;
import io.trino.sql.planner.plan.TableWriterNode.InsertTarget;
import io.trino.sql.planner.plan.TableWriterNode.UpdateTarget;
import io.trino.sql.planner.plan.TableWriterNode.WriterTarget;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.plan.UpdateNode;

import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.isAtMostScalar;
import static io.trino.sql.planner.plan.ChildReplacer.replaceChildren;
import static io.trino.sql.planner.planprinter.PlanPrinter.textLogicalPlan;
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

    public BeginTableWrite(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        try {
            return SimplePlanRewriter.rewriteWith(new Rewriter(session), plan, Optional.empty());
        }
        catch (RuntimeException e) {
            try {
                int nestLevel = 4; // so that it renders reasonably within exception stacktrace
                String explain = textLogicalPlan(plan, types, metadata, StatsAndCosts.empty(), session, nestLevel, false);
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
                    node.getNotNullColumnSymbols(),
                    node.getPartitioningScheme(),
                    node.getPreferredPartitioningScheme(),
                    node.getStatisticsAggregation(),
                    node.getStatisticsAggregationDescriptor());
        }

        @Override
        public PlanNode visitDelete(DeleteNode node, RewriteContext<Optional<WriterTarget>> context)
        {
            DeleteTarget deleteTarget = (DeleteTarget) getContextTarget(context);
            return new DeleteNode(
                    node.getId(),
                    rewriteModifyTableScan(node.getSource(), deleteTarget.getHandleOrElseThrow()),
                    deleteTarget,
                    node.getRowId(),
                    node.getOutputSymbols());
        }

        @Override
        public PlanNode visitUpdate(UpdateNode node, RewriteContext<Optional<WriterTarget>> context)
        {
            UpdateTarget updateTarget = (UpdateTarget) getContextTarget(context);
            return new UpdateNode(
                    node.getId(),
                    rewriteModifyTableScan(node.getSource(), updateTarget.getHandleOrElseThrow()),
                    updateTarget,
                    node.getRowId(),
                    node.getColumnValueAndRowIdSymbols(),
                    node.getOutputSymbols());
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
            if (node instanceof DeleteNode) {
                DeleteNode deleteNode = (DeleteNode) node;
                DeleteTarget delete = deleteNode.getTarget();
                return new DeleteTarget(
                        Optional.of(findTableScanHandle(deleteNode.getSource())),
                        delete.getSchemaTableName());
            }
            if (node instanceof UpdateNode) {
                UpdateNode updateNode = (UpdateNode) node;
                UpdateTarget update = updateNode.getTarget();
                return new UpdateTarget(
                        Optional.of(findTableScanHandle(updateNode.getSource())),
                        update.getSchemaTableName(),
                        update.getUpdatedColumns(),
                        update.getUpdatedColumnHandles());
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
            if (target instanceof CreateReference) {
                CreateReference create = (CreateReference) target;
                return new CreateTarget(metadata.beginCreateTable(session, create.getCatalog(), create.getTableMetadata(), create.getLayout()), create.getTableMetadata().getTable());
            }
            if (target instanceof InsertReference) {
                InsertReference insert = (InsertReference) target;
                return new InsertTarget(metadata.beginInsert(session, insert.getHandle(), insert.getColumns()), metadata.getTableMetadata(session, insert.getHandle()).getTable());
            }
            if (target instanceof DeleteTarget) {
                DeleteTarget delete = (DeleteTarget) target;
                TableHandle newHandle = metadata.beginDelete(session, delete.getHandleOrElseThrow());
                return new DeleteTarget(Optional.of(newHandle), delete.getSchemaTableName());
            }
            if (target instanceof UpdateTarget) {
                UpdateTarget update = (UpdateTarget) target;
                TableHandle newHandle = metadata.beginUpdate(session, update.getHandleOrElseThrow(), update.getUpdatedColumnHandles());
                return new UpdateTarget(
                        Optional.of(newHandle),
                        update.getSchemaTableName(),
                        update.getUpdatedColumns(),
                        update.getUpdatedColumnHandles());
            }
            if (target instanceof TableWriterNode.RefreshMaterializedViewReference) {
                TableWriterNode.RefreshMaterializedViewReference refreshMV = (TableWriterNode.RefreshMaterializedViewReference) target;
                return new TableWriterNode.RefreshMaterializedViewTarget(
                        refreshMV.getStorageTableHandle(),
                        metadata.beginRefreshMaterializedView(session, refreshMV.getStorageTableHandle(), refreshMV.getSourceTableHandles()),
                        metadata.getTableMetadata(session, refreshMV.getStorageTableHandle()).getTable(),
                        refreshMV.getSourceTableHandles());
            }
            throw new IllegalArgumentException("Unhandled target type: " + target.getClass().getSimpleName());
        }

        private TableHandle findTableScanHandle(PlanNode node)
        {
            if (node instanceof TableScanNode) {
                return ((TableScanNode) node).getTable();
            }
            if (node instanceof FilterNode) {
                return findTableScanHandle(((FilterNode) node).getSource());
            }
            if (node instanceof ProjectNode) {
                return findTableScanHandle(((ProjectNode) node).getSource());
            }
            if (node instanceof SemiJoinNode) {
                return findTableScanHandle(((SemiJoinNode) node).getSource());
            }
            if (node instanceof JoinNode) {
                JoinNode joinNode = (JoinNode) node;
                if (joinNode.getType() == JoinNode.Type.INNER && isAtMostScalar(joinNode.getRight())) {
                    return findTableScanHandle(joinNode.getLeft());
                }
            }
            throw new IllegalArgumentException("Invalid descendant for DeleteNode or UpdateNode: " + node.getClass().getName());
        }

        private PlanNode rewriteModifyTableScan(PlanNode node, TableHandle handle)
        {
            if (node instanceof TableScanNode) {
                TableScanNode scan = (TableScanNode) node;
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

            if (node instanceof FilterNode) {
                PlanNode source = rewriteModifyTableScan(((FilterNode) node).getSource(), handle);
                return replaceChildren(node, ImmutableList.of(source));
            }
            if (node instanceof ProjectNode) {
                PlanNode source = rewriteModifyTableScan(((ProjectNode) node).getSource(), handle);
                return replaceChildren(node, ImmutableList.of(source));
            }
            if (node instanceof SemiJoinNode) {
                PlanNode source = rewriteModifyTableScan(((SemiJoinNode) node).getSource(), handle);
                return replaceChildren(node, ImmutableList.of(source, ((SemiJoinNode) node).getFilteringSource()));
            }
            if (node instanceof JoinNode) {
                JoinNode joinNode = (JoinNode) node;
                if (joinNode.getType() == JoinNode.Type.INNER && isAtMostScalar(joinNode.getRight())) {
                    PlanNode source = rewriteModifyTableScan(joinNode.getLeft(), handle);
                    return replaceChildren(node, ImmutableList.of(source, joinNode.getRight()));
                }
            }
            throw new IllegalArgumentException("Invalid descendant for DeleteNode or UpdateNode: " + node.getClass().getName());
        }
    }

    private static WriterTarget getContextTarget(RewriteContext<Optional<WriterTarget>> context)
    {
        return context.get().orElseThrow(() -> new IllegalStateException("WriterTarget not present"));
    }
}
