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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableMap;
import io.trino.metadata.ResolvingIdentity;
import io.trino.metadata.TableHandle;
import io.trino.sql.planner.plan.IndexSourceNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.SimplePlanRewriter;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.Map;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

/**
 * Carries the identities the tables of a plan fragment were resolved as, for example the owner of a SECURITY DEFINER
 * view, from the coordinator to a worker. The serialized form of a {@link TableHandle} leaves the identity out, so that
 * query details never expose it, which makes this the only way a worker learns it.
 * <p>
 * Only the tables a fragment reads, in table scans and index sources, are covered. The other table handles of a
 * fragment, such as the targets of writes, are used with the session identity.
 */
public final class TableResolvingIdentities
{
    private TableResolvingIdentities() {}

    /**
     * The identities the tables read by {@code root} were resolved as, by the id of the node reading them.
     */
    public static Map<PlanNodeId, ResolvingIdentity> collect(PlanNode root)
    {
        ImmutableMap.Builder<PlanNodeId, ResolvingIdentity> identities = ImmutableMap.builder();
        root.accept(new Collector(), identities);
        return identities.buildOrThrow();
    }

    /**
     * Restores the identities of the tables read by {@code fragment}. A fragment which was not serialized keeps the
     * identities it was planned with.
     */
    public static PlanFragment restoreIn(PlanFragment fragment, Map<PlanNodeId, ResolvingIdentity> identities)
    {
        return fragment.withRoot(SimplePlanRewriter.rewriteWith(new Restorer(identities), fragment.getRoot()));
    }

    private static final class Collector
            extends SimplePlanVisitor<ImmutableMap.Builder<PlanNodeId, ResolvingIdentity>>
    {
        @Override
        public Void visitTableScan(TableScanNode node, ImmutableMap.Builder<PlanNodeId, ResolvingIdentity> identities)
        {
            identities.put(node.getId(), node.getTable().requireResolvingIdentity());
            return super.visitTableScan(node, identities);
        }

        @Override
        public Void visitIndexSource(IndexSourceNode node, ImmutableMap.Builder<PlanNodeId, ResolvingIdentity> identities)
        {
            identities.put(node.getId(), node.getTableHandle().requireResolvingIdentity());
            return super.visitIndexSource(node, identities);
        }
    }

    private static final class Restorer
            extends SimplePlanRewriter<Void>
    {
        private final Map<PlanNodeId, ResolvingIdentity> identities;

        private Restorer(Map<PlanNodeId, ResolvingIdentity> identities)
        {
            this.identities = requireNonNull(identities, "identities is null");
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Void> context)
        {
            return node.withTableHandle(restore(node.getTable(), node.getId()));
        }

        @Override
        public PlanNode visitIndexSource(IndexSourceNode node, RewriteContext<Void> context)
        {
            return new IndexSourceNode(
                    node.getId(),
                    node.getIndexHandle(),
                    restore(node.getTableHandle(), node.getId()),
                    node.getLookupSymbols(),
                    node.getOutputSymbols(),
                    node.getAssignments());
        }

        private TableHandle restore(TableHandle table, PlanNodeId node)
        {
            if (table.resolvingIdentity().isPresent()) {
                // the fragment was not serialized, so the handle still carries the identity it was planned with
                return table;
            }
            ResolvingIdentity identity = identities.get(node);
            verify(identity != null, "No identity for the table of the plan node %s", node);
            return table.withResolvingIdentity(identity);
        }
    }
}
