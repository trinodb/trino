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
package io.trino.execution.scheduler;

import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;
import io.trino.Session;
import io.trino.connector.informationschema.InformationSchemaTableHandle;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.connector.system.SystemTableHandle;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.RefreshMaterializedViewNode;
import io.trino.sql.planner.plan.TableScanNode;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.List;
import java.util.function.Function;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

public class NoMemoryAwarePartitionMemoryEstimator
{
    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    public @interface ForNoMemoryAwarePartitionMemoryEstimator {}

    public static class Factory
            implements PartitionMemoryEstimatorFactory
    {
        private final PartitionMemoryEstimatorFactory delegateFactory;

        @Inject
        public Factory(@ForNoMemoryAwarePartitionMemoryEstimator PartitionMemoryEstimatorFactory delegateFactory)
        {
            this.delegateFactory = requireNonNull(delegateFactory, "delegateFactory is null");
        }

        @Override
        public PartitionMemoryEstimator createPartitionMemoryEstimator(
                Session session,
                PlanFragment planFragment,
                Function<PlanFragmentId, PlanFragment> sourceFragmentLookup)
        {
            if (isNoMemoryFragment(planFragment, sourceFragmentLookup)) {
                return NoMemoryPartitionMemoryEstimator.INSTANCE;
            }
            return delegateFactory.createPartitionMemoryEstimator(session, planFragment, sourceFragmentLookup);
        }

        private boolean isNoMemoryFragment(PlanFragment fragment, Function<PlanFragmentId, PlanFragment> childFragmentLookup)
        {
            if (fragment.getRoot().getSources().stream()
                    .anyMatch(planNode -> planNode instanceof RefreshMaterializedViewNode)) {
                // REFRESH MATERIALIZED VIEW will issue other SQL commands under the hood. If its task memory is
                // non-zero, then a deadlock scenario is possible if we only have a single node in the cluster.
                return true;
            }

            // If source fragments are not tagged as "no-memory" assume that they may produce significant amount of data.
            // We stay on the safe side an assume that we should use standard memory estimation for this fragment
            if (!fragment.getRemoteSourceNodes().stream().flatMap(node -> node.getSourceFragmentIds().stream())
                    // TODO: childFragmentLookup will be executed for subtree of every fragment in query plan. That means fragment will be
                    // analyzed multiple time. Given fact that logic here is not extremely expensive and plans are not gigantic (up to ~200 fragments)
                    // we can keep it as a first approach. Ultimately we should profile execution and possibly put in place some mechanisms to avoid repeated work.
                    .allMatch(sourceFragmentId -> isNoMemoryFragment(childFragmentLookup.apply(sourceFragmentId), childFragmentLookup))) {
                return false;
            }

            // If fragment source is not reading any external tables or only accesses information_schema assume it does not need significant amount of memory.
            // Allow scheduling even if whole server memory is pre allocated.
            List<PlanNode> tableScanNodes = PlanNodeSearcher.searchFrom(fragment.getRoot()).whereIsInstanceOfAny(TableScanNode.class).findAll();
            return tableScanNodes.stream().allMatch(node -> isMetadataTableScan((TableScanNode) node));
        }

        private static boolean isMetadataTableScan(TableScanNode tableScanNode)
        {
            return (tableScanNode.getTable().getConnectorHandle() instanceof InformationSchemaTableHandle) ||
                    (tableScanNode.getTable().getCatalogHandle().getCatalogName().equals(GlobalSystemConnector.NAME) &&
                            (tableScanNode.getTable().getConnectorHandle() instanceof SystemTableHandle systemHandle) &&
                            systemHandle.getSchemaName().equals("jdbc"));
        }
    }
}
