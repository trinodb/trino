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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.trino.Session;
import io.trino.connector.CatalogProperties;
import io.trino.cost.StatsAndCosts;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.CatalogInfo;
import io.trino.metadata.CatalogManager;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableProperties.TablePartitioning;
import io.trino.operator.RetryPolicy;
import io.trino.spi.TrinoException;
import io.trino.spi.TrinoWarning;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.ExplainAnalyzeNode;
import io.trino.sql.planner.plan.MergeWriterNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.RefreshMaterializedViewNode;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.SimplePlanRewriter;
import io.trino.sql.planner.plan.SimpleTableExecuteNode;
import io.trino.sql.planner.plan.StatisticsWriterNode;
import io.trino.sql.planner.plan.TableDeleteNode;
import io.trino.sql.planner.plan.TableExecuteNode;
import io.trino.sql.planner.plan.TableFinishNode;
import io.trino.sql.planner.plan.TableFunctionNode;
import io.trino.sql.planner.plan.TableFunctionProcessorNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.transaction.TransactionManager;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.in;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.getQueryMaxStageCount;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.SystemSessionProperties.isForceSingleNodeOutput;
import static io.trino.spi.StandardErrorCode.QUERY_HAS_TOO_MANY_STAGES;
import static io.trino.spi.connector.StandardWarningCode.TOO_MANY_STAGES;
import static io.trino.sql.planner.SchedulingOrderVisitor.scheduleOrder;
import static io.trino.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.planprinter.PlanPrinter.jsonFragmentPlan;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Splits a logical plan into fragments that can be shipped and executed on distributed nodes
 */
public class PlanFragmenter
{
    private static final String TOO_MANY_STAGES_MESSAGE = "" +
            "If the query contains multiple aggregates with DISTINCT over different columns, please set the 'use_mark_distinct' session property to false. " +
            "If the query contains WITH clauses that are referenced more than once, please create temporary table(s) for the queries in those clauses.";

    private final Metadata metadata;
    private final FunctionManager functionManager;
    private final TransactionManager transactionManager;
    private final CatalogManager catalogManager;
    private final int stageCountWarningThreshold;

    @Inject
    public PlanFragmenter(
            Metadata metadata,
            FunctionManager functionManager,
            TransactionManager transactionManager,
            CatalogManager catalogManager,
            QueryManagerConfig queryManagerConfig)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
        this.stageCountWarningThreshold = requireNonNull(queryManagerConfig, "queryManagerConfig is null").getStageCountWarningThreshold();
    }

    public SubPlan createSubPlans(Session session, Plan plan, boolean forceSingleNode, WarningCollector warningCollector)
    {
        List<CatalogProperties> activeCatalogs = transactionManager.getActiveCatalogs(session.getTransactionId().orElseThrow()).stream()
                .map(CatalogInfo::getCatalogHandle)
                .flatMap(catalogHandle -> catalogManager.getCatalogProperties(catalogHandle).stream())
                .collect(toImmutableList());
        Fragmenter fragmenter = new Fragmenter(session, metadata, functionManager, plan.getTypes(), plan.getStatsAndCosts(), activeCatalogs);

        FragmentProperties properties = new FragmentProperties(new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), plan.getRoot().getOutputSymbols()));
        if (forceSingleNode || isForceSingleNodeOutput(session)) {
            properties = properties.setSingleNodeDistribution();
        }
        PlanNode root = SimplePlanRewriter.rewriteWith(fragmenter, plan.getRoot(), properties);

        SubPlan subPlan = fragmenter.buildRootFragment(root, properties);
        subPlan = reassignPartitioningHandleIfNecessary(session, subPlan);

        checkState(!isForceSingleNodeOutput(session) || subPlan.getFragment().getPartitioning().isSingleNode(), "Root of PlanFragment is not single node");

        // TODO: Remove query_max_stage_count session property and use queryManagerConfig.getMaxStageCount() here
        sanityCheckFragmentedPlan(subPlan, warningCollector, getQueryMaxStageCount(session), stageCountWarningThreshold);

        return subPlan;
    }

    private void sanityCheckFragmentedPlan(SubPlan subPlan, WarningCollector warningCollector, int maxStageCount, int stageCountSoftLimit)
    {
        subPlan.sanityCheck();
        int fragmentCount = subPlan.getAllFragments().size();
        if (fragmentCount > maxStageCount) {
            throw new TrinoException(QUERY_HAS_TOO_MANY_STAGES, format(
                    "Number of stages in the query (%s) exceeds the allowed maximum (%s). %s",
                    fragmentCount,
                    maxStageCount,
                    TOO_MANY_STAGES_MESSAGE));
        }
        if (fragmentCount > stageCountSoftLimit) {
            warningCollector.add(new TrinoWarning(TOO_MANY_STAGES, format(
                    "Number of stages in the query (%s) exceeds the soft limit (%s). %s",
                    fragmentCount,
                    stageCountSoftLimit,
                    TOO_MANY_STAGES_MESSAGE)));
        }
    }

    private SubPlan reassignPartitioningHandleIfNecessary(Session session, SubPlan subPlan)
    {
        return reassignPartitioningHandleIfNecessaryHelper(session, subPlan, subPlan.getFragment().getPartitioning());
    }

    private SubPlan reassignPartitioningHandleIfNecessaryHelper(Session session, SubPlan subPlan, PartitioningHandle newOutputPartitioningHandle)
    {
        PlanFragment fragment = subPlan.getFragment();

        PlanNode newRoot = fragment.getRoot();
        // If the fragment's partitioning is SINGLE or COORDINATOR_ONLY, leave the sources as is (this is for single-node execution)
        if (!fragment.getPartitioning().isSingleNode()) {
            PartitioningHandleReassigner partitioningHandleReassigner = new PartitioningHandleReassigner(fragment.getPartitioning(), metadata, session);
            newRoot = SimplePlanRewriter.rewriteWith(partitioningHandleReassigner, newRoot);
        }
        PartitioningScheme outputPartitioningScheme = fragment.getOutputPartitioningScheme();
        Partitioning newOutputPartitioning = outputPartitioningScheme.getPartitioning();
        if (outputPartitioningScheme.getPartitioning().getHandle().getCatalogHandle().isPresent()) {
            // Do not replace the handle if the source's output handle is a system one, e.g. broadcast.
            newOutputPartitioning = newOutputPartitioning.withAlternativePartitioningHandle(newOutputPartitioningHandle);
        }
        PlanFragment newFragment = new PlanFragment(
                fragment.getId(),
                newRoot,
                fragment.getSymbols(),
                fragment.getPartitioning(),
                fragment.getPartitionCount(),
                fragment.getPartitionedSources(),
                new PartitioningScheme(
                        newOutputPartitioning,
                        outputPartitioningScheme.getOutputLayout(),
                        outputPartitioningScheme.getHashColumn(),
                        outputPartitioningScheme.isReplicateNullsAndAny(),
                        outputPartitioningScheme.getBucketToPartition(),
                        outputPartitioningScheme.getPartitionCount()),
                fragment.getStatsAndCosts(),
                fragment.getActiveCatalogs(),
                fragment.getJsonRepresentation());

        ImmutableList.Builder<SubPlan> childrenBuilder = ImmutableList.builder();
        for (SubPlan child : subPlan.getChildren()) {
            childrenBuilder.add(reassignPartitioningHandleIfNecessaryHelper(session, child, fragment.getPartitioning()));
        }
        return new SubPlan(newFragment, childrenBuilder.build());
    }

    private static class Fragmenter
            extends SimplePlanRewriter<FragmentProperties>
    {
        private static final int ROOT_FRAGMENT_ID = 0;

        private final Session session;
        private final Metadata metadata;
        private final FunctionManager functionManager;
        private final TypeProvider types;
        private final StatsAndCosts statsAndCosts;
        private final List<CatalogProperties> activeCatalogs;
        private int nextFragmentId = ROOT_FRAGMENT_ID + 1;

        public Fragmenter(Session session, Metadata metadata, FunctionManager functionManager, TypeProvider types, StatsAndCosts statsAndCosts, List<CatalogProperties> activeCatalogs)
        {
            this.session = requireNonNull(session, "session is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.functionManager = requireNonNull(functionManager, "functionManager is null");
            this.types = requireNonNull(types, "types is null");
            this.statsAndCosts = requireNonNull(statsAndCosts, "statsAndCosts is null");
            this.activeCatalogs = requireNonNull(activeCatalogs, "activeCatalogs is null");
        }

        public SubPlan buildRootFragment(PlanNode root, FragmentProperties properties)
        {
            return buildFragment(root, properties, new PlanFragmentId(String.valueOf(ROOT_FRAGMENT_ID)));
        }

        private PlanFragmentId nextFragmentId()
        {
            return new PlanFragmentId(String.valueOf(nextFragmentId++));
        }

        private SubPlan buildFragment(PlanNode root, FragmentProperties properties, PlanFragmentId fragmentId)
        {
            Set<Symbol> dependencies = SymbolsExtractor.extractOutputSymbols(root);

            List<PlanNodeId> schedulingOrder = scheduleOrder(root);
            boolean equals = properties.getPartitionedSources().equals(ImmutableSet.copyOf(schedulingOrder));
            checkArgument(equals, "Expected scheduling order (%s) to contain an entry for all partitioned sources (%s)", schedulingOrder, properties.getPartitionedSources());

            Map<Symbol, Type> symbols = Maps.filterKeys(types.allTypes(), in(dependencies));

            PlanFragment fragment = new PlanFragment(
                    fragmentId,
                    root,
                    symbols,
                    properties.getPartitioningHandle(),
                    properties.getPartitionCount(),
                    schedulingOrder,
                    properties.getPartitioningScheme(),
                    statsAndCosts.getForSubplan(root),
                    activeCatalogs,
                    Optional.of(jsonFragmentPlan(root, symbols, metadata, functionManager, session)));

            return new SubPlan(fragment, properties.getChildren());
        }

        @Override
        public PlanNode visitOutput(OutputNode node, RewriteContext<FragmentProperties> context)
        {
            if (isForceSingleNodeOutput(session)) {
                context.get().setSingleNodeDistribution();
            }

            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitExplainAnalyze(ExplainAnalyzeNode node, RewriteContext<FragmentProperties> context)
        {
            context.get().setCoordinatorOnlyDistribution();
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitStatisticsWriterNode(StatisticsWriterNode node, RewriteContext<FragmentProperties> context)
        {
            context.get().setCoordinatorOnlyDistribution();
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitSimpleTableExecuteNode(SimpleTableExecuteNode node, RewriteContext<FragmentProperties> context)
        {
            context.get().setCoordinatorOnlyDistribution();
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitTableFinish(TableFinishNode node, RewriteContext<FragmentProperties> context)
        {
            context.get().setCoordinatorOnlyDistribution();
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitTableDelete(TableDeleteNode node, RewriteContext<FragmentProperties> context)
        {
            context.get().setCoordinatorOnlyDistribution();
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<FragmentProperties> context)
        {
            PartitioningHandle partitioning = metadata.getTableProperties(session, node.getTable())
                    .getTablePartitioning()
                    .filter(value -> node.isUseConnectorNodePartitioning())
                    .map(TablePartitioning::getPartitioningHandle)
                    .orElse(SOURCE_DISTRIBUTION);

            context.get().addSourceDistribution(node.getId(), partitioning, metadata, session);
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitRefreshMaterializedView(RefreshMaterializedViewNode node, RewriteContext<FragmentProperties> context)
        {
            context.get().setCoordinatorOnlyDistribution();
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitTableWriter(TableWriterNode node, RewriteContext<FragmentProperties> context)
        {
            node.getPartitioningScheme().ifPresent(scheme -> context.get().setDistribution(
                    scheme.getPartitioning().getHandle(),
                    scheme.getPartitionCount(),
                    metadata,
                    session));
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitTableExecute(TableExecuteNode node, RewriteContext<FragmentProperties> context)
        {
            node.getPartitioningScheme().ifPresent(scheme -> context.get().setDistribution(
                    scheme.getPartitioning().getHandle(),
                    scheme.getPartitionCount(),
                    metadata,
                    session));
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitMergeWriter(MergeWriterNode node, RewriteContext<FragmentProperties> context)
        {
            node.getPartitioningScheme().ifPresent(scheme -> context.get().setDistribution(
                    scheme.getPartitioning().getHandle(),
                    scheme.getPartitionCount(),
                    metadata,
                    session));
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitValues(ValuesNode node, RewriteContext<FragmentProperties> context)
        {
            // An empty values node is compatible with any distribution, so
            // don't attempt to overwrite one's already been chosen
            if (node.getRowCount() != 0 || !context.get().hasDistribution()) {
                context.get().setSingleNodeDistribution();
            }
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitTableFunction(TableFunctionNode node, RewriteContext<FragmentProperties> context)
        {
            throw new IllegalStateException(format("Unexpected node: TableFunctionNode (%s)", node.getName()));
        }

        @Override
        public PlanNode visitTableFunctionProcessor(TableFunctionProcessorNode node, RewriteContext<FragmentProperties> context)
        {
            if (node.getSource().isEmpty()) {
                // context is mutable. The leaf node should set the PartitioningHandle.
                context.get().addSourceDistribution(node.getId(), SOURCE_DISTRIBUTION, metadata, session);
            }
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitExchange(ExchangeNode exchange, RewriteContext<FragmentProperties> context)
        {
            if (exchange.getScope() != REMOTE) {
                return context.defaultRewrite(exchange, context.get());
            }

            PartitioningScheme partitioningScheme = exchange.getPartitioningScheme();

            if (exchange.getType() == ExchangeNode.Type.GATHER) {
                context.get().setSingleNodeDistribution();
            }
            else if (exchange.getType() == ExchangeNode.Type.REPARTITION) {
                context.get().setDistribution(
                        partitioningScheme.getPartitioning().getHandle(),
                        partitioningScheme.getPartitionCount(),
                        metadata,
                        session);
            }

            ImmutableList.Builder<FragmentProperties> childrenProperties = ImmutableList.builder();
            ImmutableList.Builder<SubPlan> childrenBuilder = ImmutableList.builder();
            for (int sourceIndex = 0; sourceIndex < exchange.getSources().size(); sourceIndex++) {
                FragmentProperties childProperties = new FragmentProperties(partitioningScheme.translateOutputLayout(exchange.getInputs().get(sourceIndex)));
                childrenProperties.add(childProperties);
                childrenBuilder.add(buildSubPlan(exchange.getSources().get(sourceIndex), childProperties, context));
            }

            List<SubPlan> children = childrenBuilder.build();
            context.get().addChildren(children);

            List<PlanFragmentId> childrenIds = children.stream()
                    .map(SubPlan::getFragment)
                    .map(PlanFragment::getId)
                    .collect(toImmutableList());

            return new RemoteSourceNode(
                    exchange.getId(),
                    childrenIds,
                    exchange.getOutputSymbols(),
                    exchange.getOrderingScheme(),
                    exchange.getType(),
                    isWorkerCoordinatorBoundary(context.get(), childrenProperties.build()) ? getRetryPolicy(session) : RetryPolicy.NONE);
        }

        private SubPlan buildSubPlan(PlanNode node, FragmentProperties properties, RewriteContext<FragmentProperties> context)
        {
            PlanFragmentId planFragmentId = nextFragmentId();
            PlanNode child = context.rewrite(node, properties);
            return buildFragment(child, properties, planFragmentId);
        }

        private static boolean isWorkerCoordinatorBoundary(FragmentProperties fragmentProperties, List<FragmentProperties> childFragmentsProperties)
        {
            if (!fragmentProperties.getPartitioningHandle().isCoordinatorOnly()) {
                // receiver stage is not a coordinator stage
                return false;
            }
            if (childFragmentsProperties.stream().allMatch(properties -> properties.getPartitioningHandle().isCoordinatorOnly())) {
                // coordinator to coordinator exchange
                return false;
            }
            checkArgument(
                    childFragmentsProperties.stream().noneMatch(properties -> properties.getPartitioningHandle().isCoordinatorOnly()),
                    "Plans are not expected to have a mix of coordinator only fragments and distributed fragments as siblings");
            return true;
        }
    }

    private static class FragmentProperties
    {
        private final List<SubPlan> children = new ArrayList<>();

        private final PartitioningScheme partitioningScheme;

        private Optional<PartitioningHandle> partitioningHandle = Optional.empty();
        private Optional<Integer> partitionCount = Optional.empty();
        private final Set<PlanNodeId> partitionedSources = new HashSet<>();

        public FragmentProperties(PartitioningScheme partitioningScheme)
        {
            this.partitioningScheme = partitioningScheme;
        }

        public List<SubPlan> getChildren()
        {
            return children;
        }

        public boolean hasDistribution()
        {
            return partitioningHandle.isPresent();
        }

        public FragmentProperties setSingleNodeDistribution()
        {
            if (partitioningHandle.isPresent() && partitioningHandle.get().isSingleNode()) {
                // already single node distribution
                return this;
            }

            checkState(partitioningHandle.isEmpty(),
                    "Cannot overwrite partitioning with %s (currently set to %s)",
                    SINGLE_DISTRIBUTION,
                    partitioningHandle);

            partitioningHandle = Optional.of(SINGLE_DISTRIBUTION);

            return this;
        }

        public FragmentProperties setDistribution(
                PartitioningHandle distribution,
                Optional<Integer> partitionCount,
                Metadata metadata,
                Session session)
        {
            if (partitioningHandle.isEmpty()) {
                partitioningHandle = Optional.of(distribution);
                this.partitionCount = partitionCount;
                return this;
            }

            PartitioningHandle currentPartitioning = this.partitioningHandle.get();

            if (currentPartitioning.equals(distribution)) {
                return this;
            }

            // If already system SINGLE or COORDINATOR_ONLY, leave it as is (this is for single-node execution)
            if (currentPartitioning.isSingleNode()) {
                return this;
            }

            if (isCompatibleSystemPartitioning(distribution)) {
                return this;
            }

            if (isCompatibleScaledWriterPartitioning(currentPartitioning, distribution)) {
                this.partitioningHandle = Optional.of(distribution);
                this.partitionCount = partitionCount;
                return this;
            }

            if (currentPartitioning.equals(SOURCE_DISTRIBUTION)) {
                this.partitioningHandle = Optional.of(distribution);
                return this;
            }

            Optional<PartitioningHandle> commonPartitioning = metadata.getCommonPartitioning(session, currentPartitioning, distribution);
            if (commonPartitioning.isPresent()) {
                partitioningHandle = commonPartitioning;
                return this;
            }

            throw new IllegalStateException(format(
                    "Cannot set distribution to %s. Already set to %s",
                    distribution,
                    this.partitioningHandle));
        }

        private boolean isCompatibleSystemPartitioning(PartitioningHandle distribution)
        {
            ConnectorPartitioningHandle currentHandle = partitioningHandle.get().getConnectorHandle();
            ConnectorPartitioningHandle distributionHandle = distribution.getConnectorHandle();
            if ((currentHandle instanceof SystemPartitioningHandle) &&
                    (distributionHandle instanceof SystemPartitioningHandle)) {
                return ((SystemPartitioningHandle) currentHandle).getPartitioning() ==
                        ((SystemPartitioningHandle) distributionHandle).getPartitioning();
            }
            return false;
        }

        private static boolean isCompatibleScaledWriterPartitioning(PartitioningHandle current, PartitioningHandle suggested)
        {
            if (current.equals(FIXED_HASH_DISTRIBUTION) && suggested.equals(SCALED_WRITER_HASH_DISTRIBUTION)) {
                return true;
            }
            PartitioningHandle currentWithScaledWritersEnabled = new PartitioningHandle(
                    current.getCatalogHandle(),
                    current.getTransactionHandle(),
                    current.getConnectorHandle(),
                    true);
            return currentWithScaledWritersEnabled.equals(suggested);
        }

        public FragmentProperties setCoordinatorOnlyDistribution()
        {
            if (partitioningHandle.isPresent() && partitioningHandle.get().isCoordinatorOnly()) {
                // already single node distribution
                return this;
            }

            // only system SINGLE can be upgraded to COORDINATOR_ONLY
            checkState(partitioningHandle.isEmpty() || partitioningHandle.get().equals(SINGLE_DISTRIBUTION),
                    "Cannot overwrite partitioning with %s (currently set to %s)",
                    COORDINATOR_DISTRIBUTION,
                    partitioningHandle);

            partitioningHandle = Optional.of(COORDINATOR_DISTRIBUTION);

            return this;
        }

        public FragmentProperties addSourceDistribution(PlanNodeId source, PartitioningHandle distribution, Metadata metadata, Session session)
        {
            requireNonNull(source, "source is null");
            requireNonNull(distribution, "distribution is null");

            partitionedSources.add(source);

            if (partitioningHandle.isEmpty()) {
                partitioningHandle = Optional.of(distribution);
                return this;
            }

            PartitioningHandle currentPartitioning = partitioningHandle.get();

            // If already system SINGLE or COORDINATOR_ONLY, leave it as is (this is for single-node execution)
            if (currentPartitioning.equals(SINGLE_DISTRIBUTION) || currentPartitioning.equals(COORDINATOR_DISTRIBUTION)) {
                return this;
            }

            if (currentPartitioning.equals(distribution)) {
                return this;
            }

            Optional<PartitioningHandle> commonPartitioning = metadata.getCommonPartitioning(session, currentPartitioning, distribution);
            if (commonPartitioning.isPresent()) {
                partitioningHandle = commonPartitioning;
                return this;
            }

            throw new IllegalStateException(format("Cannot overwrite distribution with %s (currently set to %s)", distribution, currentPartitioning));
        }

        public FragmentProperties addChildren(List<SubPlan> children)
        {
            this.children.addAll(children);

            return this;
        }

        public PartitioningScheme getPartitioningScheme()
        {
            return partitioningScheme;
        }

        public PartitioningHandle getPartitioningHandle()
        {
            return partitioningHandle.get();
        }

        public Optional<Integer> getPartitionCount()
        {
            return partitionCount;
        }

        public Set<PlanNodeId> getPartitionedSources()
        {
            return partitionedSources;
        }
    }

    private static final class PartitioningHandleReassigner
            extends SimplePlanRewriter<Void>
    {
        private final PartitioningHandle fragmentPartitioningHandle;
        private final Metadata metadata;
        private final Session session;

        public PartitioningHandleReassigner(PartitioningHandle fragmentPartitioningHandle, Metadata metadata, Session session)
        {
            this.fragmentPartitioningHandle = fragmentPartitioningHandle;
            this.metadata = metadata;
            this.session = session;
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Void> context)
        {
            PartitioningHandle partitioning = metadata.getTableProperties(session, node.getTable())
                    .getTablePartitioning()
                    .filter(value -> node.isUseConnectorNodePartitioning())
                    .map(TablePartitioning::getPartitioningHandle)
                    .orElse(SOURCE_DISTRIBUTION);
            if (partitioning.equals(fragmentPartitioningHandle)) {
                // do nothing if the current scan node's partitioning matches the fragment's
                return node;
            }

            TableHandle newTable = metadata.makeCompatiblePartitioning(session, node.getTable(), fragmentPartitioningHandle);
            return new TableScanNode(
                    node.getId(),
                    newTable,
                    node.getOutputSymbols(),
                    node.getAssignments(),
                    node.getEnforcedConstraint(),
                    node.getStatistics(),
                    node.isUpdateTarget(),
                    // plan was already fragmented with scan node's partitioning
                    // and new partitioning is compatible with previous one
                    node.getUseConnectorNodePartitioning());
        }
    }
}
