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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.spi.connector.ConstantProperty;
import io.trino.spi.connector.GroupingProperty;
import io.trino.spi.connector.LocalProperty;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.SystemPartitioningHandle;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ApplyNode;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.DistinctLimitNode;
import io.trino.sql.planner.plan.EnforceSingleRowNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.ExplainAnalyzeNode;
import io.trino.sql.planner.plan.IndexJoinNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PatternRecognitionNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.RowNumberNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.SimpleTableExecuteNode;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.SpatialJoinNode;
import io.trino.sql.planner.plan.StatisticsWriterNode;
import io.trino.sql.planner.plan.TableExecuteNode;
import io.trino.sql.planner.plan.TableFinishNode;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.TopNRankingNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.tree.SymbolReference;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.SystemSessionProperties.getTaskConcurrency;
import static io.trino.SystemSessionProperties.getTaskWriterCount;
import static io.trino.SystemSessionProperties.isDistributedSortEnabled;
import static io.trino.SystemSessionProperties.isSpillEnabled;
import static io.trino.sql.ExpressionUtils.isEffectivelyLiteral;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.optimizations.StreamPreferredProperties.any;
import static io.trino.sql.planner.optimizations.StreamPreferredProperties.defaultParallelism;
import static io.trino.sql.planner.optimizations.StreamPreferredProperties.exactlyPartitionedOn;
import static io.trino.sql.planner.optimizations.StreamPreferredProperties.fixedParallelism;
import static io.trino.sql.planner.optimizations.StreamPreferredProperties.partitionedOn;
import static io.trino.sql.planner.optimizations.StreamPreferredProperties.singleStream;
import static io.trino.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties.StreamDistribution.FIXED;
import static io.trino.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties.StreamDistribution.SINGLE;
import static io.trino.sql.planner.optimizations.StreamPropertyDerivations.derivePropertiesRecursively;
import static io.trino.sql.planner.plan.ChildReplacer.replaceChildren;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Type.GATHER;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.sql.planner.plan.ExchangeNode.gatheringExchange;
import static io.trino.sql.planner.plan.ExchangeNode.mergingExchange;
import static io.trino.sql.planner.plan.ExchangeNode.partitionedExchange;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class AddLocalExchanges
        implements PlanOptimizer
{
    private final PlannerContext plannerContext;
    private final TypeAnalyzer typeAnalyzer;

    public AddLocalExchanges(PlannerContext plannerContext, TypeAnalyzer typeAnalyzer)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        PlanWithProperties result = plan.accept(new Rewriter(symbolAllocator, idAllocator, session), any());
        return result.getNode();
    }

    private class Rewriter
            extends PlanVisitor<PlanWithProperties, StreamPreferredProperties>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final Session session;
        private final TypeProvider types;

        public Rewriter(SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, Session session)
        {
            this.types = symbolAllocator.getTypes();
            this.idAllocator = idAllocator;
            this.session = session;
        }

        @Override
        protected PlanWithProperties visitPlan(PlanNode node, StreamPreferredProperties parentPreferences)
        {
            return planAndEnforceChildren(
                    node,
                    parentPreferences.withoutPreference().withDefaultParallelism(session),
                    parentPreferences.withDefaultParallelism(session));
        }

        @Override
        public PlanWithProperties visitApply(ApplyNode node, StreamPreferredProperties parentPreferences)
        {
            throw new IllegalStateException("Unexpected node: " + node.getClass().getName());
        }

        @Override
        public PlanWithProperties visitCorrelatedJoin(CorrelatedJoinNode node, StreamPreferredProperties parentPreferences)
        {
            throw new IllegalStateException("Unexpected node: " + node.getClass().getName());
        }

        @Override
        public PlanWithProperties visitOutput(OutputNode node, StreamPreferredProperties parentPreferences)
        {
            return planAndEnforceChildren(
                    node,
                    any().withOrderSensitivity(),
                    any().withOrderSensitivity());
        }

        @Override
        public PlanWithProperties visitExplainAnalyze(ExplainAnalyzeNode node, StreamPreferredProperties parentPreferences)
        {
            // Although explain analyze discards all output, we want to maintain the behavior
            // of a normal output node, so declare the node to be order sensitive
            return planAndEnforceChildren(
                    node,
                    singleStream().withOrderSensitivity(),
                    singleStream().withOrderSensitivity());
        }

        @Override
        public PlanWithProperties visitProject(ProjectNode node, StreamPreferredProperties parentPreferences)
        {
            // Special handling for trivial projections. Applies to identity and renaming projections, and constants
            // It might be extended to handle other low-cost projections.
            if (node.getAssignments().getExpressions().stream().allMatch(expression -> expression instanceof SymbolReference || isEffectivelyLiteral(plannerContext, session, expression))) {
                if (parentPreferences.isSingleStreamPreferred()) {
                    // Do not enforce gathering exchange below project:
                    // - if project's source is single stream, no exchanges will be added around project,
                    // - if project's source is distributed, gather will be added on top of project.
                    return planAndEnforceChildren(
                            node,
                            parentPreferences.withoutPreference(),
                            parentPreferences.withDefaultParallelism(session));
                }
                // Do not enforce hashed repartition below project. Execute project with the same distribution as its source:
                // - if project's source is single stream, hash partitioned exchange will be added on top of project,
                // - if project's source is distributed, and the distribution does not satisfy parent partitioning requirements, hash partitioned exchange will be added on top of project.
                if (parentPreferences.getPartitioningColumns().isPresent() && !parentPreferences.getPartitioningColumns().get().isEmpty()) {
                    return planAndEnforceChildren(
                            node,
                            parentPreferences.withoutPreference(),
                            parentPreferences.withDefaultParallelism(session));
                }
                // If round-robin exchange is required by the parent, enforce it below project:
                // - if project's source is single stream, round robin exchange will be added below project,
                // - if project's source is distributed, no exchanges will be added around project.
                return planAndEnforceChildren(
                        node,
                        parentPreferences,
                        parentPreferences.withDefaultParallelism(session));
            }

            return planAndEnforceChildren(
                    node,
                    parentPreferences.withoutPreference().withDefaultParallelism(session),
                    parentPreferences.withDefaultParallelism(session));
        }

        //
        // Nodes that always require a single stream
        //

        @Override
        public PlanWithProperties visitSort(SortNode node, StreamPreferredProperties parentPreferences)
        {
            if (isDistributedSortEnabled(session)) {
                PlanWithProperties sortPlan = planAndEnforceChildren(node, fixedParallelism(), fixedParallelism());

                if (!sortPlan.getProperties().isSingleStream()) {
                    return deriveProperties(
                            mergingExchange(
                                    idAllocator.getNextId(),
                                    LOCAL,
                                    sortPlan.getNode(),
                                    node.getOrderingScheme()),
                            sortPlan.getProperties());
                }

                return sortPlan;
            }
            // sort requires that all data be in one stream
            // this node changes the input organization completely, so we do not pass through parent preferences
            return planAndEnforceChildren(node, singleStream(), defaultParallelism(session));
        }

        @Override
        public PlanWithProperties visitStatisticsWriterNode(StatisticsWriterNode node, StreamPreferredProperties context)
        {
            // analyze finish requires that all data be in one stream
            // this node changes the input organization completely, so we do not pass through parent preferences
            return planAndEnforceChildren(node, singleStream(), defaultParallelism(session));
        }

        @Override
        public PlanWithProperties visitTableFinish(TableFinishNode node, StreamPreferredProperties parentPreferences)
        {
            // table commit requires that all data be in one stream
            // this node changes the input organization completely, so we do not pass through parent preferences
            return planAndEnforceChildren(node, singleStream(), defaultParallelism(session));
        }

        @Override
        public PlanWithProperties visitTopN(TopNNode node, StreamPreferredProperties parentPreferences)
        {
            if (node.getStep() == TopNNode.Step.PARTIAL) {
                return planAndEnforceChildren(
                        node,
                        parentPreferences.withoutPreference().withDefaultParallelism(session),
                        parentPreferences.withDefaultParallelism(session));
            }

            // final topN requires that all data be in one stream
            // also, a final changes the input organization completely, so we do not pass through parent preferences
            return planAndEnforceChildren(
                    node,
                    singleStream(),
                    defaultParallelism(session));
        }

        @Override
        public PlanWithProperties visitLimit(LimitNode node, StreamPreferredProperties parentPreferences)
        {
            if (node.isWithTies()) {
                throw new IllegalStateException("Unexpected node: LimitNode with ties");
            }

            if (node.isPartial()) {
                StreamPreferredProperties requiredProperties = parentPreferences.withoutPreference().withDefaultParallelism(session);
                StreamPreferredProperties preferredProperties = parentPreferences.withDefaultParallelism(session);
                if (node.requiresPreSortedInputs()) {
                    requiredProperties = requiredProperties.withOrderSensitivity();
                    preferredProperties = preferredProperties.withOrderSensitivity();
                }
                return planAndEnforceChildren(node, requiredProperties, preferredProperties);
            }

            // final limit requires that all data be in one stream
            // also, a final changes the input organization completely, so we do not pass through parent preferences
            return planAndEnforceChildren(
                    node,
                    singleStream(),
                    defaultParallelism(session));
        }

        @Override
        public PlanWithProperties visitDistinctLimit(DistinctLimitNode node, StreamPreferredProperties parentPreferences)
        {
            // final limit requires that all data be in one stream
            StreamPreferredProperties requiredProperties;
            StreamPreferredProperties preferredProperties;
            if (node.isPartial()) {
                requiredProperties = parentPreferences.withoutPreference().withDefaultParallelism(session);
                preferredProperties = parentPreferences.withDefaultParallelism(session);
            }
            else {
                // a final changes the input organization completely, so we do not pass through parent preferences
                requiredProperties = singleStream();
                preferredProperties = defaultParallelism(session);
            }

            return planAndEnforceChildren(node, requiredProperties, preferredProperties);
        }

        @Override
        public PlanWithProperties visitEnforceSingleRow(EnforceSingleRowNode node, StreamPreferredProperties parentPreferences)
        {
            return planAndEnforceChildren(node, singleStream(), defaultParallelism(session));
        }

        //
        // Nodes that require parallel streams to be partitioned
        //

        @Override
        public PlanWithProperties visitAggregation(AggregationNode node, StreamPreferredProperties parentPreferences)
        {
            checkState(node.getStep() == AggregationNode.Step.SINGLE, "step of aggregation is expected to be SINGLE, but it is %s", node.getStep());

            if (node.hasSingleNodeExecutionPreference(plannerContext.getMetadata())) {
                return planAndEnforceChildren(node, singleStream(), defaultParallelism(session));
            }

            List<Symbol> groupingKeys = node.getGroupingKeys();
            if (node.hasDefaultOutput()) {
                checkState(node.isDecomposable(plannerContext.getMetadata()));

                // Put fixed local exchange directly below final aggregation to ensure that final and partial aggregations are separated by exchange (in a local runner mode)
                // This is required so that default outputs from multiple instances of partial aggregations are passed to a single final aggregation.
                PlanWithProperties child = planAndEnforce(node.getSource(), any(), defaultParallelism(session));
                PlanWithProperties exchange = deriveProperties(
                        partitionedExchange(
                                idAllocator.getNextId(),
                                LOCAL,
                                child.getNode(),
                                groupingKeys,
                                Optional.empty()),
                        child.getProperties());
                return rebaseAndDeriveProperties(node, ImmutableList.of(exchange));
            }

            StreamPreferredProperties childRequirements = parentPreferences
                    .constrainTo(node.getSource().getOutputSymbols())
                    .withDefaultParallelism(session)
                    .withPartitioning(groupingKeys);

            PlanWithProperties child = planAndEnforce(node.getSource(), childRequirements, childRequirements);

            List<Symbol> preGroupedSymbols = ImmutableList.of();
            if (LocalProperties.match(child.getProperties().getLocalProperties(), LocalProperties.grouped(groupingKeys)).get(0).isEmpty()) {
                // !isPresent() indicates the property was satisfied completely
                preGroupedSymbols = groupingKeys;
            }

            AggregationNode result = new AggregationNode(
                    node.getId(),
                    child.getNode(),
                    node.getAggregations(),
                    node.getGroupingSets(),
                    preGroupedSymbols,
                    node.getStep(),
                    node.getHashSymbol(),
                    node.getGroupIdSymbol());

            return deriveProperties(result, child.getProperties());
        }

        @Override
        public PlanWithProperties visitWindow(WindowNode node, StreamPreferredProperties parentPreferences)
        {
            StreamPreferredProperties childRequirements = parentPreferences
                    .constrainTo(node.getSource().getOutputSymbols())
                    .withDefaultParallelism(session)
                    .withPartitioning(node.getPartitionBy());

            PlanWithProperties child = planAndEnforce(node.getSource(), childRequirements, childRequirements);

            List<LocalProperty<Symbol>> desiredProperties = new ArrayList<>();
            if (!node.getPartitionBy().isEmpty()) {
                desiredProperties.add(new GroupingProperty<>(node.getPartitionBy()));
            }
            node.getOrderingScheme().ifPresent(orderingScheme -> desiredProperties.addAll(orderingScheme.toLocalProperties()));
            Iterator<Optional<LocalProperty<Symbol>>> matchIterator = LocalProperties.match(child.getProperties().getLocalProperties(), desiredProperties).iterator();

            Set<Symbol> prePartitionedInputs = ImmutableSet.of();
            if (!node.getPartitionBy().isEmpty()) {
                Optional<LocalProperty<Symbol>> groupingRequirement = matchIterator.next();
                Set<Symbol> unPartitionedInputs = groupingRequirement.map(LocalProperty::getColumns).orElse(ImmutableSet.of());
                prePartitionedInputs = node.getPartitionBy().stream()
                        .filter(symbol -> !unPartitionedInputs.contains(symbol))
                        .collect(toImmutableSet());
            }

            int preSortedOrderPrefix = 0;
            if (prePartitionedInputs.equals(ImmutableSet.copyOf(node.getPartitionBy()))) {
                while (matchIterator.hasNext() && matchIterator.next().isEmpty()) {
                    preSortedOrderPrefix++;
                }
            }

            WindowNode result = new WindowNode(
                    node.getId(),
                    child.getNode(),
                    node.getSpecification(),
                    node.getWindowFunctions(),
                    node.getHashSymbol(),
                    prePartitionedInputs,
                    preSortedOrderPrefix);

            return deriveProperties(result, child.getProperties());
        }

        @Override
        public PlanWithProperties visitPatternRecognition(PatternRecognitionNode node, StreamPreferredProperties parentPreferences)
        {
            StreamPreferredProperties childRequirements = parentPreferences
                    .constrainTo(node.getSource().getOutputSymbols())
                    .withDefaultParallelism(session)
                    .withPartitioning(node.getPartitionBy());

            PlanWithProperties child = planAndEnforce(node.getSource(), childRequirements, childRequirements);

            List<LocalProperty<Symbol>> desiredProperties = new ArrayList<>();
            if (!node.getPartitionBy().isEmpty()) {
                desiredProperties.add(new GroupingProperty<>(node.getPartitionBy()));
            }
            node.getOrderingScheme().ifPresent(orderingScheme -> desiredProperties.addAll(orderingScheme.toLocalProperties()));
            Iterator<Optional<LocalProperty<Symbol>>> matchIterator = LocalProperties.match(child.getProperties().getLocalProperties(), desiredProperties).iterator();

            Set<Symbol> prePartitionedInputs = ImmutableSet.of();
            if (!node.getPartitionBy().isEmpty()) {
                Optional<LocalProperty<Symbol>> groupingRequirement = matchIterator.next();
                Set<Symbol> unPartitionedInputs = groupingRequirement.map(LocalProperty::getColumns).orElse(ImmutableSet.of());
                prePartitionedInputs = node.getPartitionBy().stream()
                        .filter(symbol -> !unPartitionedInputs.contains(symbol))
                        .collect(toImmutableSet());
            }

            int preSortedOrderPrefix = 0;
            if (prePartitionedInputs.equals(ImmutableSet.copyOf(node.getPartitionBy()))) {
                while (matchIterator.hasNext() && matchIterator.next().isEmpty()) {
                    preSortedOrderPrefix++;
                }
            }

            PatternRecognitionNode result = new PatternRecognitionNode(
                    node.getId(),
                    child.getNode(),
                    node.getSpecification(),
                    node.getHashSymbol(),
                    prePartitionedInputs,
                    preSortedOrderPrefix,
                    node.getWindowFunctions(),
                    node.getMeasures(),
                    node.getCommonBaseFrame(),
                    node.getRowsPerMatch(),
                    node.getSkipToLabel(),
                    node.getSkipToPosition(),
                    node.isInitial(),
                    node.getPattern(),
                    node.getSubsets(),
                    node.getVariableDefinitions());

            return deriveProperties(result, child.getProperties());
        }

        @Override
        public PlanWithProperties visitMarkDistinct(MarkDistinctNode node, StreamPreferredProperties parentPreferences)
        {
            // mark distinct requires that all data partitioned
            StreamPreferredProperties childRequirements = parentPreferences
                    .constrainTo(node.getSource().getOutputSymbols())
                    .withDefaultParallelism(session)
                    .withPartitioning(node.getDistinctSymbols());

            PlanWithProperties child = planAndEnforce(node.getSource(), childRequirements, childRequirements);

            MarkDistinctNode result = new MarkDistinctNode(
                    node.getId(),
                    child.getNode(),
                    node.getMarkerSymbol(),
                    pruneMarkDistinctSymbols(node, child.getProperties().getLocalProperties()),
                    node.getHashSymbol());

            return deriveProperties(result, child.getProperties());
        }

        /**
         * Prune redundant distinct symbols to reduce CPU cost of hashing corresponding values and amount of memory
         * needed to store all the distinct values.
         * <p>
         * Consider the following plan,
         * <pre>
         *  - MarkDistinctNode (unique, c1, c2)
         *      - Join
         *          - AssignUniqueId (unique)
         *              - probe (c1, c2)
         *          - build
         * </pre>
         * In this case MarkDistinctNode (unique, c1, c2) is equivalent to MarkDistinctNode (unique),
         * because if two rows match on `unique`, they must match on `c1` and `c2` as well.
         * <p>
         * More generally, any distinct symbol that is functionally dependent on a subset of
         * other distinct symbols can be dropped.
         * <p>
         * Ideally, this logic would be encapsulated in a separate rule, but currently no rule other
         * than AddLocalExchanges can reason about local properties.
         */
        private List<Symbol> pruneMarkDistinctSymbols(MarkDistinctNode node, List<LocalProperty<Symbol>> localProperties)
        {
            if (localProperties.isEmpty()) {
                return node.getDistinctSymbols();
            }

            // Identify functional dependencies between distinct symbols: in the list of local properties any constant
            // symbol is functionally dependent on the set of symbols that appears earlier.
            ImmutableSet.Builder<Symbol> redundantSymbolsBuilder = ImmutableSet.builder();
            for (LocalProperty<Symbol> property : localProperties) {
                if (property instanceof ConstantProperty) {
                    redundantSymbolsBuilder.add(((ConstantProperty<Symbol>) property).getColumn());
                }
                else if (!node.getDistinctSymbols().containsAll(property.getColumns())) {
                    // Ran into a non-distinct symbol. There will be no more symbols that are functionally dependent on distinct symbols exclusively.
                    break;
                }
            }

            Set<Symbol> redundantSymbols = redundantSymbolsBuilder.build();
            List<Symbol> remainingSymbols = node.getDistinctSymbols().stream()
                    .filter(symbol -> !redundantSymbols.contains(symbol))
                    .collect(toImmutableList());
            if (remainingSymbols.isEmpty()) {
                // This happens when all distinct symbols are constants.
                // In that case, keep the first symbol (don't drop them all).
                return ImmutableList.of(node.getDistinctSymbols().get(0));
            }
            return remainingSymbols;
        }

        @Override
        public PlanWithProperties visitRowNumber(RowNumberNode node, StreamPreferredProperties parentPreferences)
        {
            StreamPreferredProperties requiredProperties;
            if (node.isOrderSensitive()) {
                // for an order sensitive RowNumberNode pass the orderSensitive context
                verify(node.getPartitionBy().isEmpty(), "unexpected partitioning");
                requiredProperties = singleStream().withOrderSensitivity();
            }
            else {
                requiredProperties = parentPreferences.withDefaultParallelism(session).withPartitioning(node.getPartitionBy());
            }
            return planAndEnforceChildren(node, requiredProperties, requiredProperties);
        }

        @Override
        public PlanWithProperties visitTopNRanking(TopNRankingNode node, StreamPreferredProperties parentPreferences)
        {
            StreamPreferredProperties requiredProperties = parentPreferences.withDefaultParallelism(session);

            // final topN ranking requires that all data be partitioned
            if (!node.isPartial()) {
                requiredProperties = requiredProperties.withPartitioning(node.getPartitionBy());
            }

            return planAndEnforceChildren(node, requiredProperties, requiredProperties);
        }

        @Override
        public PlanWithProperties visitSimpleTableExecuteNode(SimpleTableExecuteNode node, StreamPreferredProperties context)
        {
            return planAndEnforceChildren(node, singleStream(), singleStream());
        }

        //
        // Table Writer and Table Execute
        //

        @Override
        public PlanWithProperties visitTableWriter(TableWriterNode node, StreamPreferredProperties parentPreferences)
        {
            return visitTableWriter(node, node.getPartitioningScheme(), node.getSource(), parentPreferences);
        }

        @Override
        public PlanWithProperties visitTableExecute(TableExecuteNode node, StreamPreferredProperties parentPreferences)
        {
            return visitTableWriter(node, node.getPartitioningScheme(), node.getSource(), parentPreferences);
        }

        private PlanWithProperties visitTableWriter(PlanNode node, Optional<PartitioningScheme> partitioningSchemeOptional, PlanNode source, StreamPreferredProperties parentPreferences)
        {
            if (getTaskWriterCount(session) == 1) {
                return planAndEnforceChildren(node, singleStream(), defaultParallelism(session));
            }
            if (partitioningSchemeOptional.isEmpty()) {
                return planAndEnforceChildren(node, fixedParallelism(), fixedParallelism());
            }

            PartitioningScheme partitioningScheme = partitioningSchemeOptional.get();

            if (partitioningScheme.getPartitioning().getHandle().equals(FIXED_HASH_DISTRIBUTION)) {
                // arbitrary hash function on predefined set of partition columns
                StreamPreferredProperties preference = partitionedOn(partitioningScheme.getPartitioning().getColumns());
                return planAndEnforceChildren(node, preference, preference);
            }

            // connector provided hash function
            verify(!(partitioningScheme.getPartitioning().getHandle().getConnectorHandle() instanceof SystemPartitioningHandle));
            verify(
                    partitioningScheme.getPartitioning().getArguments().stream().noneMatch(Partitioning.ArgumentBinding::isConstant),
                    "Table writer partitioning has constant arguments");
            PlanWithProperties newSource = source.accept(this, parentPreferences);
            PlanWithProperties exchange = deriveProperties(
                    partitionedExchange(
                            idAllocator.getNextId(),
                            LOCAL,
                            newSource.getNode(),
                            partitioningScheme),
                    newSource.getProperties());

            return rebaseAndDeriveProperties(node, ImmutableList.of(exchange));
        }

        //
        // Exchanges
        //

        @Override
        public PlanWithProperties visitExchange(ExchangeNode node, StreamPreferredProperties parentPreferences)
        {
            checkArgument(node.getScope() != LOCAL, "AddLocalExchanges cannot process a plan containing a local exchange");
            // this node changes the input organization completely, so we do not pass through parent preferences
            if (node.getOrderingScheme().isPresent()) {
                return planAndEnforceChildren(
                        node,
                        any().withOrderSensitivity(),
                        any().withOrderSensitivity());
            }
            return planAndEnforceChildren(node, any(), defaultParallelism(session));
        }

        @Override
        public PlanWithProperties visitUnion(UnionNode node, StreamPreferredProperties preferredProperties)
        {
            // Union is replaced with an exchange which does not retain streaming properties from the children
            List<PlanWithProperties> sourcesWithProperties = node.getSources().stream()
                    .map(source -> source.accept(this, any()))
                    .collect(toImmutableList());

            List<PlanNode> sources = sourcesWithProperties.stream()
                    .map(PlanWithProperties::getNode)
                    .collect(toImmutableList());

            List<StreamProperties> inputProperties = sourcesWithProperties.stream()
                    .map(PlanWithProperties::getProperties)
                    .collect(toImmutableList());

            List<List<Symbol>> inputLayouts = new ArrayList<>(sources.size());
            for (int i = 0; i < sources.size(); i++) {
                inputLayouts.add(node.sourceOutputLayout(i));
            }

            if (preferredProperties.isSingleStreamPreferred()) {
                ExchangeNode exchangeNode = new ExchangeNode(
                        idAllocator.getNextId(),
                        GATHER,
                        LOCAL,
                        new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), node.getOutputSymbols()),
                        sources,
                        inputLayouts,
                        Optional.empty());
                return deriveProperties(exchangeNode, inputProperties);
            }

            Optional<List<Symbol>> preferredPartitionColumns = preferredProperties.getPartitioningColumns();
            if (preferredPartitionColumns.isPresent()) {
                ExchangeNode exchangeNode = new ExchangeNode(
                        idAllocator.getNextId(),
                        REPARTITION,
                        LOCAL,
                        new PartitioningScheme(
                                Partitioning.create(FIXED_HASH_DISTRIBUTION, preferredPartitionColumns.get()),
                                node.getOutputSymbols(),
                                Optional.empty()),
                        sources,
                        inputLayouts,
                        Optional.empty());
                return deriveProperties(exchangeNode, inputProperties);
            }

            // multiple streams preferred
            ExchangeNode exchangeNode = new ExchangeNode(
                    idAllocator.getNextId(),
                    REPARTITION,
                    LOCAL,
                    new PartitioningScheme(Partitioning.create(FIXED_ARBITRARY_DISTRIBUTION, ImmutableList.of()), node.getOutputSymbols()),
                    sources,
                    inputLayouts,
                    Optional.empty());

            return deriveProperties(exchangeNode, inputProperties);
        }

        //
        // Joins
        //

        @Override
        public PlanWithProperties visitJoin(JoinNode node, StreamPreferredProperties parentPreferences)
        {
            PlanWithProperties probe = planAndEnforce(
                    node.getLeft(),
                    defaultParallelism(session),
                    parentPreferences.constrainTo(node.getLeft().getOutputSymbols()).withDefaultParallelism(session));

            if (isSpillEnabled(session)) {
                if (probe.getProperties().getDistribution() != FIXED) {
                    // Disable spill for joins over non-fixed streams as otherwise we would need to insert local exchange.
                    // Such local exchanges can hurt performance when spill is not triggered.
                    // When spill is not triggered it should not induce performance penalty.
                    node = node.withSpillable(false);
                }
                else {
                    node = node.withSpillable(true);
                }
            }

            // this build consumes the input completely, so we do not pass through parent preferences
            List<Symbol> buildHashSymbols = Lists.transform(node.getCriteria(), JoinNode.EquiJoinClause::getRight);
            StreamPreferredProperties buildPreference;
            if (getTaskConcurrency(session) > 1) {
                buildPreference = exactlyPartitionedOn(buildHashSymbols);
            }
            else {
                buildPreference = singleStream();
            }
            PlanWithProperties build = planAndEnforce(node.getRight(), buildPreference, buildPreference);

            return rebaseAndDeriveProperties(node, ImmutableList.of(probe, build));
        }

        @Override
        public PlanWithProperties visitSemiJoin(SemiJoinNode node, StreamPreferredProperties parentPreferences)
        {
            PlanWithProperties source = planAndEnforce(
                    node.getSource(),
                    defaultParallelism(session),
                    parentPreferences.constrainTo(node.getSource().getOutputSymbols()).withDefaultParallelism(session));

            // this filter source consumes the input completely, so we do not pass through parent preferences
            PlanWithProperties filteringSource = planAndEnforce(node.getFilteringSource(), singleStream(), singleStream());

            return rebaseAndDeriveProperties(node, ImmutableList.of(source, filteringSource));
        }

        @Override
        public PlanWithProperties visitSpatialJoin(SpatialJoinNode node, StreamPreferredProperties parentPreferences)
        {
            PlanWithProperties probe = planAndEnforce(
                    node.getLeft(),
                    defaultParallelism(session),
                    parentPreferences.constrainTo(node.getLeft().getOutputSymbols())
                            .withDefaultParallelism(session));

            PlanWithProperties build = planAndEnforce(node.getRight(), singleStream(), singleStream());

            return rebaseAndDeriveProperties(node, ImmutableList.of(probe, build));
        }

        @Override
        public PlanWithProperties visitIndexJoin(IndexJoinNode node, StreamPreferredProperties parentPreferences)
        {
            PlanWithProperties probe = planAndEnforce(
                    node.getProbeSource(),
                    defaultParallelism(session),
                    parentPreferences.constrainTo(node.getProbeSource().getOutputSymbols()).withDefaultParallelism(session));

            // index source does not support local parallel and must produce a single stream
            StreamProperties indexStreamProperties = derivePropertiesRecursively(node.getIndexSource(), plannerContext, session, types, typeAnalyzer);
            checkArgument(indexStreamProperties.getDistribution() == SINGLE, "index source must be single stream");
            PlanWithProperties index = new PlanWithProperties(node.getIndexSource(), indexStreamProperties);

            return rebaseAndDeriveProperties(node, ImmutableList.of(probe, index));
        }

        //
        // Helpers
        //

        private PlanWithProperties planAndEnforceChildren(PlanNode node, StreamPreferredProperties requiredProperties, StreamPreferredProperties preferredProperties)
        {
            // plan and enforce each child, but strip any requirement not in terms of symbols produced from the child
            // Note: this assumes the child uses the same symbols as the parent
            List<PlanWithProperties> children = node.getSources().stream()
                    .map(source -> planAndEnforce(
                            source,
                            requiredProperties.constrainTo(source.getOutputSymbols()),
                            preferredProperties.constrainTo(source.getOutputSymbols())))
                    .collect(toImmutableList());

            return rebaseAndDeriveProperties(node, children);
        }

        private PlanWithProperties planAndEnforce(PlanNode node, StreamPreferredProperties requiredProperties, StreamPreferredProperties preferredProperties)
        {
            // verify properties are in terms of symbols produced by the node
            List<Symbol> outputSymbols = node.getOutputSymbols();
            checkArgument(requiredProperties.getPartitioningColumns().map(outputSymbols::containsAll).orElse(true));
            checkArgument(preferredProperties.getPartitioningColumns().map(outputSymbols::containsAll).orElse(true));

            // plan the node using the preferred properties
            PlanWithProperties result = node.accept(this, preferredProperties);

            // enforce the required properties
            result = enforce(result, requiredProperties);

            checkState(requiredProperties.isSatisfiedBy(result.getProperties()), "required properties not enforced");
            return result;
        }

        private PlanWithProperties enforce(PlanWithProperties planWithProperties, StreamPreferredProperties requiredProperties)
        {
            if (requiredProperties.isSatisfiedBy(planWithProperties.getProperties())) {
                return planWithProperties;
            }

            if (requiredProperties.isSingleStreamPreferred()) {
                ExchangeNode exchangeNode = gatheringExchange(idAllocator.getNextId(), LOCAL, planWithProperties.getNode());
                return deriveProperties(exchangeNode, planWithProperties.getProperties());
            }

            Optional<List<Symbol>> requiredPartitionColumns = requiredProperties.getPartitioningColumns();
            if (requiredPartitionColumns.isEmpty()) {
                // unpartitioned parallel streams required
                ExchangeNode exchangeNode = partitionedExchange(
                        idAllocator.getNextId(),
                        LOCAL,
                        planWithProperties.getNode(),
                        new PartitioningScheme(Partitioning.create(FIXED_ARBITRARY_DISTRIBUTION, ImmutableList.of()), planWithProperties.getNode().getOutputSymbols()));

                return deriveProperties(exchangeNode, planWithProperties.getProperties());
            }

            if (requiredProperties.isParallelPreferred()) {
                // partitioned parallel streams required
                ExchangeNode exchangeNode = partitionedExchange(
                        idAllocator.getNextId(),
                        LOCAL,
                        planWithProperties.getNode(),
                        requiredPartitionColumns.get(),
                        Optional.empty());
                return deriveProperties(exchangeNode, planWithProperties.getProperties());
            }

            // no explicit parallel requirement, so gather to a single stream
            ExchangeNode exchangeNode = gatheringExchange(
                    idAllocator.getNextId(),
                    LOCAL,
                    planWithProperties.getNode());
            return deriveProperties(exchangeNode, planWithProperties.getProperties());
        }

        private PlanWithProperties rebaseAndDeriveProperties(PlanNode node, List<PlanWithProperties> children)
        {
            PlanNode result = replaceChildren(
                    node,
                    children.stream()
                            .map(PlanWithProperties::getNode)
                            .collect(toList()));

            List<StreamProperties> inputProperties = children.stream()
                    .map(PlanWithProperties::getProperties)
                    .collect(toImmutableList());

            return deriveProperties(result, inputProperties);
        }

        private PlanWithProperties deriveProperties(PlanNode result, StreamProperties inputProperties)
        {
            return new PlanWithProperties(result, StreamPropertyDerivations.deriveProperties(result, inputProperties, plannerContext, session, types, typeAnalyzer));
        }

        private PlanWithProperties deriveProperties(PlanNode result, List<StreamProperties> inputProperties)
        {
            return new PlanWithProperties(result, StreamPropertyDerivations.deriveProperties(result, inputProperties, plannerContext, session, types, typeAnalyzer));
        }
    }

    private static class PlanWithProperties
    {
        private final PlanNode node;
        private final StreamProperties properties;

        public PlanWithProperties(PlanNode node, StreamProperties properties)
        {
            this.node = requireNonNull(node, "node is null");
            this.properties = requireNonNull(properties, "properties is null");
        }

        public PlanNode getNode()
        {
            return node;
        }

        public StreamProperties getProperties()
        {
            return properties;
        }
    }
}
