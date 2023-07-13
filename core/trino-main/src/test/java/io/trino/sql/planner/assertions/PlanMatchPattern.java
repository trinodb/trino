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
package io.trino.sql.planner.assertions;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.trino.Session;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.StatsProvider;
import io.trino.metadata.Metadata;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.GroupReference;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.optimizations.SymbolMapper;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Step;
import io.trino.sql.planner.plan.ApplyNode;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.DistinctLimitNode;
import io.trino.sql.planner.plan.EnforceSingleRowNode;
import io.trino.sql.planner.plan.ExceptNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.GroupIdNode;
import io.trino.sql.planner.plan.IndexJoinNode;
import io.trino.sql.planner.plan.IndexSourceNode;
import io.trino.sql.planner.plan.IntersectNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinNode.Type;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.MergeWriterNode;
import io.trino.sql.planner.plan.OffsetNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.SpatialJoinNode;
import io.trino.sql.planner.plan.TableExecuteNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FrameBound;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.SortItem;
import io.trino.sql.tree.WindowFrame;
import org.intellij.lang.annotations.Language;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.connector.SortOrder.DESC_NULLS_FIRST;
import static io.trino.spi.connector.SortOrder.DESC_NULLS_LAST;
import static io.trino.sql.planner.assertions.MatchResult.NO_MATCH;
import static io.trino.sql.planner.assertions.MatchResult.match;
import static io.trino.sql.planner.assertions.StrictAssignedSymbolsMatcher.actualAssignments;
import static io.trino.sql.planner.assertions.StrictSymbolsMatcher.actualOutputs;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.sql.tree.ComparisonExpression.Operator.IS_DISTINCT_FROM;
import static io.trino.sql.tree.SortItem.NullOrdering.FIRST;
import static io.trino.sql.tree.SortItem.NullOrdering.UNDEFINED;
import static io.trino.sql.tree.SortItem.Ordering.ASCENDING;
import static io.trino.sql.tree.SortItem.Ordering.DESCENDING;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

public final class PlanMatchPattern
{
    private final List<Matcher> matchers = new ArrayList<>();

    private final List<PlanMatchPattern> sourcePatterns;
    private boolean anyTree;

    public static PlanMatchPattern node(Class<? extends PlanNode> nodeClass, PlanMatchPattern... sources)
    {
        return any(sources).with(new PlanNodeMatcher(nodeClass));
    }

    public static PlanMatchPattern any(PlanMatchPattern... sources)
    {
        return new PlanMatchPattern(ImmutableList.copyOf(sources));
    }

    /**
     * Matches to any tree of nodes with children matching to given source matchers.
     * anyTree(tableScan("nation")) - will match to any plan which all leafs contain
     * any node containing table scan from nation table.
     * <p>
     * Note: anyTree does not match zero nodes. E.g. output(anyTree(tableScan)) will NOT match TableScan node followed by OutputNode.
     */
    public static PlanMatchPattern anyTree(PlanMatchPattern... sources)
    {
        return any(sources).matchToAnyNodeTree();
    }

    public static PlanMatchPattern anyNot(Class<? extends PlanNode> excludeNodeClass, PlanMatchPattern... sources)
    {
        return any(sources).with(new NotPlanNodeMatcher(excludeNodeClass));
    }

    public static PlanMatchPattern tableScan(String expectedTableName)
    {
        return node(TableScanNode.class)
                .with(new TableScanMatcher(
                        expectedTableName,
                        Optional.empty(),
                        Optional.empty()));
    }

    public static PlanMatchPattern tableScan(String expectedTableName, Map<String, String> columnReferences)
    {
        PlanMatchPattern result = tableScan(expectedTableName);
        return result.addColumnReferences(expectedTableName, columnReferences);
    }

    public static PlanMatchPattern tableScan(
            Predicate<ConnectorTableHandle> expectedTable,
            TupleDomain<Predicate<ColumnHandle>> enforcedConstraints,
            Map<String, Predicate<ColumnHandle>> expectedColumns)
    {
        return tableScan(expectedTable, enforcedConstraints, expectedColumns, statistics -> true);
    }

    public static PlanMatchPattern tableScan(
            Predicate<ConnectorTableHandle> expectedTable,
            TupleDomain<Predicate<ColumnHandle>> enforcedConstraints,
            Map<String, Predicate<ColumnHandle>> expectedColumns,
            Predicate<Optional<PlanNodeStatsEstimate>> expectedStatistics)
    {
        PlanMatchPattern pattern = ConnectorAwareTableScanMatcher.create(expectedTable, enforcedConstraints, expectedStatistics);
        expectedColumns.entrySet().forEach(column -> pattern.withAlias(column.getKey(), new ColumnHandleMatcher(column.getValue())));
        return pattern;
    }

    public static PlanMatchPattern strictTableScan(String expectedTableName, Map<String, String> columnReferences)
    {
        return tableScan(expectedTableName, columnReferences)
                .withExactAssignedOutputs(columnReferences.values().stream()
                        .map(columnName -> columnReference(expectedTableName, columnName))
                        .collect(toImmutableList()));
    }

    public static PlanMatchPattern strictConstrainedTableScan(String expectedTableName, Map<String, String> columnReferences, Map<String, Domain> constraint)
    {
        return strictTableScan(expectedTableName, columnReferences)
                .with(new TableScanMatcher(
                        expectedTableName,
                        Optional.of(constraint),
                        Optional.empty()));
    }

    public static PlanMatchPattern constrainedTableScan(String expectedTableName, Map<String, Domain> constraint)
    {
        return node(TableScanNode.class)
                .with(new TableScanMatcher(
                        expectedTableName,
                        Optional.of(constraint),
                        Optional.empty()));
    }

    public static PlanMatchPattern constrainedTableScan(String expectedTableName, Map<String, Domain> constraint, Map<String, String> columnReferences)
    {
        PlanMatchPattern result = constrainedTableScan(expectedTableName, constraint);
        return result.addColumnReferences(expectedTableName, columnReferences);
    }

    public static PlanMatchPattern constrainedTableScanWithTableLayout(String expectedTableName, Map<String, Domain> constraint, Map<String, String> columnReferences)
    {
        return node(TableScanNode.class)
                .with(new TableScanMatcher(
                        expectedTableName,
                        Optional.of(constraint),
                        Optional.of(true)))
                .addColumnReferences(expectedTableName, columnReferences);
    }

    public static PlanMatchPattern indexJoin(
            IndexJoinNode.Type type,
            List<ExpectedValueProvider<IndexJoinNode.EquiJoinClause>> criteria,
            Optional<String> probeHashSymbol,
            Optional<String> indexHashSymbol,
            PlanMatchPattern probeSource,
            PlanMatchPattern indexSource)
    {
        return node(IndexJoinNode.class, probeSource, indexSource)
                .with(new IndexJoinMatcher(type, criteria, probeHashSymbol.map(SymbolAlias::new), indexHashSymbol.map(SymbolAlias::new)));
    }

    public static ExpectedValueProvider<IndexJoinNode.EquiJoinClause> indexJoinEquiClause(String probe, String index)
    {
        return new IndexJoinEquiClauseProvider(new SymbolAlias(probe), new SymbolAlias(index));
    }

    public static PlanMatchPattern constrainedIndexSource(String expectedTableName, Map<String, String> columnReferences)
    {
        return node(IndexSourceNode.class)
                .with(new IndexSourceMatcher(expectedTableName))
                .addColumnReferences(expectedTableName, columnReferences);
    }

    private PlanMatchPattern addColumnReferences(String expectedTableName, Map<String, String> columnReferences)
    {
        columnReferences.entrySet().forEach(
                reference -> withAlias(reference.getKey(), columnReference(expectedTableName, reference.getValue())));
        return this;
    }

    public static PlanMatchPattern aggregation(
            Map<String, ExpectedValueProvider<FunctionCall>> aggregations,
            PlanMatchPattern source)
    {
        PlanMatchPattern result = node(AggregationNode.class, source);
        aggregations.entrySet().forEach(
                aggregation -> result.withAlias(aggregation.getKey(), new AggregationFunctionMatcher(aggregation.getValue())));
        return result;
    }

    public static PlanMatchPattern aggregation(
            Map<String, ExpectedValueProvider<FunctionCall>> aggregations,
            Step step,
            PlanMatchPattern source)
    {
        PlanMatchPattern result = node(AggregationNode.class, source).with(new AggregationStepMatcher(step));
        aggregations.entrySet().forEach(
                aggregation -> result.withAlias(aggregation.getKey(), new AggregationFunctionMatcher(aggregation.getValue())));
        return result;
    }

    public static PlanMatchPattern aggregation(
            Map<String, ExpectedValueProvider<FunctionCall>> aggregations,
            Predicate<AggregationNode> predicate,
            PlanMatchPattern source)
    {
        PlanMatchPattern result = node(AggregationNode.class, source)
                .with(new PredicateMatcher<>(predicate));
        aggregations.entrySet().forEach(
                aggregation -> result.withAlias(aggregation.getKey(), new AggregationFunctionMatcher(aggregation.getValue())));
        return result;
    }

    public static PlanMatchPattern aggregation(
            GroupingSetDescriptor groupingSets,
            Map<Optional<String>, ExpectedValueProvider<FunctionCall>> aggregations,
            Optional<Symbol> groupId,
            Step step,
            PlanMatchPattern source)
    {
        return aggregation(groupingSets, aggregations, ImmutableList.of(), groupId, step, source);
    }

    public static PlanMatchPattern aggregation(
            GroupingSetDescriptor groupingSets,
            Map<Optional<String>, ExpectedValueProvider<FunctionCall>> aggregations,
            List<String> preGroupedSymbols,
            Optional<Symbol> groupId,
            Step step,
            PlanMatchPattern source)
    {
        return aggregation(groupingSets, aggregations, preGroupedSymbols, ImmutableList.of(), groupId, step, source);
    }

    public static PlanMatchPattern aggregation(
            GroupingSetDescriptor groupingSets,
            Map<Optional<String>, ExpectedValueProvider<FunctionCall>> aggregations,
            List<String> preGroupedSymbols,
            List<String> masks,
            Optional<Symbol> groupId,
            Step step,
            PlanMatchPattern source)
    {
        PlanMatchPattern result = node(AggregationNode.class, source).with(new AggregationMatcher(groupingSets, preGroupedSymbols, masks, groupId, step));
        aggregations.entrySet().forEach(
                aggregation -> result.withAlias(aggregation.getKey(), new AggregationFunctionMatcher(aggregation.getValue())));
        return result;
    }

    public static PlanMatchPattern distinctLimit(long limit, List<String> distinctSymbols, PlanMatchPattern source)
    {
        return node(DistinctLimitNode.class, source).with(new DistinctLimitMatcher(
                limit,
                toSymbolAliases(distinctSymbols),
                Optional.empty()));
    }

    public static PlanMatchPattern distinctLimit(long limit, List<String> distinctSymbols, String hashSymbol, PlanMatchPattern source)
    {
        return node(DistinctLimitNode.class, source).with(new DistinctLimitMatcher(
                limit,
                toSymbolAliases(distinctSymbols),
                Optional.of(new SymbolAlias(hashSymbol))));
    }

    public static PlanMatchPattern markDistinct(
            String markerSymbol,
            List<String> distinctSymbols,
            PlanMatchPattern source)
    {
        return node(MarkDistinctNode.class, source).with(new MarkDistinctMatcher(
                new SymbolAlias(markerSymbol),
                toSymbolAliases(distinctSymbols),
                Optional.empty()));
    }

    public static PlanMatchPattern markDistinct(
            String markerSymbol,
            List<String> distinctSymbols,
            String hashSymbol,
            PlanMatchPattern source)
    {
        return node(MarkDistinctNode.class, source).with(new MarkDistinctMatcher(
                new SymbolAlias(markerSymbol),
                toSymbolAliases(distinctSymbols),
                Optional.of(new SymbolAlias(hashSymbol))));
    }

    public static ExpectedValueProvider<WindowNode.Frame> windowFrame(
            WindowFrame.Type type,
            FrameBound.Type startType,
            Optional<String> startValue,
            FrameBound.Type endType,
            Optional<String> endValue,
            Optional<String> sortKey)
    {
        return windowFrame(type, startType, startValue, sortKey, endType, endValue, sortKey);
    }

    public static ExpectedValueProvider<WindowNode.Frame> windowFrame(
            WindowFrame.Type type,
            FrameBound.Type startType,
            Optional<String> startValue,
            Optional<String> sortKeyForStartComparison,
            FrameBound.Type endType,
            Optional<String> endValue,
            Optional<String> sortKeyForEndComparison)
    {
        return new WindowFrameProvider(
                type,
                startType,
                startValue.map(SymbolAlias::new),
                sortKeyForStartComparison.map(SymbolAlias::new),
                endType,
                endValue.map(SymbolAlias::new),
                sortKeyForEndComparison.map(SymbolAlias::new));
    }

    public static PlanMatchPattern window(Consumer<WindowMatcher.Builder> handler, PlanMatchPattern source)
    {
        WindowMatcher.Builder builder = new WindowMatcher.Builder(source);
        handler.accept(builder);
        return builder.build();
    }

    public static PlanMatchPattern rowNumber(Consumer<RowNumberMatcher.Builder> handler, PlanMatchPattern source)
    {
        RowNumberMatcher.Builder builder = new RowNumberMatcher.Builder(source);
        handler.accept(builder);
        return builder.build();
    }

    public static PlanMatchPattern topNRanking(Consumer<TopNRankingMatcher.Builder> handler, PlanMatchPattern source)
    {
        TopNRankingMatcher.Builder builder = new TopNRankingMatcher.Builder(source);
        handler.accept(builder);
        return builder.build();
    }

    public static PlanMatchPattern patternRecognition(Consumer<PatternRecognitionMatcher.Builder> handler, PlanMatchPattern source)
    {
        PatternRecognitionMatcher.Builder builder = new PatternRecognitionMatcher.Builder(source);
        handler.accept(builder);
        return builder.build();
    }

    public static PlanMatchPattern join(JoinNode.Type type, Consumer<JoinMatcher.Builder> handler)
    {
        JoinMatcher.Builder builder = new JoinMatcher.Builder(type);
        handler.accept(builder);
        return builder.build();
    }

    public static PlanMatchPattern sort(PlanMatchPattern source)
    {
        return node(SortNode.class, source);
    }

    public static PlanMatchPattern sort(List<Ordering> orderBy, PlanMatchPattern source)
    {
        return node(SortNode.class, source)
                .with(new SortMatcher(orderBy));
    }

    public static PlanMatchPattern topN(long count, List<Ordering> orderBy, PlanMatchPattern source)
    {
        return topN(count, orderBy, TopNNode.Step.SINGLE, source);
    }

    public static PlanMatchPattern topN(long count, List<Ordering> orderBy, TopNNode.Step step, PlanMatchPattern source)
    {
        return node(TopNNode.class, source).with(new TopNMatcher(count, orderBy, step));
    }

    public static PlanMatchPattern output(PlanMatchPattern source)
    {
        return node(OutputNode.class, source);
    }

    public static PlanMatchPattern output(List<String> outputs, PlanMatchPattern source)
    {
        PlanMatchPattern result = output(source);
        result.withOutputs(outputs);
        return result;
    }

    public static PlanMatchPattern strictOutput(List<String> outputs, PlanMatchPattern source)
    {
        return output(outputs, source).withExactOutputs(outputs);
    }

    public static PlanMatchPattern project(PlanMatchPattern source)
    {
        return node(ProjectNode.class, source);
    }

    public static PlanMatchPattern project(Map<String, ExpressionMatcher> assignments, PlanMatchPattern source)
    {
        PlanMatchPattern result = project(source);
        assignments.entrySet().forEach(
                assignment -> result.withAlias(assignment.getKey(), assignment.getValue()));
        return result;
    }

    public static PlanMatchPattern identityProject(PlanMatchPattern source)
    {
        return node(ProjectNode.class, source).with(new IdentityProjectionMatcher());
    }

    public static PlanMatchPattern strictProject(Map<String, ExpressionMatcher> assignments, PlanMatchPattern source)
    {
        /*
         * Under the current implementation of project, all of the outputs are also in the assignment.
         * If the implementation changes, this will need to change too.
         */
        return project(assignments, source)
                .withExactAssignedOutputs(assignments.values())
                .withExactAssignments(assignments.values());
    }

    public static PlanMatchPattern semiJoin(String sourceSymbolAlias, String filteringSymbolAlias, String outputAlias, PlanMatchPattern source, PlanMatchPattern filtering)
    {
        return semiJoin(sourceSymbolAlias, filteringSymbolAlias, outputAlias, Optional.empty(), Optional.empty(), source, filtering);
    }

    public static PlanMatchPattern semiJoin(String sourceSymbolAlias, String filteringSymbolAlias, String outputAlias, Optional<SemiJoinNode.DistributionType> distributionType, PlanMatchPattern source, PlanMatchPattern filtering)
    {
        return semiJoin(sourceSymbolAlias, filteringSymbolAlias, outputAlias, distributionType, Optional.empty(), source, filtering);
    }

    public static PlanMatchPattern semiJoin(String sourceSymbolAlias, String filteringSymbolAlias, String outputAlias, boolean hasDynamicFilter, PlanMatchPattern source, PlanMatchPattern filtering)
    {
        return semiJoin(sourceSymbolAlias, filteringSymbolAlias, outputAlias, Optional.empty(), Optional.of(hasDynamicFilter), source, filtering);
    }

    public static PlanMatchPattern semiJoin(String sourceSymbolAlias, String filteringSymbolAlias, String outputAlias, Optional<SemiJoinNode.DistributionType> distributionType, Optional<Boolean> hasDynamicFilter, PlanMatchPattern source, PlanMatchPattern filtering)
    {
        return node(SemiJoinNode.class, source, filtering).with(new SemiJoinMatcher(sourceSymbolAlias, filteringSymbolAlias, outputAlias, distributionType, hasDynamicFilter));
    }

    public static PlanMatchPattern spatialJoin(@Language("SQL") String expectedFilter, PlanMatchPattern left, PlanMatchPattern right)
    {
        return spatialJoin(expectedFilter, Optional.empty(), left, right);
    }

    public static PlanMatchPattern spatialJoin(@Language("SQL") String expectedFilter, Optional<String> kdbTree, PlanMatchPattern left, PlanMatchPattern right)
    {
        return spatialJoin(expectedFilter, kdbTree, Optional.empty(), left, right);
    }

    public static PlanMatchPattern spatialJoin(@Language("SQL") String expectedFilter, Optional<String> kdbTree, Optional<List<String>> outputSymbols, PlanMatchPattern left, PlanMatchPattern right)
    {
        return node(SpatialJoinNode.class, left, right).with(
                new SpatialJoinMatcher(SpatialJoinNode.Type.INNER, PlanBuilder.expression(expectedFilter), kdbTree, outputSymbols));
    }

    public static PlanMatchPattern spatialLeftJoin(@Language("SQL") String expectedFilter, PlanMatchPattern left, PlanMatchPattern right)
    {
        return node(SpatialJoinNode.class, left, right).with(
                new SpatialJoinMatcher(SpatialJoinNode.Type.LEFT, PlanBuilder.expression(expectedFilter), Optional.empty(), Optional.empty()));
    }

    public static PlanMatchPattern unnest(PlanMatchPattern source)
    {
        return node(UnnestNode.class, source);
    }

    public static PlanMatchPattern unnest(List<String> replicateSymbols, List<UnnestMapping> mappings, PlanMatchPattern source)
    {
        return unnest(replicateSymbols, mappings, Optional.empty(), INNER, Optional.empty(), source);
    }

    public static PlanMatchPattern unnest(
            List<String> replicateSymbols,
            List<UnnestMapping> mappings,
            Optional<String> ordinalitySymbol,
            Type type,
            Optional<String> filter,
            PlanMatchPattern source)
    {
        PlanMatchPattern result = node(UnnestNode.class, source)
                .with(new UnnestMatcher(
                        replicateSymbols,
                        mappings,
                        ordinalitySymbol,
                        type,
                        filter.map(predicate -> PlanBuilder.expression(predicate))));

        mappings.forEach(mapping -> {
            for (int i = 0; i < mapping.getOutputs().size(); i++) {
                result.withAlias(mapping.getOutputs().get(i), new UnnestedSymbolMatcher(mapping.getInput(), i));
            }
        });

        ordinalitySymbol.ifPresent(symbol -> result.withAlias(symbol, new OrdinalitySymbolMatcher()));

        return result;
    }

    public static PlanMatchPattern exchange(PlanMatchPattern... sources)
    {
        return node(ExchangeNode.class, sources);
    }

    public static PlanMatchPattern exchange(ExchangeNode.Scope scope, PlanMatchPattern... sources)
    {
        return exchange(scope, Optional.empty(), Optional.empty(), ImmutableList.of(), ImmutableSet.of(), Optional.empty(), sources);
    }

    public static PlanMatchPattern exchange(ExchangeNode.Scope scope, ExchangeNode.Type type, PlanMatchPattern... sources)
    {
        return exchange(scope, type, ImmutableList.of(), sources);
    }

    public static PlanMatchPattern exchange(ExchangeNode.Scope scope, ExchangeNode.Type type, PartitioningHandle partitioningHandle, PlanMatchPattern... sources)
    {
        return exchange(scope, Optional.of(type), Optional.of(partitioningHandle), ImmutableList.of(), ImmutableSet.of(), Optional.empty(), sources);
    }

    public static PlanMatchPattern exchange(ExchangeNode.Scope scope, ExchangeNode.Type type, List<Ordering> orderBy, PlanMatchPattern... sources)
    {
        return exchange(scope, type, orderBy, ImmutableSet.of(), sources);
    }

    public static PlanMatchPattern exchange(ExchangeNode.Scope scope, ExchangeNode.Type type, List<Ordering> orderBy, Set<String> partitionedBy, PlanMatchPattern... sources)
    {
        return exchange(scope, type, orderBy, partitionedBy, Optional.empty(), sources);
    }

    public static PlanMatchPattern exchange(
            ExchangeNode.Scope scope,
            ExchangeNode.Type type,
            List<Ordering> orderBy,
            Set<String> partitionedBy,
            Optional<List<List<String>>> inputs,
            PlanMatchPattern... sources)
    {
        return exchange(scope, Optional.of(type), Optional.empty(), orderBy, partitionedBy, inputs, sources);
    }

    public static PlanMatchPattern exchange(
            ExchangeNode.Scope scope,
            Optional<ExchangeNode.Type> type,
            Optional<PartitioningHandle> partitioningHandle,
            List<Ordering> orderBy,
            Set<String> partitionedBy,
            Optional<List<List<String>>> inputs,
            PlanMatchPattern... sources)
    {
        return exchange(scope, type, partitioningHandle, orderBy, partitionedBy, inputs, ImmutableList.of(), Optional.empty(), sources);
    }

    public static PlanMatchPattern exchange(ExchangeNode.Scope scope, Optional<Integer> partitionCount, PlanMatchPattern... sources)
    {
        return exchange(scope, Optional.empty(), Optional.empty(), ImmutableList.of(), ImmutableSet.of(), Optional.empty(), ImmutableList.of(), Optional.of(partitionCount), sources);
    }

    public static PlanMatchPattern exchange(ExchangeNode.Scope scope, ExchangeNode.Type type, Optional<Integer> partitionCount, PlanMatchPattern... sources)
    {
        return exchange(scope, Optional.of(type), Optional.empty(), ImmutableList.of(), ImmutableSet.of(), Optional.empty(), ImmutableList.of(), Optional.of(partitionCount), sources);
    }

    public static PlanMatchPattern exchange(ExchangeNode.Scope scope, PartitioningHandle partitioningHandle, Optional<Integer> partitionCount, PlanMatchPattern... sources)
    {
        return exchange(scope, Optional.empty(), Optional.of(partitioningHandle), ImmutableList.of(), ImmutableSet.of(), Optional.empty(), ImmutableList.of(), Optional.of(partitionCount), sources);
    }

    public static PlanMatchPattern exchange(
            ExchangeNode.Scope scope,
            Optional<ExchangeNode.Type> type,
            Optional<PartitioningHandle> partitioningHandle,
            List<Ordering> orderBy,
            Set<String> partitionedBy,
            Optional<List<List<String>>> inputs,
            List<String> outputSymbolAliases,
            Optional<Optional<Integer>> partitionCount,
            PlanMatchPattern... sources)
    {
        PlanMatchPattern result = node(ExchangeNode.class, sources)
                .with(new ExchangeMatcher(scope, type, partitioningHandle, orderBy, partitionedBy, inputs, partitionCount));

        for (int i = 0; i < outputSymbolAliases.size(); i++) {
            String outputSymbol = outputSymbolAliases.get(i);
            int index = i;
            result.withAlias(outputSymbol, (node, session, metadata, symbolAliases) -> {
                ExchangeNode exchangeNode = (ExchangeNode) node;
                List<Symbol> outputSymbols = exchangeNode.getPartitioningScheme().getOutputLayout();
                checkState(index < outputSymbols.size(), "outputSymbolAliases size is more than exchange output symbols");
                return Optional.ofNullable(outputSymbols.get(index));
            });
        }
        return result;
    }

    public static PlanMatchPattern union(PlanMatchPattern... sources)
    {
        return node(UnionNode.class, sources);
    }

    public static PlanMatchPattern assignUniqueId(String uniqueSymbolAlias, PlanMatchPattern source)
    {
        return node(AssignUniqueId.class, source)
                .withAlias(uniqueSymbolAlias, new AssignUniqueIdMatcher());
    }

    public static PlanMatchPattern intersect(PlanMatchPattern... sources)
    {
        return intersect(true, sources);
    }

    public static PlanMatchPattern intersect(boolean distinct, PlanMatchPattern... sources)
    {
        return node(IntersectNode.class, sources)
                .with(new DistinctMatcher(distinct));
    }

    public static PlanMatchPattern except(PlanMatchPattern... sources)
    {
        return except(true, sources);
    }

    public static PlanMatchPattern except(boolean distinct, PlanMatchPattern... sources)
    {
        return node(ExceptNode.class, sources)
                .with(new DistinctMatcher(distinct));
    }

    public static ExpectedValueProvider<JoinNode.EquiJoinClause> equiJoinClause(String left, String right)
    {
        return new EquiJoinClauseProvider(new SymbolAlias(left), new SymbolAlias(right));
    }

    public static SymbolAlias symbol(String alias)
    {
        return new SymbolAlias(alias);
    }

    public static PlanMatchPattern filter(@Language("SQL") String expectedPredicate, PlanMatchPattern source)
    {
        return filter(PlanBuilder.expression(expectedPredicate), source);
    }

    public static PlanMatchPattern filter(Expression expectedPredicate, PlanMatchPattern source)
    {
        return node(FilterNode.class, source).with(new FilterMatcher(expectedPredicate, Optional.empty()));
    }

    public static PlanMatchPattern filter(Expression expectedPredicate, Expression dynamicFilter, PlanMatchPattern source)
    {
        return node(FilterNode.class, source).with(new FilterMatcher(expectedPredicate, Optional.of(dynamicFilter)));
    }

    public static PlanMatchPattern apply(List<String> correlationSymbolAliases, Map<String, ExpressionMatcher> subqueryAssignments, PlanMatchPattern inputPattern, PlanMatchPattern subqueryPattern)
    {
        PlanMatchPattern result = node(ApplyNode.class, inputPattern, subqueryPattern)
                .with(new CorrelationMatcher(correlationSymbolAliases));
        subqueryAssignments.entrySet().forEach(
                assignment -> result.withAlias(assignment.getKey(), assignment.getValue()));
        return result;
    }

    public static PlanMatchPattern correlatedJoin(List<String> correlationSymbolAliases, PlanMatchPattern inputPattern, PlanMatchPattern subqueryPattern)
    {
        return node(CorrelatedJoinNode.class, inputPattern, subqueryPattern)
                .with(new CorrelationMatcher(correlationSymbolAliases));
    }

    public static PlanMatchPattern correlatedJoin(List<String> correlationSymbolAliases, @Language("SQL") String filter, PlanMatchPattern inputPattern, PlanMatchPattern subqueryPattern)
    {
        return correlatedJoin(correlationSymbolAliases, inputPattern, subqueryPattern)
                .with(new CorrelatedJoinMatcher(PlanBuilder.expression(filter)));
    }

    public static PlanMatchPattern groupId(List<List<String>> groupingSets, String groupIdSymbol, PlanMatchPattern source)
    {
        return groupId(groupingSets, ImmutableList.of(), groupIdSymbol, source);
    }

    public static PlanMatchPattern groupId(
            List<List<String>> groupingSets,
            List<String> aggregationArguments,
            String groupIdSymbol,
            PlanMatchPattern source)
    {
        return groupId(groupingSets, ImmutableMap.of(), aggregationArguments, groupIdSymbol, source);
    }

    public static PlanMatchPattern groupId(
            List<List<String>> groupingSets,
            Map<String, String> groupingColumns,
            List<String> aggregationArguments,
            String groupIdSymbol,
            PlanMatchPattern source)
    {
        return node(GroupIdNode.class, source).with(new GroupIdMatcher(
                groupingSets,
                groupingColumns,
                aggregationArguments,
                groupIdSymbol));
    }

    public static PlanMatchPattern values(
            Map<String, Integer> aliasToIndex,
            Optional<Integer> expectedOutputSymbolCount,
            Optional<List<Expression>> expectedRows)
    {
        return node(ValuesNode.class).with(new ValuesMatcher(aliasToIndex, expectedOutputSymbolCount, expectedRows));
    }

    private static PlanMatchPattern values(List<String> aliases, Optional<List<List<Expression>>> expectedRows)
    {
        return values(
                aliasToIndex(aliases),
                Optional.of(aliases.size()),
                expectedRows.map(list -> list.stream()
                        .map(Row::new)
                        .collect(toImmutableList())));
    }

    public static Map<String, Integer> aliasToIndex(List<String> aliases)
    {
        return Maps.uniqueIndex(IntStream.range(0, aliases.size()).boxed().iterator(), aliases::get);
    }

    public static PlanMatchPattern values(Map<String, Integer> aliasToIndex)
    {
        return values(aliasToIndex, Optional.empty(), Optional.empty());
    }

    public static PlanMatchPattern values(String... aliases)
    {
        return values(ImmutableList.copyOf(aliases));
    }

    public static PlanMatchPattern values(int rowCount)
    {
        return values(ImmutableList.of(), nCopies(rowCount, ImmutableList.of()));
    }

    public static PlanMatchPattern values(List<String> aliases, List<List<Expression>> expectedRows)
    {
        return values(aliases, Optional.of(expectedRows));
    }

    public static PlanMatchPattern values(List<String> aliases)
    {
        return values(aliases, Optional.empty());
    }

    public static PlanMatchPattern offset(long rowCount, PlanMatchPattern source)
    {
        return node(OffsetNode.class, source).with(new OffsetMatcher(rowCount));
    }

    public static PlanMatchPattern limit(long limit, PlanMatchPattern source)
    {
        return limit(limit, ImmutableList.of(), false, source);
    }

    public static PlanMatchPattern limit(long limit, List<Ordering> tiesResolvers, PlanMatchPattern source)
    {
        return limit(limit, tiesResolvers, false, source);
    }

    public static PlanMatchPattern limit(long limit, List<Ordering> tiesResolvers, boolean partial, PlanMatchPattern source)
    {
        return limit(limit, tiesResolvers, partial, ImmutableList.of(), source);
    }

    public static PlanMatchPattern limit(long limit, List<Ordering> tiesResolvers, boolean partial, List<String> preSortedInputs, PlanMatchPattern source)
    {
        return node(LimitNode.class, source).with(new LimitMatcher(
                limit,
                tiesResolvers,
                partial,
                preSortedInputs.stream()
                        .map(SymbolAlias::new)
                        .collect(toImmutableList())));
    }

    public static PlanMatchPattern enforceSingleRow(PlanMatchPattern source)
    {
        return node(EnforceSingleRowNode.class, source);
    }

    public static PlanMatchPattern mergeWriter(PlanMatchPattern source)
    {
        return node(MergeWriterNode.class, source);
    }

    public static PlanMatchPattern tableWriter(List<String> columns, List<String> columnNames, PlanMatchPattern source)
    {
        return node(TableWriterNode.class, source).with(new TableWriterMatcher(columns, columnNames));
    }

    public static PlanMatchPattern tableExecute(List<String> columns, List<String> columnNames, PlanMatchPattern source)
    {
        return node(TableExecuteNode.class, source).with(new TableExecuteMatcher(columns, columnNames));
    }

    public static PlanMatchPattern tableFunction(Consumer<TableFunctionMatcher.Builder> handler, PlanMatchPattern... sources)
    {
        TableFunctionMatcher.Builder builder = new TableFunctionMatcher.Builder(sources);
        handler.accept(builder);
        return builder.build();
    }

    public static PlanMatchPattern tableFunctionProcessor(Consumer<TableFunctionProcessorMatcher.Builder> handler, PlanMatchPattern source)
    {
        TableFunctionProcessorMatcher.Builder builder = new TableFunctionProcessorMatcher.Builder(source);
        handler.accept(builder);
        return builder.build();
    }

    public static PlanMatchPattern tableFunctionProcessor(Consumer<TableFunctionProcessorMatcher.Builder> handler)
    {
        TableFunctionProcessorMatcher.Builder builder = new TableFunctionProcessorMatcher.Builder();
        handler.accept(builder);
        return builder.build();
    }

    public PlanMatchPattern(List<PlanMatchPattern> sourcePatterns)
    {
        requireNonNull(sourcePatterns, "sourcePatterns are null");

        this.sourcePatterns = ImmutableList.copyOf(sourcePatterns);
    }

    List<PlanMatchingState> shapeMatches(PlanNode node)
    {
        ImmutableList.Builder<PlanMatchingState> states = ImmutableList.builder();
        if (anyTree) {
            int sourcesCount = node.getSources().size();
            if (sourcesCount > 1) {
                states.add(new PlanMatchingState(nCopies(sourcesCount, this)));
            }
            else {
                states.add(new PlanMatchingState(ImmutableList.of(this)));
            }
        }
        if (node instanceof GroupReference) {
            if (sourcePatterns.isEmpty() && shapeMatchesMatchers(node)) {
                states.add(new PlanMatchingState(ImmutableList.of()));
            }
        }
        else if (node.getSources().size() == sourcePatterns.size() && shapeMatchesMatchers(node)) {
            states.add(new PlanMatchingState(sourcePatterns));
        }
        return states.build();
    }

    private boolean shapeMatchesMatchers(PlanNode node)
    {
        return matchers.stream().allMatch(it -> it.shapeMatches(node));
    }

    MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        SymbolAliases.Builder newAliases = SymbolAliases.builder();

        for (Matcher matcher : matchers) {
            MatchResult matchResult = matcher.detailMatches(node, stats, session, metadata, symbolAliases);
            if (!matchResult.isMatch()) {
                return NO_MATCH;
            }
            newAliases.putAll(matchResult.getAliases());
        }

        return match(newAliases.build());
    }

    public <T extends PlanNode> PlanMatchPattern with(Class<T> clazz, Predicate<T> predicate)
    {
        return with(new Matcher()
        {
            @Override
            public boolean shapeMatches(PlanNode node)
            {
                return clazz.isInstance(node);
            }

            @Override
            public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
            {
                if (predicate.test(clazz.cast(node))) {
                    return match();
                }

                return NO_MATCH;
            }
        });
    }

    public PlanMatchPattern with(Matcher matcher)
    {
        matchers.add(matcher);
        return this;
    }

    public PlanMatchPattern withAlias(String alias)
    {
        return withAlias(Optional.of(alias), new AliasPresent(alias));
    }

    public PlanMatchPattern withAlias(String alias, RvalueMatcher matcher)
    {
        return withAlias(Optional.of(alias), matcher);
    }

    public PlanMatchPattern withAlias(Optional<String> alias, RvalueMatcher matcher)
    {
        matchers.add(new AliasMatcher(alias, matcher));
        return this;
    }

    public PlanMatchPattern withNumberOfOutputColumns(int numberOfSymbols)
    {
        matchers.add(new SymbolCardinalityMatcher(numberOfSymbols));
        return this;
    }

    /*
     * This is useful if you already know the bindings for the aliases you expect to find
     * in the outputs. This is the case for symbols that are produced by a direct or indirect
     * source of the node you're applying this to.
     */
    public PlanMatchPattern withExactOutputs(String... expectedAliases)
    {
        return withExactOutputs(ImmutableList.copyOf(expectedAliases));
    }

    public PlanMatchPattern withExactOutputs(List<String> expectedAliases)
    {
        matchers.add(new StrictSymbolsMatcher(actualOutputs(), expectedAliases));
        return this;
    }

    /*
     * withExactAssignments and withExactAssignedOutputs are needed for matching symbols
     * that are produced in the node that you're matching. The name of the symbol bound to
     * the alias is *not* known when the Matcher is run, and so you need to match by what
     * is being assigned to it.
     */
    public PlanMatchPattern withExactAssignedOutputs(RvalueMatcher... expectedAliases)
    {
        return withExactAssignedOutputs(ImmutableList.copyOf(expectedAliases));
    }

    public PlanMatchPattern withExactAssignedOutputs(Collection<? extends RvalueMatcher> expectedAliases)
    {
        matchers.add(new StrictAssignedSymbolsMatcher(actualOutputs(), expectedAliases));
        return this;
    }

    public PlanMatchPattern withExactAssignments(RvalueMatcher... expectedAliases)
    {
        return withExactAssignments(ImmutableList.copyOf(expectedAliases));
    }

    public PlanMatchPattern withExactAssignments(Collection<? extends RvalueMatcher> expectedAliases)
    {
        matchers.add(new StrictAssignedSymbolsMatcher(actualAssignments(), expectedAliases));
        return this;
    }

    public PlanMatchPattern withOutputRowCount(double expectedOutputRowCount)
    {
        matchers.add(new StatsOutputRowCountMatcher(expectedOutputRowCount));
        return this;
    }

    public static RvalueMatcher columnReference(String tableName, String columnName)
    {
        return new ColumnReference(tableName, columnName);
    }

    public static ExpressionMatcher expression(@Language("SQL") String expression)
    {
        return new ExpressionMatcher(expression);
    }

    public static ExpressionMatcher expression(Expression expression)
    {
        return new ExpressionMatcher(expression);
    }

    public PlanMatchPattern withOutputs(String... aliases)
    {
        return withOutputs(ImmutableList.copyOf(aliases));
    }

    public PlanMatchPattern withOutputs(List<String> aliases)
    {
        matchers.add(new OutputMatcher(aliases));
        return this;
    }

    PlanMatchPattern matchToAnyNodeTree()
    {
        anyTree = true;
        return this;
    }

    public boolean isTerminated()
    {
        return sourcePatterns.isEmpty();
    }

    public static PlanTestSymbol anySymbol()
    {
        return new AnySymbol();
    }

    public static ExpectedValueProvider<FunctionCall> functionCall(String name, List<String> args)
    {
        return new FunctionCallProvider(QualifiedName.of(name), toSymbolAliases(args));
    }

    public static ExpectedValueProvider<FunctionCall> functionCall(String name, List<String> args, List<Ordering> orderBy)
    {
        return new FunctionCallProvider(QualifiedName.of(name), toSymbolAliases(args), orderBy);
    }

    public static ExpectedValueProvider<FunctionCall> functionCall(
            String name,
            Optional<WindowFrame> frame,
            List<String> args)
    {
        return new FunctionCallProvider(QualifiedName.of(name), frame, false, toSymbolAliases(args));
    }

    public static ExpectedValueProvider<FunctionCall> functionCall(
            String name,
            boolean distinct,
            List<PlanTestSymbol> args)
    {
        return new FunctionCallProvider(QualifiedName.of(name), distinct, args);
    }

    public static ExpectedValueProvider<FunctionCall> functionCall(String name, List<String> args, String filter)
    {
        return new FunctionCallProvider(QualifiedName.of(name), toSymbolAliases(args), symbol(filter));
    }

    public static List<Expression> toSymbolReferences(List<PlanTestSymbol> aliases, SymbolAliases symbolAliases)
    {
        return aliases
                .stream()
                .map(arg -> arg.toSymbol(symbolAliases).toSymbolReference())
                .collect(toImmutableList());
    }

    private static List<PlanTestSymbol> toSymbolAliases(List<String> aliases)
    {
        return aliases
                .stream()
                .map(PlanMatchPattern::symbol)
                .collect(toImmutableList());
    }

    public static ExpectedValueProvider<DataOrganizationSpecification> specification(
            List<String> partitionBy,
            List<String> orderBy,
            Map<String, SortOrder> orderings)
    {
        return new SpecificationProvider(
                partitionBy
                        .stream()
                        .map(SymbolAlias::new)
                        .collect(toImmutableList()),
                orderBy
                        .stream()
                        .map(SymbolAlias::new)
                        .collect(toImmutableList()),
                orderings
                        .entrySet()
                        .stream()
                        .collect(toImmutableMap(entry -> new SymbolAlias(entry.getKey()), Map.Entry::getValue)));
    }

    public static Ordering sort(String field, SortItem.Ordering ordering, SortItem.NullOrdering nullOrdering)
    {
        return new Ordering(field, ordering, nullOrdering);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        toString(builder, 0);
        return builder.toString();
    }

    private void toString(StringBuilder builder, int indent)
    {
        checkState(matchers.stream().filter(PlanNodeMatcher.class::isInstance).count() <= 1);

        builder.append(indentString(indent)).append("- ");
        if (anyTree) {
            builder.append("anyTree");
        }
        else {
            builder.append("node");
        }

        Optional<PlanNodeMatcher> planNodeMatcher = matchers.stream()
                .filter(PlanNodeMatcher.class::isInstance)
                .map(PlanNodeMatcher.class::cast)
                .findFirst();

        planNodeMatcher.ifPresent(nodeMatcher -> builder.append("(").append(nodeMatcher.getNodeClass().getSimpleName()).append(")"));

        builder.append("\n");

        List<Matcher> matchersToPrint = matchers.stream()
                .filter(matcher -> !(matcher instanceof PlanNodeMatcher))
                .collect(toImmutableList());

        for (Matcher matcher : matchersToPrint) {
            builder.append(indentString(indent + 1)).append(matcher.toString()).append("\n");
        }

        for (PlanMatchPattern pattern : sourcePatterns) {
            pattern.toString(builder, indent + 1);
        }
    }

    private static String indentString(int indent)
    {
        return "    ".repeat(indent);
    }

    public static GroupingSetDescriptor globalAggregation()
    {
        return singleGroupingSet();
    }

    public static GroupingSetDescriptor singleGroupingSet(String... groupingKeys)
    {
        return singleGroupingSet(ImmutableList.copyOf(groupingKeys));
    }

    public static GroupingSetDescriptor singleGroupingSet(List<String> groupingKeys)
    {
        Set<Integer> globalGroupingSets;
        if (groupingKeys.size() == 0) {
            globalGroupingSets = ImmutableSet.of(0);
        }
        else {
            globalGroupingSets = ImmutableSet.of();
        }

        return new GroupingSetDescriptor(groupingKeys, 1, globalGroupingSets);
    }

    public static class DynamicFilterPattern
    {
        private final Expression probe;
        private final ComparisonExpression.Operator operator;
        private final SymbolAlias build;
        private final boolean nullAllowed;

        public DynamicFilterPattern(String probeExpression, ComparisonExpression.Operator operator, String buildAlias, boolean nullAllowed)
        {
            this(
                    PlanBuilder.expression(probeExpression),
                    operator,
                    buildAlias,
                    nullAllowed);
        }

        public DynamicFilterPattern(Expression probe, ComparisonExpression.Operator operator, String buildAlias, boolean nullAllowed)
        {
            this.probe = requireNonNull(probe, "probe is null");
            this.operator = requireNonNull(operator, "operator is null");
            this.build = new SymbolAlias(requireNonNull(buildAlias, "buildAlias is null"));
            this.nullAllowed = nullAllowed;
        }

        public DynamicFilterPattern(String probeAlias, ComparisonExpression.Operator operator, String buildAlias)
        {
            this(probeAlias, operator, buildAlias, false);
        }

        Expression getExpression(SymbolAliases aliases)
        {
            Expression probeMapped = symbolMapper(aliases).map(probe);
            if (nullAllowed) {
                return new NotExpression(
                        new ComparisonExpression(
                                IS_DISTINCT_FROM,
                                probeMapped,
                                build.toSymbol(aliases).toSymbolReference()));
            }
            return new ComparisonExpression(
                    operator,
                    probeMapped,
                    build.toSymbol(aliases).toSymbolReference());
        }

        private static SymbolMapper symbolMapper(SymbolAliases symbolAliases)
        {
            return new SymbolMapper(symbol -> Symbol.from(symbolAliases.get(symbol.getName())));
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("probe", probe)
                    .add("operator", operator)
                    .add("build", build)
                    .toString();
        }
    }

    public static class GroupingSetDescriptor
    {
        private final List<String> groupingKeys;
        private final int groupingSetCount;
        private final Set<Integer> globalGroupingSets;

        private GroupingSetDescriptor(List<String> groupingKeys, int groupingSetCount, Set<Integer> globalGroupingSets)
        {
            this.groupingKeys = groupingKeys;
            this.groupingSetCount = groupingSetCount;
            this.globalGroupingSets = globalGroupingSets;
        }

        public List<String> getGroupingKeys()
        {
            return groupingKeys;
        }

        public int getGroupingSetCount()
        {
            return groupingSetCount;
        }

        public Set<Integer> getGlobalGroupingSets()
        {
            return globalGroupingSets;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("keys", groupingKeys)
                    .add("count", groupingSetCount)
                    .add("globalSets", globalGroupingSets)
                    .toString();
        }
    }

    public static class UnnestMapping
    {
        private final String input;
        private final List<String> outputs;

        private UnnestMapping(String input, List<String> outputs)
        {
            this.input = requireNonNull(input, "input is null");
            this.outputs = requireNonNull(outputs, "outputs is null");
        }

        public static UnnestMapping unnestMapping(String input, List<String> outputs)
        {
            return new UnnestMapping(input, outputs);
        }

        public String getInput()
        {
            return input;
        }

        public List<String> getOutputs()
        {
            return outputs;
        }
    }

    public static class Ordering
    {
        private final String field;
        private final SortItem.Ordering ordering;
        private final SortItem.NullOrdering nullOrdering;

        private Ordering(String field, SortItem.Ordering ordering, SortItem.NullOrdering nullOrdering)
        {
            this.field = field;
            this.ordering = ordering;
            this.nullOrdering = nullOrdering;
        }

        public String getField()
        {
            return field;
        }

        public SortItem.Ordering getOrdering()
        {
            return ordering;
        }

        public SortItem.NullOrdering getNullOrdering()
        {
            return nullOrdering;
        }

        public SortOrder getSortOrder()
        {
            checkState(nullOrdering != UNDEFINED, "nullOrdering is undefined");
            if (ordering == ASCENDING) {
                if (nullOrdering == FIRST) {
                    return ASC_NULLS_FIRST;
                }
                return ASC_NULLS_LAST;
            }
            checkState(ordering == DESCENDING);
            if (nullOrdering == FIRST) {
                return DESC_NULLS_FIRST;
            }
            return DESC_NULLS_LAST;
        }

        @Override
        public String toString()
        {
            String result = field + " " + ordering;
            if (nullOrdering != UNDEFINED) {
                result += " NULLS " + nullOrdering;
            }

            return result;
        }
    }
}
