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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.type.Type;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableFunctionNode;
import io.trino.sql.planner.plan.TableFunctionNode.PassThroughSpecification;
import io.trino.sql.planner.plan.TableFunctionNode.TableArgumentProperties;
import io.trino.sql.planner.plan.TableFunctionProcessorNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.planner.plan.WindowNode.Frame;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.IfExpression;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.QualifiedName;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.sql.planner.plan.JoinNode.Type.FULL;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.sql.planner.plan.JoinNode.Type.LEFT;
import static io.trino.sql.planner.plan.JoinNode.Type.RIGHT;
import static io.trino.sql.planner.plan.Patterns.tableFunction;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.IS_DISTINCT_FROM;
import static io.trino.sql.tree.FrameBound.Type.UNBOUNDED_FOLLOWING;
import static io.trino.sql.tree.FrameBound.Type.UNBOUNDED_PRECEDING;
import static io.trino.sql.tree.LogicalExpression.Operator.AND;
import static io.trino.sql.tree.LogicalExpression.Operator.OR;
import static io.trino.sql.tree.WindowFrame.Type.ROWS;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

/**
 * This rule prepares cartesian product of partitions
 * from all inputs of table function.
 * <p>
 * It rewrites TableFunctionNode with potentially many sources
 * into a TableFunctionProcessorNode. The new node has one
 * source being a combination of the original sources.
 * <p>
 * The original sources are combined with joins. The join
 * conditions depend on the prune when empty property, and on
 * the co-partitioning of sources.
 * <p>
 * The resulting source should be partitioned and ordered
 * according to combined schemas from the component sources.
 * <p>
 * Example transformation for two sources, both with set semantics
 * and KEEP WHEN EMPTY property:
 * <pre>
 * - TableFunction foo
 *      - source T1(a1, b1) PARTITION BY a1 ORDER BY b1
 *      - source T2(a2, b2) PARTITION BY a2
 * </pre>
 * Is transformed into:
 * <pre>
 * - TableFunctionProcessor foo
 *      PARTITION BY (a1, a2), ORDER BY combined_row_number
 *      - Project
 *          marker_1 <= IF(table1_row_number = combined_row_number, table1_row_number, CAST(null AS bigint))
 *          marker_2 <= IF(table2_row_number = combined_row_number, table2_row_number, CAST(null AS bigint))
 *          - Project
 *              combined_row_number <= IF(COALESCE(table1_row_number, BIGINT '-1') > COALESCE(table2_row_number, BIGINT '-1'), table1_row_number, table2_row_number)
 *              combined_partition_size <= IF(COALESCE(table1_partition_size, BIGINT '-1') > COALESCE(table2_partition_size, BIGINT '-1'), table1_partition_size, table2_partition_size)
 *              - FULL Join
 *                  [table1_row_number = table2_row_number OR
 *                   table1_row_number > table2_partition_size AND table2_row_number = BIGINT '1' OR
 *                   table2_row_number > table1_partition_size AND table1_row_number = BIGINT '1']
 *                  - Window [PARTITION BY a1 ORDER BY b1]
 *                      table1_row_number <= row_number()
 *                      table1_partition_size <= count()
 *                          - source T1(a1, b1)
 *                  - Window [PARTITION BY a2]
 *                      table2_row_number <= row_number()
 *                      table2_partition_size <= count()
 *                          - source T2(a2, b2)
 * </pre>
 */
public class ImplementTableFunctionSource
        implements Rule<TableFunctionNode>
{
    private static final Pattern<TableFunctionNode> PATTERN = tableFunction();
    private static final Frame FULL_FRAME = new Frame(
            ROWS,
            UNBOUNDED_PRECEDING,
            Optional.empty(),
            Optional.empty(),
            UNBOUNDED_FOLLOWING,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());
    private static final DataOrganizationSpecification UNORDERED_SINGLE_PARTITION = new DataOrganizationSpecification(ImmutableList.of(), Optional.empty());

    private final Metadata metadata;

    public ImplementTableFunctionSource(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<TableFunctionNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(TableFunctionNode node, Captures captures, Context context)
    {
        if (node.getSources().isEmpty()) {
            return Result.ofPlanNode(new TableFunctionProcessorNode(
                    node.getId(),
                    node.getName(),
                    node.getProperOutputs(),
                    Optional.empty(),
                    false,
                    ImmutableList.of(),
                    ImmutableList.of(),
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableSet.of(),
                    0,
                    Optional.empty(),
                    node.getHandle()));
        }

        if (node.getSources().size() == 1) {
            // Single source does not require pre-processing.
            // If the source has row semantics, its specification is empty.
            // If the source has set semantics, its specification is present, even if there is no partitioning or ordering specified.
            // This property can be used later to choose optimal distribution.
            TableArgumentProperties sourceProperties = getOnlyElement(node.getTableArgumentProperties());
            return Result.ofPlanNode(new TableFunctionProcessorNode(
                    node.getId(),
                    node.getName(),
                    node.getProperOutputs(),
                    Optional.of(getOnlyElement(node.getSources())),
                    sourceProperties.isPruneWhenEmpty(),
                    ImmutableList.of(sourceProperties.getPassThroughSpecification()),
                    ImmutableList.of(sourceProperties.getRequiredColumns()),
                    Optional.empty(),
                    sourceProperties.getSpecification(),
                    ImmutableSet.of(),
                    0,
                    Optional.empty(),
                    node.getHandle()));
        }
        Map<String, SourceWithProperties> sources = mapSourcesByName(node.getSources(), node.getTableArgumentProperties());
        ImmutableList.Builder<NodeWithSymbols> intermediateResultsBuilder = ImmutableList.builder();
        ResolvedFunction rowNumberFunction = metadata.resolveFunction(context.getSession(), QualifiedName.of("row_number"), ImmutableList.of());
        ResolvedFunction countFunction = metadata.resolveFunction(context.getSession(), QualifiedName.of("count"), ImmutableList.of());

        // handle co-partitioned sources
        for (List<String> copartitioningList : node.getCopartitioningLists()) {
            List<SourceWithProperties> sourceList = copartitioningList.stream()
                    .map(sources::get)
                    .collect(toImmutableList());
            intermediateResultsBuilder.add(copartition(sourceList, rowNumberFunction, countFunction, context));
        }

        // prepare non-co-partitioned sources
        Set<String> copartitionedSources = node.getCopartitioningLists().stream()
                .flatMap(Collection::stream)
                .collect(toImmutableSet());
        sources.entrySet().stream()
                .filter(entry -> !copartitionedSources.contains(entry.getKey()))
                .map(entry -> planWindowFunctionsForSource(entry.getValue().source(), entry.getValue().properties(), rowNumberFunction, countFunction, context))
                .forEach(intermediateResultsBuilder::add);

        NodeWithSymbols finalResultSource;

        List<NodeWithSymbols> intermediateResultSources = intermediateResultsBuilder.build();
        if (intermediateResultSources.size() == 1) {
            finalResultSource = getOnlyElement(intermediateResultSources);
        }
        else {
            NodeWithSymbols first = intermediateResultSources.get(0);
            NodeWithSymbols second = intermediateResultSources.get(1);
            JoinedNodes joined = join(first, second, context);

            for (int i = 2; i < intermediateResultSources.size(); i++) {
                NodeWithSymbols joinedWithSymbols = appendHelperSymbolsForJoinedNodes(joined, context);
                joined = join(joinedWithSymbols, intermediateResultSources.get(i), context);
            }

            finalResultSource = appendHelperSymbolsForJoinedNodes(joined, context);
        }

        // For each source, all source's output symbols are mapped to the source's row number symbol.
        // The row number symbol will be later converted to a marker of "real" input rows vs "filler" input rows of the source.
        // The "filler" input rows are the rows appended while joining partitions of different lengths,
        // to fill the smaller partition up to the bigger partition's size. They are a side effect of the algorithm,
        // and should not be processed by the table function.
        Map<Symbol, Symbol> rowNumberSymbols = finalResultSource.rowNumberSymbolsMapping();

        // The max row number symbol from all joined partitions.
        Symbol finalRowNumberSymbol = finalResultSource.rowNumber();
        // Combined partitioning lists from all sources.
        List<Symbol> finalPartitionBy = finalResultSource.partitionBy();

        NodeWithMarkers marked = appendMarkerSymbols(finalResultSource.node(), ImmutableSet.copyOf(rowNumberSymbols.values()), finalRowNumberSymbol, context);

        // Remap the symbol mapping: replace the row number symbol with the corresponding marker symbol.
        // In the new map, every source symbol is associated with the corresponding marker symbol.
        // Null value of the marker indicates that the source value should be ignored by the table function.
        ImmutableMap<Symbol, Symbol> markerSymbols = rowNumberSymbols.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> marked.symbolToMarker().get(entry.getValue())));

        // Use the final row number symbol for ordering the combined sources.
        // It runs along each partition in the cartesian product, numbering the partition's rows according to the expected ordering / orderings.
        // note: ordering is necessary even if all the source tables are not ordered. Thanks to the ordering, the original rows
        // of each input table come before the "filler" rows.
        Optional<OrderingScheme> finalOrderBy = Optional.of(new OrderingScheme(ImmutableList.of(finalRowNumberSymbol), ImmutableMap.of(finalRowNumberSymbol, ASC_NULLS_LAST)));

        // derive the prune when empty property
        boolean pruneWhenEmpty = node.getTableArgumentProperties().stream().anyMatch(TableArgumentProperties::isPruneWhenEmpty);

        // Combine the pass through specifications from all sources
        List<PassThroughSpecification> passThroughSpecifications = node.getTableArgumentProperties().stream()
                .map(TableArgumentProperties::getPassThroughSpecification)
                .collect(toImmutableList());

        // Combine the required symbols from all sources
        List<List<Symbol>> requiredSymbols = node.getTableArgumentProperties().stream()
                .map(TableArgumentProperties::getRequiredColumns)
                .collect(toImmutableList());

        return Result.ofPlanNode(new TableFunctionProcessorNode(
                node.getId(),
                node.getName(),
                node.getProperOutputs(),
                Optional.of(marked.node()),
                pruneWhenEmpty,
                passThroughSpecifications,
                requiredSymbols,
                Optional.of(markerSymbols),
                Optional.of(new DataOrganizationSpecification(finalPartitionBy, finalOrderBy)),
                ImmutableSet.of(),
                0,
                Optional.empty(),
                node.getHandle()));
    }

    private static Map<String, SourceWithProperties> mapSourcesByName(List<PlanNode> sources, List<TableArgumentProperties> properties)
    {
        return Streams.zip(sources.stream(), properties.stream(), SourceWithProperties::new)
                .collect(toImmutableMap(entry -> entry.properties().getArgumentName(), identity()));
    }

    private static NodeWithSymbols planWindowFunctionsForSource(
            PlanNode source,
            TableArgumentProperties argumentProperties,
            ResolvedFunction rowNumberFunction,
            ResolvedFunction countFunction,
            Context context)
    {
        String argumentName = argumentProperties.getArgumentName();

        Symbol rowNumber = context.getSymbolAllocator().newSymbol(argumentName + "_row_number", BIGINT);
        Map<Symbol, Symbol> rowNumberSymbolMapping = source.getOutputSymbols().stream()
                .collect(toImmutableMap(identity(), symbol -> rowNumber));

        Symbol partitionSize = context.getSymbolAllocator().newSymbol(argumentName + "_partition_size", BIGINT);

        // If the source has set semantics, its specification is present, even if there is no partitioning or ordering specified.
        // If the source has row semantics, its specification is empty. Currently, such source is processed
        // as if it was a single partition. Alternatively, it could be split into smaller partitions of arbitrary size.
        DataOrganizationSpecification specification = argumentProperties.getSpecification().orElse(UNORDERED_SINGLE_PARTITION);

        PlanNode window = new WindowNode(
                context.getIdAllocator().getNextId(),
                source,
                specification,
                ImmutableMap.of(
                        rowNumber, new WindowNode.Function(rowNumberFunction, ImmutableList.of(), FULL_FRAME, false),
                        partitionSize, new WindowNode.Function(countFunction, ImmutableList.of(), FULL_FRAME, false)),
                Optional.empty(),
                ImmutableSet.of(),
                0);

        return new NodeWithSymbols(window, rowNumber, partitionSize, specification.getPartitionBy(), argumentProperties.isPruneWhenEmpty(), rowNumberSymbolMapping);
    }

    private static NodeWithSymbols copartition(
            List<SourceWithProperties> sourceList,
            ResolvedFunction rowNumberFunction,
            ResolvedFunction countFunction,
            Context context)
    {
        checkArgument(sourceList.size() >= 2, "co-partitioning list should contain at least two tables");

        // Reorder the co-partitioned sources to process the sources with prune when empty property first.
        // It allows to use inner or side joins instead of outer joins.
        sourceList = sourceList.stream()
                .sorted(Comparator.comparingInt(source -> source.properties().isPruneWhenEmpty() ? -1 : 1))
                .collect(toImmutableList());

        NodeWithSymbols first = planWindowFunctionsForSource(sourceList.get(0).source(), sourceList.get(0).properties(), rowNumberFunction, countFunction, context);
        NodeWithSymbols second = planWindowFunctionsForSource(sourceList.get(1).source(), sourceList.get(1).properties(), rowNumberFunction, countFunction, context);
        JoinedNodes copartitioned = copartition(first, second, context);

        for (int i = 2; i < sourceList.size(); i++) {
            NodeWithSymbols copartitionedWithSymbols = appendHelperSymbolsForCopartitionedNodes(copartitioned, context);
            NodeWithSymbols next = planWindowFunctionsForSource(sourceList.get(i).source(), sourceList.get(i).properties(), rowNumberFunction, countFunction, context);
            copartitioned = copartition(copartitionedWithSymbols, next, context);
        }

        return appendHelperSymbolsForCopartitionedNodes(copartitioned, context);
    }

    private static JoinedNodes copartition(NodeWithSymbols left, NodeWithSymbols right, Context context)
    {
        checkArgument(left.partitionBy().size() == right.partitionBy().size(), "co-partitioning lists do not match");

        // In StatementAnalyzer we require that co-partitioned tables have non-empty partitioning column lists.
        // Co-partitioning tables with empty partition by would be ineffective.
        checkState(!left.partitionBy().isEmpty(), "co-partitioned tables must have partitioning columns");

        Expression leftRowNumber = left.rowNumber().toSymbolReference();
        Expression leftPartitionSize = left.partitionSize().toSymbolReference();
        List<Expression> leftPartitionBy = left.partitionBy().stream()
                .map(Symbol::toSymbolReference)
                .collect(toImmutableList());
        Expression rightRowNumber = right.rowNumber().toSymbolReference();
        Expression rightPartitionSize = right.partitionSize().toSymbolReference();
        List<Expression> rightPartitionBy = right.partitionBy().stream()
                .map(Symbol::toSymbolReference)
                .collect(toImmutableList());

        List<Expression> copartitionConjuncts = Streams.zip(
                        leftPartitionBy.stream(),
                        rightPartitionBy.stream(),
                        (leftColumn, rightColumn) -> new NotExpression(new ComparisonExpression(IS_DISTINCT_FROM, leftColumn, rightColumn)))
                .collect(toImmutableList());

        // Align matching partitions (co-partitions) from left and right source, according to row number.
        // Matching partitions are identified by their corresponding partitioning columns being NOT DISTINCT from each other.
        // If one or both sources are ordered, the row number reflects the ordering.
        // The second and third disjunct in the join condition account for the situation when partitions have different sizes.
        // It preserves the outstanding rows from the bigger partition, matching them to the first row from the smaller partition.
        //
        // (P1_1 IS NOT DISTINCT FROM P2_1) AND (P1_2 IS NOT DISTINCT FROM P2_2) AND ...
        // AND (
        //      R1 = R2
        //      OR
        //      (R1 > S2 AND R2 = 1)
        //      OR
        //      (R2 > S1 AND R1 = 1))
        Expression joinCondition = new LogicalExpression(
                AND,
                ImmutableList.<Expression>builder()
                        .addAll(copartitionConjuncts)
                        .add(new LogicalExpression(OR, ImmutableList.of(
                                new ComparisonExpression(EQUAL, leftRowNumber, rightRowNumber),
                                new LogicalExpression(AND, ImmutableList.of(
                                        new ComparisonExpression(GREATER_THAN, leftRowNumber, rightPartitionSize),
                                        new ComparisonExpression(EQUAL, rightRowNumber, new GenericLiteral("BIGINT", "1")))),
                                new LogicalExpression(AND, ImmutableList.of(
                                        new ComparisonExpression(GREATER_THAN, rightRowNumber, leftPartitionSize),
                                        new ComparisonExpression(EQUAL, leftRowNumber, new GenericLiteral("BIGINT", "1")))))))
                        .build());

        // The join type depends on the prune when empty property of the sources.
        // If a source is prune when empty, we should not process any co-partition which is not present in this source,
        // so effectively the other source becomes inner side of the join.
        //
        // example:
        // table T1 partition by P1              table T2 partition by P2
        //   P1     C1                             P2     C2
        //  ----------                            ----------
        //   1     'a'                             2     'c'
        //   2     'b'                             3     'd'
        //
        // co-partitioning results:
        // 1) T1 is prune when empty: do LEFT JOIN to drop co-partition '3'
        //   P1     C1     P2     C2
        //  ------------------------
        //   1      'a'    null   null
        //   2      'b'    2      'c'
        //
        // 2) T2 is prune when empty: do RIGHT JOIN to drop co-partition '1'
        //   P1     C1     P2     C2
        //  ------------------------
        //   2      'b'     2     'c'
        //   null   null    3     'd'
        //
        // 3) T1 and T2 are both prune when empty: do INNER JOIN to drop co-partitions '1' and '3'
        //   P1     C1     P2     C2
        //  ------------------------
        //   2      'b'    2      'c'
        //
        // 4) neither table is prune when empty: do FULL JOIN to preserve all co-partitions
        //   P1     C1     P2     C2
        //  ------------------------
        //   1      'a'    null   null
        //   2      'b'    2      'c'
        //   null   null   3      'd'
        JoinNode.Type joinType;
        if (left.pruneWhenEmpty() && right.pruneWhenEmpty()) {
            joinType = INNER;
        }
        else if (left.pruneWhenEmpty()) {
            joinType = LEFT;
        }
        else if (right.pruneWhenEmpty()) {
            joinType = RIGHT;
        }
        else {
            joinType = FULL;
        }

        return new JoinedNodes(
                new JoinNode(
                        context.getIdAllocator().getNextId(),
                        joinType,
                        left.node(),
                        right.node(),
                        ImmutableList.of(),
                        left.node().getOutputSymbols(),
                        right.node().getOutputSymbols(),
                        false,
                        Optional.of(joinCondition),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of(),
                        Optional.empty()),
                left.rowNumber(),
                left.partitionSize(),
                left.partitionBy(),
                left.pruneWhenEmpty(),
                left.rowNumberSymbolsMapping(),
                right.rowNumber(),
                right.partitionSize(),
                right.partitionBy(),
                right.pruneWhenEmpty(),
                right.rowNumberSymbolsMapping());
    }

    private static NodeWithSymbols appendHelperSymbolsForCopartitionedNodes(
            JoinedNodes copartitionedNodes,
            Context context)
    {
        checkArgument(copartitionedNodes.leftPartitionBy().size() == copartitionedNodes.rightPartitionBy().size(), "co-partitioning lists do not match");

        Expression leftRowNumber = copartitionedNodes.leftRowNumber().toSymbolReference();
        Expression leftPartitionSize = copartitionedNodes.leftPartitionSize().toSymbolReference();
        Expression rightRowNumber = copartitionedNodes.rightRowNumber().toSymbolReference();
        Expression rightPartitionSize = copartitionedNodes.rightPartitionSize().toSymbolReference();

        // Derive row number for joined partitions: this is the bigger partition's row number. One of the combined values might be null as a result of outer join.
        Symbol joinedRowNumber = context.getSymbolAllocator().newSymbol("combined_row_number", BIGINT);
        Expression rowNumberExpression = new IfExpression(
                new ComparisonExpression(
                        GREATER_THAN,
                        new CoalesceExpression(leftRowNumber, new GenericLiteral("BIGINT", "-1")),
                        new CoalesceExpression(rightRowNumber, new GenericLiteral("BIGINT", "-1"))),
                leftRowNumber,
                rightRowNumber);

        // Derive partition size for joined partitions: this is the bigger partition's size. One of the combined values might be null as a result of outer join.
        Symbol joinedPartitionSize = context.getSymbolAllocator().newSymbol("combined_partition_size", BIGINT);
        Expression partitionSizeExpression = new IfExpression(
                new ComparisonExpression(
                        GREATER_THAN,
                        new CoalesceExpression(leftPartitionSize, new GenericLiteral("BIGINT", "-1")),
                        new CoalesceExpression(rightPartitionSize, new GenericLiteral("BIGINT", "-1"))),
                leftPartitionSize,
                rightPartitionSize);

        // Derive partitioning columns for joined partitions.
        // Either the combined partitioning columns are pairwise NOT DISTINCT (this is the co-partitioning rule),
        // or one of them is null as a result of outer join.
        ImmutableList.Builder<Symbol> joinedPartitionBy = ImmutableList.builder();
        Assignments.Builder joinedPartitionByAssignments = Assignments.builder();
        for (int i = 0; i < copartitionedNodes.leftPartitionBy().size(); i++) {
            Symbol leftColumn = copartitionedNodes.leftPartitionBy().get(i);
            Symbol rightColumn = copartitionedNodes.rightPartitionBy().get(i);
            Type type = context.getSymbolAllocator().getTypes().get(leftColumn);

            Symbol joinedColumn = context.getSymbolAllocator().newSymbol("combined_partition_column", type);
            joinedPartitionByAssignments.put(joinedColumn, new CoalesceExpression(leftColumn.toSymbolReference(), rightColumn.toSymbolReference()));
            joinedPartitionBy.add(joinedColumn);
        }

        PlanNode project = new ProjectNode(
                context.getIdAllocator().getNextId(),
                copartitionedNodes.joinedNode(),
                Assignments.builder()
                        .putIdentities(copartitionedNodes.joinedNode().getOutputSymbols())
                        .put(joinedRowNumber, rowNumberExpression)
                        .put(joinedPartitionSize, partitionSizeExpression)
                        .putAll(joinedPartitionByAssignments.build())
                        .build());

        boolean joinedPruneWhenEmpty = copartitionedNodes.leftPruneWhenEmpty() || copartitionedNodes.rightPruneWhenEmpty();

        Map<Symbol, Symbol> joinedRowNumberSymbolsMapping = ImmutableMap.<Symbol, Symbol>builder()
                .putAll(copartitionedNodes.leftRowNumberSymbolsMapping())
                .putAll(copartitionedNodes.rightRowNumberSymbolsMapping())
                .buildOrThrow();

        return new NodeWithSymbols(project, joinedRowNumber, joinedPartitionSize, joinedPartitionBy.build(), joinedPruneWhenEmpty, joinedRowNumberSymbolsMapping);
    }

    private static JoinedNodes join(NodeWithSymbols left, NodeWithSymbols right, Context context)
    {
        Expression leftRowNumber = left.rowNumber().toSymbolReference();
        Expression leftPartitionSize = left.partitionSize().toSymbolReference();
        Expression rightRowNumber = right.rowNumber().toSymbolReference();
        Expression rightPartitionSize = right.partitionSize().toSymbolReference();

        // Align rows from left and right source according to row number. Because every partition is row-numbered, this produces cartesian product of partitions.
        // If one or both sources are ordered, the row number reflects the ordering.
        // The second and third disjunct in the join condition account for the situation when partitions have different sizes. It preserves the outstanding rows
        // from the bigger partition, matching them to the first row from the smaller partition.
        //
        // R1 = R2
        // OR
        // (R1 > S2 AND R2 = 1)
        // OR
        // (R2 > S1 AND R1 = 1)
        Expression joinCondition = new LogicalExpression(OR, ImmutableList.of(
                new ComparisonExpression(EQUAL, leftRowNumber, rightRowNumber),
                new LogicalExpression(AND, ImmutableList.of(
                        new ComparisonExpression(GREATER_THAN, leftRowNumber, rightPartitionSize),
                        new ComparisonExpression(EQUAL, rightRowNumber, new GenericLiteral("BIGINT", "1")))),
                new LogicalExpression(AND, ImmutableList.of(
                        new ComparisonExpression(GREATER_THAN, rightRowNumber, leftPartitionSize),
                        new ComparisonExpression(EQUAL, leftRowNumber, new GenericLiteral("BIGINT", "1"))))));

        JoinNode.Type joinType;
        if (left.pruneWhenEmpty() && right.pruneWhenEmpty()) {
            joinType = INNER;
        }
        else if (left.pruneWhenEmpty()) {
            joinType = LEFT;
        }
        else if (right.pruneWhenEmpty()) {
            joinType = RIGHT;
        }
        else {
            joinType = FULL;
        }

        return new JoinedNodes(
                new JoinNode(
                        context.getIdAllocator().getNextId(),
                        joinType,
                        left.node(),
                        right.node(),
                        ImmutableList.of(),
                        left.node().getOutputSymbols(),
                        right.node().getOutputSymbols(),
                        false,
                        Optional.of(joinCondition),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of(),
                        Optional.empty()),
                left.rowNumber(),
                left.partitionSize(),
                left.partitionBy(),
                left.pruneWhenEmpty(),
                left.rowNumberSymbolsMapping(),
                right.rowNumber(),
                right.partitionSize(),
                right.partitionBy(),
                right.pruneWhenEmpty(),
                right.rowNumberSymbolsMapping());
    }

    private static NodeWithSymbols appendHelperSymbolsForJoinedNodes(JoinedNodes joinedNodes, Context context)
    {
        Expression leftRowNumber = joinedNodes.leftRowNumber().toSymbolReference();
        Expression leftPartitionSize = joinedNodes.leftPartitionSize().toSymbolReference();
        Expression rightRowNumber = joinedNodes.rightRowNumber().toSymbolReference();
        Expression rightPartitionSize = joinedNodes.rightPartitionSize().toSymbolReference();

        // Derive row number for joined partitions: this is the bigger partition's row number. One of the combined values might be null as a result of outer join.
        Symbol joinedRowNumber = context.getSymbolAllocator().newSymbol("combined_row_number", BIGINT);
        Expression rowNumberExpression = new IfExpression(
                new ComparisonExpression(
                        GREATER_THAN,
                        new CoalesceExpression(leftRowNumber, new GenericLiteral("BIGINT", "-1")),
                        new CoalesceExpression(rightRowNumber, new GenericLiteral("BIGINT", "-1"))),
                leftRowNumber,
                rightRowNumber);

        // Derive partition size for joined partitions: this is the bigger partition's size. One of the combined values might be null as a result of outer join.
        Symbol joinedPartitionSize = context.getSymbolAllocator().newSymbol("combined_partition_size", BIGINT);
        Expression partitionSizeExpression = new IfExpression(
                new ComparisonExpression(
                        GREATER_THAN,
                        new CoalesceExpression(leftPartitionSize, new GenericLiteral("BIGINT", "-1")),
                        new CoalesceExpression(rightPartitionSize, new GenericLiteral("BIGINT", "-1"))),
                leftPartitionSize,
                rightPartitionSize);

        PlanNode project = new ProjectNode(
                context.getIdAllocator().getNextId(),
                joinedNodes.joinedNode(),
                Assignments.builder()
                        .putIdentities(joinedNodes.joinedNode().getOutputSymbols())
                        .put(joinedRowNumber, rowNumberExpression)
                        .put(joinedPartitionSize, partitionSizeExpression)
                        .build());

        List<Symbol> joinedPartitionBy = ImmutableList.<Symbol>builder()
                .addAll(joinedNodes.leftPartitionBy())
                .addAll(joinedNodes.rightPartitionBy())
                .build();

        boolean joinedPruneWhenEmpty = joinedNodes.leftPruneWhenEmpty() || joinedNodes.rightPruneWhenEmpty();

        Map<Symbol, Symbol> joinedRowNumberSymbolsMapping = ImmutableMap.<Symbol, Symbol>builder()
                .putAll(joinedNodes.leftRowNumberSymbolsMapping())
                .putAll(joinedNodes.rightRowNumberSymbolsMapping())
                .buildOrThrow();

        return new NodeWithSymbols(project, joinedRowNumber, joinedPartitionSize, joinedPartitionBy, joinedPruneWhenEmpty, joinedRowNumberSymbolsMapping);
    }

    private static NodeWithMarkers appendMarkerSymbols(PlanNode node, Set<Symbol> symbols, Symbol referenceSymbol, Context context)
    {
        Assignments.Builder assignments = Assignments.builder();
        assignments.putIdentities(node.getOutputSymbols());

        ImmutableMap.Builder<Symbol, Symbol> symbolsToMarkers = ImmutableMap.builder();

        for (Symbol symbol : symbols) {
            Symbol marker = context.getSymbolAllocator().newSymbol("marker", BIGINT);
            symbolsToMarkers.put(symbol, marker);
            Expression actual = symbol.toSymbolReference();
            Expression reference = referenceSymbol.toSymbolReference();
            assignments.put(marker, new IfExpression(new ComparisonExpression(EQUAL, actual, reference), actual, new Cast(new NullLiteral(), toSqlType(BIGINT))));
        }

        PlanNode project = new ProjectNode(
                context.getIdAllocator().getNextId(),
                node,
                assignments.build());

        return new NodeWithMarkers(project, symbolsToMarkers.buildOrThrow());
    }

    private record SourceWithProperties(PlanNode source, TableArgumentProperties properties)
    {
        SourceWithProperties
        {
            requireNonNull(source, "source is null");
            requireNonNull(properties, "properties is null");
        }
    }

    private record NodeWithSymbols(PlanNode node, Symbol rowNumber, Symbol partitionSize, List<Symbol> partitionBy, boolean pruneWhenEmpty, Map<Symbol, Symbol> rowNumberSymbolsMapping)
    {
        NodeWithSymbols
        {
            requireNonNull(node, "node is null");
            requireNonNull(rowNumber, "rowNumber is null");
            requireNonNull(partitionSize, "partitionSize is null");
            partitionBy = ImmutableList.copyOf(partitionBy);
            rowNumberSymbolsMapping = ImmutableMap.copyOf(rowNumberSymbolsMapping);
        }
    }

    private record JoinedNodes(
            PlanNode joinedNode,
            Symbol leftRowNumber,
            Symbol leftPartitionSize,
            List<Symbol> leftPartitionBy,
            boolean leftPruneWhenEmpty,
            Map<Symbol, Symbol> leftRowNumberSymbolsMapping,
            Symbol rightRowNumber,
            Symbol rightPartitionSize,
            List<Symbol> rightPartitionBy,
            boolean rightPruneWhenEmpty,
            Map<Symbol, Symbol> rightRowNumberSymbolsMapping)
    {
        JoinedNodes
        {
            requireNonNull(joinedNode, "joinedNode is null");
            requireNonNull(leftRowNumber, "leftRowNumber is null");
            requireNonNull(leftPartitionSize, "leftPartitionSize is null");
            leftPartitionBy = ImmutableList.copyOf(leftPartitionBy);
            leftRowNumberSymbolsMapping = ImmutableMap.copyOf(leftRowNumberSymbolsMapping);
            requireNonNull(rightRowNumber, "rightRowNumber is null");
            requireNonNull(rightPartitionSize, "rightPartitionSize is null");
            rightPartitionBy = ImmutableList.copyOf(rightPartitionBy);
            rightRowNumberSymbolsMapping = ImmutableMap.copyOf(rightRowNumberSymbolsMapping);
        }
    }

    private record NodeWithMarkers(PlanNode node, Map<Symbol, Symbol> symbolToMarker)
    {
        NodeWithMarkers
        {
            requireNonNull(node, "node is null");
            symbolToMarker = ImmutableMap.copyOf(symbolToMarker);
        }
    }
}
