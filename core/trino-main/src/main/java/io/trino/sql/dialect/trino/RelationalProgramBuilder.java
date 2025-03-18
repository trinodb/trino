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
package io.trino.sql.dialect.trino;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.spi.TrinoException;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.type.MultisetType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.Attributes.AggregationStep;
import io.trino.sql.dialect.trino.Attributes.DistributionType;
import io.trino.sql.dialect.trino.Attributes.ExchangeScope;
import io.trino.sql.dialect.trino.Attributes.ExchangeType;
import io.trino.sql.dialect.trino.Attributes.JoinType;
import io.trino.sql.dialect.trino.Attributes.NullableValues;
import io.trino.sql.dialect.trino.Attributes.SortOrderList;
import io.trino.sql.dialect.trino.Attributes.Statistics;
import io.trino.sql.dialect.trino.Attributes.TopNStep;
import io.trino.sql.dialect.trino.operation.AggregateCall;
import io.trino.sql.dialect.trino.operation.Aggregation;
import io.trino.sql.dialect.trino.operation.Constant;
import io.trino.sql.dialect.trino.operation.CorrelatedJoin;
import io.trino.sql.dialect.trino.operation.Exchange;
import io.trino.sql.dialect.trino.operation.FieldReference;
import io.trino.sql.dialect.trino.operation.Filter;
import io.trino.sql.dialect.trino.operation.Join;
import io.trino.sql.dialect.trino.operation.Limit;
import io.trino.sql.dialect.trino.operation.Output;
import io.trino.sql.dialect.trino.operation.Project;
import io.trino.sql.dialect.trino.operation.Return;
import io.trino.sql.dialect.trino.operation.Row;
import io.trino.sql.dialect.trino.operation.TableScan;
import io.trino.sql.dialect.trino.operation.TopN;
import io.trino.sql.dialect.trino.operation.Values;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Operation.AttributeKey;
import io.trino.sql.newir.SourceNode;
import io.trino.sql.newir.Value;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.ValuesNode;
import org.assertj.core.util.VisibleForTesting;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.sql.dialect.trino.Context.argumentMapping;
import static io.trino.sql.dialect.trino.Context.composedMapping;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.OperationAndMapping;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.TypeConstraint.IS_RELATION;
import static io.trino.sql.dialect.trino.TypeConstraint.IS_RELATION_ROW;
import static io.trino.sql.dialect.trino.operation.Values.valuesWithoutFields;
import static java.util.Objects.requireNonNull;

/**
 * A rewriter transforming a tree of PlanNodes into a MLIR program based on `PlanVisitor`.
 * For scalar expressions, uses another rewriter `ScalarProgramBuilder` based on `IrVisitor`.
 */
public class RelationalProgramBuilder
        extends PlanVisitor<OperationAndMapping, Context>
{
    private final ProgramBuilder.ValueNameAllocator nameAllocator;
    private final ImmutableMap.Builder<Value, SourceNode> valueMap;

    public RelationalProgramBuilder(ProgramBuilder.ValueNameAllocator nameAllocator, ImmutableMap.Builder<Value, SourceNode> valueMap)
    {
        this.nameAllocator = requireNonNull(nameAllocator, "nameAllocator is null");
        this.valueMap = requireNonNull(valueMap, "valueMap is null");
    }

    @Override
    protected OperationAndMapping visitPlan(PlanNode node, Context context)
    {
        throw new UnsupportedOperationException("The new IR does not support " + node.getClass().getSimpleName() + " yet");
    }

    @Override
    public OperationAndMapping visitAggregation(AggregationNode node, Context context)
    {
        OperationAndMapping input = node.getSource().accept(this, context);
        String resultName = nameAllocator.newName();

        // model aggregate functions
        Block.Parameter aggregateParameter = new Block.Parameter(
                nameAllocator.newName(),
                input.operation().result().type());
        Block.Builder aggregateBlockBuilder = new Block.Builder(Optional.of("^aggregates"), ImmutableList.of(aggregateParameter));
        ImmutableList.Builder<AggregateCall> aggregates = ImmutableList.builder();
        AggregationStep step = AggregationStep.of(node.getStep());

        for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : node.getAggregations().entrySet()) {
            AggregateCall aggregateCall = modelAggregateCall(entry.getValue(), aggregateParameter, entry.getKey().type(), step, context, input.mapping());
            aggregates.add(aggregateCall);
            aggregateBlockBuilder.addOperation(aggregateCall);
        }

        // collect aggregate functions in a Row
        List<AggregateCall> aggregateList = aggregates.build();
        if (aggregateList.isEmpty()) {
            String constantNullName = nameAllocator.newName();
            Constant constantNull = new Constant(constantNullName, EMPTY_ROW, null);
            valueMap.put(constantNull.result(), constantNull);
            aggregateBlockBuilder.addOperation(constantNull);
        }
        else {
            String rowName = nameAllocator.newName();
            Row aggregateRow = new Row(
                    rowName,
                    aggregateList.stream()
                            .map(Operation::result)
                            .collect(toImmutableList()),
                    aggregateList.stream()
                            .map(Operation::attributes)
                            .collect(toImmutableList()));
            valueMap.put(aggregateRow.result(), aggregateRow);
            aggregateBlockBuilder.addOperation(aggregateRow);
        }
        addReturnOperation(aggregateBlockBuilder);

        Block aggregateBlock = aggregateBlockBuilder.build();
        valueMap.put(aggregateParameter, aggregateBlock);

        // grouping keys
        Block.Parameter groupingKeysSelectorParameter = new Block.Parameter(
                nameAllocator.newName(),
                irType(relationRowType(trinoType(input.operation().result().type()))));
        Block groupingKeysSelector = fieldSelectorBlock("^groupingKeysSelector", groupingKeysSelectorParameter, input.mapping(), node.getGroupingKeys());
        valueMap.put(groupingKeysSelectorParameter, groupingKeysSelector);

        // hash
        Block.Parameter hashSelectorParameter = new Block.Parameter(
                nameAllocator.newName(),
                irType(relationRowType(trinoType(input.operation().result().type()))));
        Block hashSelector = fieldSelectorBlock("^hashSelector", hashSelectorParameter, input.mapping(), node.getHashSymbol().stream().collect(toImmutableList()));
        valueMap.put(hashSelectorParameter, hashSelector);

        OptionalInt groupIdIndex = node.getGroupIdSymbol()
                .map(symbol -> node.getGroupingKeys().indexOf(symbol))
                .map(OptionalInt::of)
                .orElse(OptionalInt.empty());

        List<Integer> preGroupedIndexes = node.getPreGroupedSymbols().stream()
                .map(symbol -> node.getGroupingKeys().indexOf(symbol))
                .collect(toImmutableList());

        Aggregation aggregation = new Aggregation(
                resultName,
                input.operation().result(),
                aggregateBlock,
                groupingKeysSelector,
                hashSelector,
                node.getGroupingSetCount(),
                ImmutableList.copyOf(node.getGlobalGroupingSets()),
                groupIdIndex,
                preGroupedIndexes,
                step,
                node.isInputReducingAggregation(),
                input.operation().attributes());
        valueMap.put(aggregation.result(), aggregation);
        Map<Symbol, Integer> outputMapping = deriveOutputMapping(relationRowType(trinoType(aggregation.result().type())), node.getOutputSymbols());
        context.block().addOperation(aggregation);
        return new OperationAndMapping(aggregation, outputMapping);
    }

    private AggregateCall modelAggregateCall(
            AggregationNode.Aggregation aggregation,
            Block.Parameter aggregateParameter,
            Type outputType,
            AggregationStep step,
            Context outerContext,
            Map<Symbol, Integer> inputMapping)
    {
        String resultName = nameAllocator.newName();

        // input to aggregate calls is a group -- it is of relation type
        // internal structures of an aggregate, like arguments or filter, are defined in terms of a single row
        Type inputRowType = relationRowType(trinoType(aggregateParameter.type()));

        // arguments
        Block.Parameter argumentsParameter = new Block.Parameter(
                nameAllocator.newName(),
                irType(inputRowType));
        Block.Builder argumentsBuilder = new Block.Builder(Optional.of("^arguments"), ImmutableList.of(argumentsParameter));

        // collect all arguments in a Row or EMPTY_ROW if there are none
        if (aggregation.getArguments().isEmpty()) {
            String constantNullName = nameAllocator.newName();
            Constant constantNull = new Constant(constantNullName, EMPTY_ROW, null);
            valueMap.put(constantNull.result(), constantNull);
            argumentsBuilder.addOperation(constantNull);
        }
        else {
            io.trino.sql.ir.Row argumentsRow = new io.trino.sql.ir.Row(ImmutableList.copyOf(aggregation.getArguments()));
            argumentsRow.accept(
                    new ScalarProgramBuilder(nameAllocator, valueMap),
                    new Context(argumentsBuilder, composedMapping(outerContext, argumentMapping(argumentsParameter, inputMapping))));
        }
        addReturnOperation(argumentsBuilder);
        Block arguments = argumentsBuilder.build();
        valueMap.put(argumentsParameter, arguments);

        // filter
        Block.Parameter filterParameter = new Block.Parameter(
                nameAllocator.newName(),
                irType(inputRowType));
        Block filterSelector = fieldSelectorBlock("^filterSelector", filterParameter, inputMapping, aggregation.getFilter().stream().collect(toImmutableList()));
        valueMap.put(filterParameter, filterSelector);

        // mask
        Block.Parameter maskParameter = new Block.Parameter(
                nameAllocator.newName(),
                irType(inputRowType));
        Block maskSelector = fieldSelectorBlock("^maskSelector", maskParameter, inputMapping, aggregation.getMask().stream().collect(toImmutableList()));
        valueMap.put(maskParameter, maskSelector);

        // order by
        Block.Parameter orderingSelectorParameter = new Block.Parameter(
                nameAllocator.newName(),
                irType(inputRowType));
        Block orderingSelector = fieldSelectorBlock("^orderingSelector", orderingSelectorParameter, inputMapping, aggregation.getOrderingScheme().map(OrderingScheme::orderBy).orElse(ImmutableList.of()));
        valueMap.put(orderingSelectorParameter, orderingSelector);

        AggregateCall aggregateCall = new AggregateCall(
                resultName,
                aggregateParameter,
                outputType,
                arguments,
                filterSelector,
                maskSelector,
                orderingSelector,
                aggregation.getOrderingScheme()
                        .map(OrderingScheme::orderingList)
                        .map(SortOrderList::new),
                aggregation.getResolvedFunction(),
                aggregation.isDistinct(),
                step);
        valueMap.put(aggregateCall.result(), aggregateCall);
        return aggregateCall;
    }

    @Override
    public OperationAndMapping visitCorrelatedJoin(CorrelatedJoinNode node, Context context)
    {
        OperationAndMapping input = node.getInput().accept(this, context);
        String resultName = nameAllocator.newName();

        Type inputRowType = relationRowType(trinoType(input.operation().result().type()));

        // model correlation as field selection (lambda)
        Block.Parameter correlationParameter = new Block.Parameter(
                nameAllocator.newName(),
                irType(inputRowType));
        Block correlation = fieldSelectorBlock("^correlationSelector", correlationParameter, input.mapping(), node.getCorrelation());
        valueMap.put(correlationParameter, correlation);

        // model subquery as a lambda
        Block.Parameter subqueryParameter = new Block.Parameter(
                nameAllocator.newName(),
                irType(inputRowType));
        Block.Builder subqueryBuilder = new Block.Builder(Optional.of("^subquery"), ImmutableList.of(subqueryParameter));
        node.getSubquery().accept(
                this,
                new Context(subqueryBuilder, composedMapping(context, argumentMapping(subqueryParameter, input.mapping()))));
        addReturnOperation(subqueryBuilder);
        Map<Symbol, Integer> subqueryMapping = deriveOutputMapping(relationRowType(trinoType(subqueryBuilder.recentOperation().result().type())), node.getSubquery().getOutputSymbols());
        Map<AttributeKey, Object> subqueryAttributes = subqueryBuilder.recentOperation().attributes();
        Block subquery = subqueryBuilder.build();
        valueMap.put(subqueryParameter, subquery);

        // model filter as a lambda
        Block.Parameter firstFilterParameter = new Block.Parameter(
                nameAllocator.newName(),
                irType(inputRowType));
        Block.Parameter secondFilterParameter = new Block.Parameter(
                nameAllocator.newName(),
                irType(relationRowType(trinoType(subquery.getReturnedType()))));
        Block.Builder filterBuilder = new Block.Builder(Optional.of("^filter"), ImmutableList.of(firstFilterParameter, secondFilterParameter));
        node.getFilter().accept(
                new ScalarProgramBuilder(nameAllocator, valueMap),
                new Context(filterBuilder, composedMapping(context, ImmutableList.of(
                        argumentMapping(firstFilterParameter, input.mapping()),
                        argumentMapping(secondFilterParameter, subqueryMapping)))));
        addReturnOperation(filterBuilder);
        Block filter = filterBuilder.build();
        valueMap.put(firstFilterParameter, filter);
        valueMap.put(secondFilterParameter, filter);

        CorrelatedJoin correlatedJoin = new CorrelatedJoin(
                resultName,
                input.operation().result(),
                correlation,
                subquery,
                filter,
                JoinType.of(node.getType()),
                input.operation().attributes(),
                subqueryAttributes);
        valueMap.put(correlatedJoin.result(), correlatedJoin);

        Map<Symbol, Integer> outputMapping = deriveOutputMapping(relationRowType(trinoType(correlatedJoin.result().type())), node.getOutputSymbols());
        context.block().addOperation(correlatedJoin);
        return new OperationAndMapping(correlatedJoin, outputMapping);
    }

    @Override
    public OperationAndMapping visitExchange(ExchangeNode node, Context context)
    {
        List<OperationAndMapping> inputs = node.getSources().stream()
                .map(source -> source.accept(this, context))
                .collect(toImmutableList());
        String resultName = nameAllocator.newName();

        // input field selectors
        ImmutableList.Builder<Block> inputSelectorsBuilder = ImmutableList.builder();
        for (int i = 0; i < node.getSources().size(); i++) {
            Block.Parameter inputSelectorParameter = new Block.Parameter(
                    nameAllocator.newName(),
                    irType(relationRowType(trinoType(inputs.get(i).operation().result().type()))));
            Block inputSelector = fieldSelectorBlock("^inputSelector", inputSelectorParameter, inputs.get(i).mapping(), node.getInputs().get(i));
            valueMap.put(inputSelectorParameter, inputSelector);
            inputSelectorsBuilder.add(inputSelector);
        }
        List<Block> inputSelectors = inputSelectorsBuilder.build();

        // per ExchangeNode constructor, inputs is not empty. inputSelectors is not empty too.
        List<Type> inputFieldTypes = trinoType(inputSelectors.getFirst().getReturnedType()).getTypeParameters();
        Type exchangeRowType = inputFieldTypes.isEmpty() ? EMPTY_ROW : RowType.anonymous(inputFieldTypes);
        Map<Symbol, Integer> outputMapping = deriveOutputMapping(exchangeRowType, node.getOutputSymbols());

        // partitioning bound arguments
        Block.Parameter boundArgumentsParameter = new Block.Parameter(
                nameAllocator.newName(),
                irType(exchangeRowType));
        Block.Builder boundArgumentsBuilder = new Block.Builder(Optional.of("^boundArguments"), ImmutableList.of(boundArgumentsParameter));

        // collect all bound arguments in a Row or EMPTY_ROW if there are none
        if (node.getPartitioningScheme().getPartitioning().getArguments().isEmpty()) {
            String constantNullName = nameAllocator.newName();
            Constant constantNull = new Constant(constantNullName, EMPTY_ROW, null);
            valueMap.put(constantNull.result(), constantNull);
            boundArgumentsBuilder.addOperation(constantNull);
        }
        else {
            io.trino.sql.ir.Row boundArgumentsRow = new io.trino.sql.ir.Row(ImmutableList.copyOf(node.getPartitioningScheme().getPartitioning().getArguments().stream()
                    .map(argumentBinding -> {
                        if (argumentBinding.isConstant()) {
                            NullableValue nullableValue = argumentBinding.getConstant();
                            return new io.trino.sql.ir.Constant(nullableValue.getType(), nullableValue.getValue());
                        }
                        else {
                            return argumentBinding.getExpression();
                        }
                    })
                    .collect(toImmutableList())));
            boundArgumentsRow.accept(
                    new ScalarProgramBuilder(nameAllocator, valueMap),
                    new Context(boundArgumentsBuilder, composedMapping(context, argumentMapping(boundArgumentsParameter, outputMapping)))); // bound arguments are defined in terms of exchange's output symbols
        }
        addReturnOperation(boundArgumentsBuilder);
        Block boundArguments = boundArgumentsBuilder.build();
        valueMap.put(boundArgumentsParameter, boundArguments);

        // partitioning hash column
        Block.Parameter hashSelectorParameter = new Block.Parameter(
                nameAllocator.newName(),
                irType(exchangeRowType));
        Block hashSelector = fieldSelectorBlock("^hashSelector", hashSelectorParameter, outputMapping, node.getPartitioningScheme().getHashColumn().stream().collect(toImmutableList())); // hash column is defined in terms of exchange's output symbols
        valueMap.put(hashSelectorParameter, hashSelector);

        // order by
        Block.Parameter orderingSelectorParameter = new Block.Parameter(
                nameAllocator.newName(),
                irType(exchangeRowType));
        Block orderingSelector = fieldSelectorBlock("^orderingSelector", orderingSelectorParameter, outputMapping, node.getOrderingScheme().map(OrderingScheme::orderBy).orElse(ImmutableList.of())); // ordering scheme is defined in terms of exchange's output symbols
        valueMap.put(orderingSelectorParameter, orderingSelector);

        NullableValue[] nullableValues = new NullableValue[node.getPartitioningScheme().getPartitioning().getArguments().size()];
        for (int i = 0; i < node.getPartitioningScheme().getPartitioning().getArguments().size(); i++) {
            nullableValues[i] = node.getPartitioningScheme().getPartitioning().getArguments().get(i).getConstant();
        }

        Exchange exchange = new Exchange(
                resultName,
                inputs.stream()
                        .map(OperationAndMapping::operation)
                        .map(Operation::result)
                        .collect(toImmutableList()),
                inputSelectors,
                boundArguments,
                hashSelector,
                orderingSelector,
                ExchangeType.of(node.getType()),
                ExchangeScope.of(node.getScope()),
                node.getPartitioningScheme().getPartitioning().getHandle(),
                new NullableValues(nullableValues),
                node.getPartitioningScheme().isReplicateNullsAndAny(),
                node.getPartitioningScheme().getBucketToPartition()
                        .map(Arrays::stream)
                        .map(IntStream::boxed)
                        .map(stream -> stream.collect(toImmutableList())),
                node.getPartitioningScheme().getPartitionCount(),
                node.getOrderingScheme()
                        .map(OrderingScheme::orderingList)
                        .map(SortOrderList::new),
                inputs.stream()
                        .map(OperationAndMapping::operation)
                        .map(Operation::attributes)
                        .collect(toImmutableList()));
        valueMap.put(exchange.result(), exchange);

        context.block().addOperation(exchange);
        return new OperationAndMapping(exchange, outputMapping);
    }

    @Override
    public OperationAndMapping visitFilter(FilterNode node, Context context)
    {
        OperationAndMapping input = node.getSource().accept(this, context);
        String resultName = nameAllocator.newName();

        // model filter predicate as a lambda (Block)
        Block.Parameter predicateParameter = new Block.Parameter(
                nameAllocator.newName(),
                irType(relationRowType(trinoType(input.operation().result().type()))));
        Block.Builder predicateBuilder = new Block.Builder(Optional.of("^predicate"), ImmutableList.of(predicateParameter));

        node.getPredicate().accept(
                new ScalarProgramBuilder(nameAllocator, valueMap),
                new Context(predicateBuilder, composedMapping(context, argumentMapping(predicateParameter, input.mapping()))));

        addReturnOperation(predicateBuilder);

        Block predicate = predicateBuilder.build();
        valueMap.put(predicateParameter, predicate);

        Filter filter = new Filter(resultName, input.operation().result(), predicate, input.operation().attributes());
        valueMap.put(filter.result(), filter);
        Map<Symbol, Integer> outputMapping = deriveOutputMapping(relationRowType(trinoType(filter.result().type())), node.getOutputSymbols());
        context.block().addOperation(filter);
        return new OperationAndMapping(filter, outputMapping);
    }

    @Override
    public OperationAndMapping visitJoin(JoinNode node, Context context)
    {
        OperationAndMapping left = node.getLeft().accept(this, context);
        OperationAndMapping right = node.getRight().accept(this, context);
        String resultName = nameAllocator.newName();

        Type leftRowType = relationRowType(trinoType(left.operation().result().type()));
        Type rightRowType = relationRowType(trinoType(right.operation().result().type()));

        // model join criteria as left and right field selectors
        List<Symbol> leftCriteriaSymbols = node.getCriteria().stream()
                .map(JoinNode.EquiJoinClause::getLeft)
                .collect(toImmutableList());
        Block.Parameter leftCriteriaSelectorParameter = new Block.Parameter(
                nameAllocator.newName(),
                irType(leftRowType));
        Block leftCriteriaSelector = fieldSelectorBlock("^leftCriteriaSelector", leftCriteriaSelectorParameter, left.mapping(), leftCriteriaSymbols);
        valueMap.put(leftCriteriaSelectorParameter, leftCriteriaSelector);

        List<Symbol> rightCriteriaSymbols = node.getCriteria().stream()
                .map(JoinNode.EquiJoinClause::getRight)
                .collect(toImmutableList());
        Block.Parameter rightCriteriaSelectorParameter = new Block.Parameter(
                nameAllocator.newName(),
                irType(rightRowType));
        Block rightCriteriaSelector = fieldSelectorBlock("^rightCriteriaSelector", rightCriteriaSelectorParameter, right.mapping(), rightCriteriaSymbols);
        valueMap.put(rightCriteriaSelectorParameter, rightCriteriaSelector);

        // join filter
        Block.Parameter leftFilterParameter = new Block.Parameter(
                nameAllocator.newName(),
                irType(leftRowType));
        Block.Parameter rightFilterParameter = new Block.Parameter(
                nameAllocator.newName(),
                irType(rightRowType));
        Block.Builder filterBuilder = new Block.Builder(Optional.of("^filter"), ImmutableList.of(leftFilterParameter, rightFilterParameter));
        node.getFilter().orElse(new io.trino.sql.ir.Constant(BOOLEAN, true)).accept(
                new ScalarProgramBuilder(nameAllocator, valueMap),
                new Context(filterBuilder, composedMapping(context, ImmutableList.of(
                        argumentMapping(leftFilterParameter, left.mapping()),
                        argumentMapping(rightFilterParameter, right.mapping())))));
        addReturnOperation(filterBuilder);
        Block filter = filterBuilder.build();
        valueMap.put(leftFilterParameter, filter);
        valueMap.put(rightFilterParameter, filter);

        // left hash symbol
        Block.Parameter leftHashSelectorParameter = new Block.Parameter(
                nameAllocator.newName(),
                irType(leftRowType));
        Block leftHashSelector = fieldSelectorBlock("^leftHashSelector", leftHashSelectorParameter, left.mapping(), node.getLeftHashSymbol().stream().collect(toImmutableList()));
        valueMap.put(leftHashSelectorParameter, leftHashSelector);

        // right hash symbol
        Block.Parameter rightHashSelectorParameter = new Block.Parameter(
                nameAllocator.newName(),
                irType(rightRowType));
        Block rightHashSelector = fieldSelectorBlock("^rightHashSelector", rightHashSelectorParameter, right.mapping(), node.getRightHashSymbol().stream().collect(toImmutableList()));
        valueMap.put(rightHashSelectorParameter, rightHashSelector);

        // left output symbols
        Block.Parameter leftOutputSelectorParameter = new Block.Parameter(
                nameAllocator.newName(),
                irType(leftRowType));
        Block leftOutputSelector = fieldSelectorBlock("^leftOutputSelector", leftOutputSelectorParameter, left.mapping(), node.getLeftOutputSymbols());
        valueMap.put(leftOutputSelectorParameter, leftOutputSelector);

        // right output symbols
        Block.Parameter rightOutputSelectorParameter = new Block.Parameter(
                nameAllocator.newName(),
                irType(rightRowType));
        Block rightOutputSelector = fieldSelectorBlock("^rightOutputSelector", rightOutputSelectorParameter, right.mapping(), node.getRightOutputSymbols());
        valueMap.put(rightOutputSelectorParameter, rightOutputSelector);

        // model dynamic filters as an attribute containing dynamic filter IDs and selector block for corresponding build side symbols
        List<Symbol> dynamicFilterTargets = node.getDynamicFilters().entrySet().stream()
                .map(Map.Entry::getValue)
                .collect(toImmutableList());
        Block.Parameter dynamicFilterTargetSelectorParameter = new Block.Parameter(
                nameAllocator.newName(),
                irType(rightRowType));
        Block dynamicFilterTargetSelector = fieldSelectorBlock("^dynamicFilterTargetSelector", dynamicFilterTargetSelectorParameter, right.mapping(), dynamicFilterTargets);
        valueMap.put(dynamicFilterTargetSelectorParameter, dynamicFilterTargetSelector);

        List<String> dynamicFilterIds = node.getDynamicFilters().entrySet().stream()
                .map(Map.Entry::getKey)
                .map(DynamicFilterId::toString)
                .collect(toImmutableList());

        Join join = new Join(
                resultName,
                left.operation().result(),
                right.operation().result(),
                leftCriteriaSelector,
                rightCriteriaSelector,
                filter,
                leftHashSelector,
                rightHashSelector,
                leftOutputSelector,
                rightOutputSelector,
                dynamicFilterTargetSelector,
                JoinType.of(node.getType()),
                node.isMaySkipOutputDuplicates(),
                node.getDistributionType().map(DistributionType::of),
                node.isSpillable(),
                dynamicFilterIds,
                node.getReorderJoinStatsAndCost(),
                left.operation().attributes(),
                right.operation().attributes());
        valueMap.put(join.result(), join);
        Map<Symbol, Integer> outputMapping = deriveOutputMapping(relationRowType(trinoType(join.result().type())), node.getOutputSymbols());
        context.block().addOperation(join);
        return new OperationAndMapping(join, outputMapping);
    }

    @Override
    public OperationAndMapping visitLimit(LimitNode node, Context context)
    {
        OperationAndMapping input = node.getSource().accept(this, context);
        String resultName = nameAllocator.newName();

        // order by
        Block.Parameter orderingSelectorParameter = new Block.Parameter(
                nameAllocator.newName(),
                irType(relationRowType(trinoType(input.operation().result().type()))));
        Block orderingSelector = fieldSelectorBlock("^orderingSelector", orderingSelectorParameter, input.mapping(), node.getTiesResolvingScheme().map(OrderingScheme::orderBy).orElse(ImmutableList.of()));
        valueMap.put(orderingSelectorParameter, orderingSelector);

        List<Integer> preSortedIndexes = node.getPreSortedInputs().stream()
                .map(symbol -> node.getTiesResolvingScheme().map(OrderingScheme::orderBy).orElse(ImmutableList.of()).indexOf(symbol))
                .collect(toImmutableList());

        Limit limit = new Limit(
                resultName,
                input.operation().result(),
                orderingSelector,
                node.getTiesResolvingScheme()
                        .map(OrderingScheme::orderingList)
                        .map(SortOrderList::new),
                node.getCount(),
                node.isPartial(),
                preSortedIndexes,
                input.operation().attributes());
        valueMap.put(limit.result(), limit);
        Map<Symbol, Integer> outputMapping = deriveOutputMapping(relationRowType(trinoType(limit.result().type())), node.getOutputSymbols());
        context.block().addOperation(limit);
        return new OperationAndMapping(limit, outputMapping);
    }

    @Override
    public OperationAndMapping visitOutput(OutputNode node, Context context)
    {
        OperationAndMapping input = node.getSource().accept(this, context);
        String resultName = nameAllocator.newName();

        // model output fields selection as a lambda (Block)
        Block.Parameter fieldSelectorParameter = new Block.Parameter(
                nameAllocator.newName(),
                irType(relationRowType(trinoType(input.operation().result().type()))));
        Block fieldSelectorBlock = fieldSelectorBlock("^outputFieldSelector", fieldSelectorParameter, input.mapping(), node.getOutputSymbols());
        valueMap.put(fieldSelectorParameter, fieldSelectorBlock);

        Output output = new Output(resultName, input.operation().result(), fieldSelectorBlock, node.getColumnNames());
        valueMap.put(output.result(), output);
        context.block().addOperation(output);
        return new OperationAndMapping(output, ImmutableMap.of()); // unlike OutputNode, the Output operation returns Void, not a relation
    }

    @Override
    public OperationAndMapping visitProject(ProjectNode node, Context context)
    {
        OperationAndMapping input = node.getSource().accept(this, context);
        String resultName = nameAllocator.newName();

        // model assignments as a lambda (Block)
        Block.Parameter assignmentsParameter = new Block.Parameter(
                nameAllocator.newName(),
                irType(relationRowType(trinoType(input.operation().result().type()))));
        Block.Builder assignmentsBuilder = new Block.Builder(Optional.of("^assignments"), ImmutableList.of(assignmentsParameter));

        // collect all assignments in a Row or EMPTY_ROW if there are none
        if (node.getAssignments().isEmpty()) {
            String constantNullName = nameAllocator.newName();
            Constant constantNull = new Constant(constantNullName, EMPTY_ROW, null);
            valueMap.put(constantNull.result(), constantNull);
            assignmentsBuilder.addOperation(constantNull);
        }
        else {
            io.trino.sql.ir.Row assignmentsRow = new io.trino.sql.ir.Row(ImmutableList.copyOf(node.getAssignments().getExpressions()));
            assignmentsRow.accept(
                    new ScalarProgramBuilder(nameAllocator, valueMap),
                    new Context(assignmentsBuilder, composedMapping(context, argumentMapping(assignmentsParameter, input.mapping()))));
        }
        addReturnOperation(assignmentsBuilder);
        Block assignments = assignmentsBuilder.build();
        valueMap.put(assignmentsParameter, assignments);

        Project project = new Project(resultName, input.operation().result(), assignments, input.operation().attributes());
        valueMap.put(project.result(), project);
        Map<Symbol, Integer> outputMapping = deriveOutputMapping(relationRowType(trinoType(project.result().type())), node.getOutputSymbols());
        context.block().addOperation(project);
        return new OperationAndMapping(project, outputMapping);
    }

    @Override
    public OperationAndMapping visitTableScan(TableScanNode node, Context context)
    {
        String resultName = nameAllocator.newName();

        Type rowType;
        if (node.getOutputSymbols().isEmpty()) {
            rowType = EMPTY_ROW;
        }
        else {
            rowType = RowType.anonymous(node.getOutputSymbols().stream()
                    .map(Symbol::type)
                    .collect(toImmutableList()));
        }

        Optional<Statistics> statistics = node.getStatistics().map(estimate -> mapStatistics(estimate, node.getOutputSymbols()));

        TableScan tableScan = new TableScan(
                resultName,
                rowType,
                node.getTable(),
                node.getOutputSymbols().stream()
                        .map(node.getAssignments()::get)
                        .collect(toImmutableList()),
                node.getEnforcedConstraint(),
                statistics,
                node.isUpdateTarget(),
                node.getUseConnectorNodePartitioning());
        valueMap.put(tableScan.result(), tableScan);
        Map<Symbol, Integer> outputMapping = deriveOutputMapping(relationRowType(trinoType(tableScan.result().type())), node.getOutputSymbols());
        context.block().addOperation(tableScan);
        return new OperationAndMapping(tableScan, outputMapping);
    }

    @VisibleForTesting
    public static Statistics mapStatistics(PlanNodeStatsEstimate estimate, List<Symbol> outputSymbols)
    {
        return new Statistics(
                estimate.getOutputRowCount(),
                estimate.getSymbolStatistics().entrySet().stream()
                        .collect(Collectors.toMap(
                                entry -> outputSymbols.indexOf(entry.getKey()),
                                Map.Entry::getValue)));
    }

    @Override
    public OperationAndMapping visitTopN(TopNNode node, Context context)
    {
        OperationAndMapping input = node.getSource().accept(this, context);
        String resultName = nameAllocator.newName();

        // order by
        Block.Parameter orderingSelectorParameter = new Block.Parameter(
                nameAllocator.newName(),
                irType(relationRowType(trinoType(input.operation().result().type()))));
        Block orderingSelector = fieldSelectorBlock("^orderingSelector", orderingSelectorParameter, input.mapping(), node.getOrderingScheme().orderBy());
        valueMap.put(orderingSelectorParameter, orderingSelector);

        TopN topN = new TopN(
                resultName,
                input.operation().result(),
                orderingSelector,
                new SortOrderList(node.getOrderingScheme().orderingList()),
                node.getCount(),
                TopNStep.of(node.getStep()),
                input.operation().attributes());
        valueMap.put(topN.result(), topN);
        Map<Symbol, Integer> outputMapping = deriveOutputMapping(relationRowType(trinoType(topN.result().type())), node.getOutputSymbols());
        context.block().addOperation(topN);
        return new OperationAndMapping(topN, outputMapping);
    }

    @Override
    public OperationAndMapping visitValues(ValuesNode node, Context context)
    {
        String resultName = nameAllocator.newName();

        Values values;
        if (node.getOutputSymbols().isEmpty()) {
            values = valuesWithoutFields(resultName, node.getRowCount());
        }
        else {
            // model each component row of Values as a no-argument Block
            // TODO we could use sequential names for blocks: "row_1", "row_2"...
            List<Block> rows = node.getRows().orElseThrow().stream()
                    .map(rowExpression -> {
                        Block.Builder rowBlock = new Block.Builder(Optional.of("^row"), ImmutableList.of());
                        rowExpression.accept(
                                new ScalarProgramBuilder(nameAllocator, valueMap),
                                new Context(rowBlock, context.symbolMapping()));
                        addReturnOperation(rowBlock);
                        return rowBlock.build();
                    })
                    .collect(toImmutableList());
            RowType rowType = RowType.anonymous(node.getOutputSymbols().stream()
                    .map(Symbol::type)
                    .collect(toImmutableList()));
            values = new Values(resultName, rowType, rows);
        }
        valueMap.put(values.result(), values);
        Map<Symbol, Integer> outputMapping = deriveOutputMapping(relationRowType(trinoType(values.result().type())), node.getOutputSymbols());
        context.block().addOperation(values);
        return new OperationAndMapping(values, outputMapping);
    }

    /**
     * A type of relation is represented as MultisetType of row type being either RowType or EmptyRowType. This method extracts the element type.
     */
    public static Type relationRowType(Type relationType)
    {
        if (!IS_RELATION.test(relationType)) {
            throw new TrinoException(IR_ERROR, "not a relation type. expected multiset of row with anonymous fields");
        }

        return ((MultisetType) relationType).getElementType();
    }

    /**
     * Map each output symbol of the PlanNode to a corresponding field index in the Operations output row type.
     */
    @VisibleForTesting
    public static Map<Symbol, Integer> deriveOutputMapping(Type relationRowType, List<Symbol> outputSymbols)
    {
        if (!IS_RELATION_ROW.test(relationRowType)) {
            throw new TrinoException(IR_ERROR, "not a relation row type. expected RowType with anonymous fields or EmptyRowType");
        }

        if (relationRowType.equals(EMPTY_ROW)) {
            if (!outputSymbols.isEmpty()) {
                throw new TrinoException(IR_ERROR, "relation row type mismatch: output symbols present for EmptyRowType");
            }
            return ImmutableMap.of();
        }

        RowType rowType = (RowType) relationRowType;
        if (rowType.getFields().size() != outputSymbols.size()) {
            throw new TrinoException(IR_ERROR, "relation RowType does not match output symbols");
        }

        // Using a HashMap because it can handle duplicates.
        // If a PlanNode outputs some symbol twice, we will use the first occurrence for mapping.
        // As a result, the downstream references to the symbol will be mapped to the same output field,
        // and the other field will be unused and eligible for pruning.
        Map<Symbol, Integer> mapping = HashMap.newHashMap(outputSymbols.size());
        for (int i = 0; i < outputSymbols.size(); i++) {
            Symbol symbol = outputSymbols.get(i);
            RowType.Field field = rowType.getFields().get(i);
            if (!symbol.type().equals(field.getType())) {
                throw new TrinoException(IR_ERROR, "symbol type does not match field type");
            }
            mapping.putIfAbsent(symbol, i);
        }
        return mapping;
    }

    private Block fieldSelectorBlock(String blockName, Block.Parameter inputRow, Map<Symbol, Integer> inputSymbolMapping, List<Symbol> selectedSymbolsList)
    {
        return fieldSelectorBlock(
                blockName,
                ImmutableList.of(inputRow),
                ImmutableList.of(inputSymbolMapping),
                ImmutableList.of(selectedSymbolsList));
    }

    /**
     * A helper method to express input field selection as a lambda.
     * Useful for operations which pass selected input fields on output, for example join, unnest.
     * Also useful for selecting input columns necessary for the operation's logic, for example ordering columns, partitioning columns.
     * // TODO this method does not support selecting correlated fields. It needs context to access them.
     *
     * @param blockName name for the result Block
     * @param inputRows arguments to the lambda representing input rows from which we want to select fields
     * @param inputSymbolMappings mapping symbol --> row field name for each input row
     * @param selectedSymbolsLists list of symbols to select from each input row
     * @return a row containing selected fields from all input rows
     */
    @VisibleForTesting
    public Block fieldSelectorBlock(String blockName, List<Block.Parameter> inputRows, List<Map<Symbol, Integer>> inputSymbolMappings, List<List<Symbol>> selectedSymbolsLists)
    {
        if (inputRows.size() != inputSymbolMappings.size()) {
            throw new TrinoException(IR_ERROR, "inputs and input symbol mappings do not match");
        }
        if (inputRows.size() != selectedSymbolsLists.size()) {
            throw new TrinoException(IR_ERROR, "inputs and symbol lists do not match");
        }

        Block.Builder selectorBlock = new Block.Builder(Optional.of(blockName), inputRows);

        // if there are no selected symbols, the selector block returns a constant null of type EMPTY_ROW
        if (selectedSymbolsLists.stream().mapToInt(List::size).sum() == 0) {
            String constantNullName = nameAllocator.newName();
            Constant constantNull = new Constant(constantNullName, EMPTY_ROW, null);
            valueMap.put(constantNull.result(), constantNull);
            selectorBlock.addOperation(constantNull);

            addReturnOperation(selectorBlock);

            return selectorBlock.build();
        }

        ImmutableList.Builder<Operation> selections = ImmutableList.builder();
        for (int i = 0; i < inputRows.size(); i++) {
            Block.Parameter parameter = inputRows.get(i);
            Map<Symbol, Integer> symbolMapping = inputSymbolMappings.get(i);
            List<Symbol> symbols = selectedSymbolsLists.get(i);
            for (Symbol symbol : symbols) {
                String value = nameAllocator.newName();
                FieldReference fieldReference = new FieldReference(value, parameter, symbolMapping.get(symbol), ImmutableMap.of()); // TODO pass appropriate row-specific input attributes through lambda arguments
                valueMap.put(fieldReference.result(), fieldReference);
                selectorBlock.addOperation(fieldReference);
                selections.add(fieldReference);
            }
        }
        // build a row of selected items
        String rowValue = nameAllocator.newName();
        Row rowConstructor = new Row(
                rowValue,
                selections.build().stream().map(Operation::result).collect(toImmutableList()),
                selections.build().stream().map(Operation::attributes).collect(toImmutableList()));
        valueMap.put(rowConstructor.result(), rowConstructor);
        selectorBlock.addOperation(rowConstructor);

        addReturnOperation(selectorBlock);

        return selectorBlock.build();
    }

    /**
     * Return the value of the recent operation in the builder
     */
    @VisibleForTesting
    public void addReturnOperation(Block.Builder builder)
    {
        String returnValue = nameAllocator.newName();
        Operation recentOperation = builder.recentOperation();
        Return returnOperation = new Return(returnValue, recentOperation.result(), recentOperation.attributes());
        valueMap.put(returnOperation.result(), returnOperation);
        builder.addOperation(returnOperation);
    }

    /**
     * A result of transforming a PlanNode into an Operation.
     * Maps each output symbol of the PlanNode to a corresponding field index in the Operations output RowType.
     */
    public record OperationAndMapping(Operation operation, Map<Symbol, Integer> mapping)
    {
        public OperationAndMapping(Operation operation, Map<Symbol, Integer> mapping)
        {
            this.operation = requireNonNull(operation, "operation is null");
            this.mapping = ImmutableMap.copyOf(requireNonNull(mapping, "mapping is null"));
        }
    }
}
