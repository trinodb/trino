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
import com.google.common.collect.ImmutableSet;
import io.trino.cost.PlanNodeStatsAndCostSummary;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.TestingColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.MultisetType;
import io.trino.spi.type.RowType;
import io.trino.sql.dialect.trino.Attributes.NullableValues;
import io.trino.sql.dialect.trino.Attributes.SortOrderList;
import io.trino.sql.dialect.trino.Attributes.Statistics;
import io.trino.sql.dialect.trino.ProgramBuilder.ValueNameAllocator;
import io.trino.sql.dialect.trino.RelationalProgramBuilder.OperationAndMapping;
import io.trino.sql.dialect.trino.operation.AggregateCall;
import io.trino.sql.dialect.trino.operation.Aggregation;
import io.trino.sql.dialect.trino.operation.Comparison;
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
import io.trino.sql.ir.Reference;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Operation.AttributeKey;
import io.trino.sql.newir.Type;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TestingConnectorTransactionHandle;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.GroupingSetDescriptor;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.tree.Identifier;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.server.protocol.spooling.SpooledBlock.SPOOLING_METADATA_SYMBOL;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.spi.connector.SortOrder.DESC_NULLS_FIRST;
import static io.trino.spi.connector.SortOrder.DESC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.spi.type.RowType.anonymousRow;
import static io.trino.spi.type.RowType.rowType;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.dialect.trino.Attributes.AggregationStep.SINGLE;
import static io.trino.sql.dialect.trino.Attributes.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.dialect.trino.Attributes.DistributionType.REPLICATED;
import static io.trino.sql.dialect.trino.Attributes.ExchangeScope.REMOTE;
import static io.trino.sql.dialect.trino.Attributes.ExchangeType.GATHER;
import static io.trino.sql.dialect.trino.Attributes.JoinType.LEFT;
import static io.trino.sql.dialect.trino.Attributes.TopNStep.FINAL;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.deriveOutputMapping;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.mapStatistics;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.operation.Values.valuesWithoutFields;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static java.lang.Boolean.TRUE;
import static java.lang.Double.NaN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestRelationalProgramBuilder
{
    private static final ValuesNode VALUES_NODE = valuesNode();
    private static final Values VALUES_OPERATION = valuesOperation();
    private static final Type VALUES_OPERATION_ROW_TYPE = irType(relationRowType(trinoType(VALUES_OPERATION.result().type())));
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();

    @Test
    public void testAggregation()
    {
        ResolvedFunction sumFunction = FUNCTION_RESOLUTION.resolveFunction("sum", fromTypes(BIGINT));

        AggregationNode aggregationNode = new AggregationNode(
                new PlanNodeId("aggregation"),
                VALUES_NODE,
                ImmutableMap.of(
                        new Symbol(BIGINT, "sum_agg"),
                        new AggregationNode.Aggregation(
                                sumFunction,
                                ImmutableList.of(new Reference(BIGINT, "a")),
                                false,
                                Optional.of(new Symbol(BOOLEAN, "b")),
                                Optional.of(new OrderingScheme(ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b")), ImmutableMap.of(new Symbol(BOOLEAN, "b"), ASC_NULLS_FIRST, new Symbol(BIGINT, "a"), DESC_NULLS_LAST))),
                                Optional.empty())),
                new GroupingSetDescriptor(ImmutableList.of(new Symbol(BOOLEAN, "b")), 1, ImmutableSet.of()),
                ImmutableList.of(new Symbol(BOOLEAN, "b")),
                AggregationNode.Step.SINGLE,
                Optional.of(new Symbol(BIGINT, "a")),
                Optional.empty(),
                Optional.of(true));

        // aggregate parameter
        Block.Parameter aggregateParameter = new Block.Parameter(
                "%10",
                VALUES_OPERATION.result().type());

        // aggregate argument
        Block.Parameter argumentsParameter = new Block.Parameter(
                "%12",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationArgument = new FieldReference("%13", argumentsParameter, 0, ImmutableMap.of());
        Row rowOperationArgument = new Row("%14", ImmutableList.of(fieldReferenceOperationArgument.result()), ImmutableList.of(fieldReferenceOperationArgument.attributes()));
        Return returnOperationArgument = new Return("%15", rowOperationArgument.result(), rowOperationArgument.attributes());

        // aggregate filter
        Block.Parameter filterParameter = new Block.Parameter(
                "%16",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationFilter = new FieldReference("%17", filterParameter, 1, ImmutableMap.of());
        Row rowOperationFilter = new Row("%18", ImmutableList.of(fieldReferenceOperationFilter.result()), ImmutableList.of(fieldReferenceOperationFilter.attributes()));
        Return returnOperationFilter = new Return("%19", rowOperationFilter.result(), rowOperationFilter.attributes());

        // aggregate mask
        Block.Parameter maskParameter = new Block.Parameter(
                "%20",
                VALUES_OPERATION_ROW_TYPE);
        Constant constantOperationMask = new Constant("%21", EMPTY_ROW, null);
        Return returnOperationMask = new Return("%22", constantOperationMask.result(), constantOperationMask.attributes());

        // aggregate ordering
        Block.Parameter orderingParameter = new Block.Parameter(
                "%23",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationOrderingA = new FieldReference("%24", orderingParameter, 0, ImmutableMap.of());
        FieldReference fieldReferenceOperationOrderingB = new FieldReference("%25", orderingParameter, 1, ImmutableMap.of());
        Row rowOperationOrdering = new Row(
                "%26",
                ImmutableList.of(fieldReferenceOperationOrderingA.result(), fieldReferenceOperationOrderingB.result()),
                ImmutableList.of(fieldReferenceOperationOrderingA.attributes(), fieldReferenceOperationOrderingB.attributes()));
        Return returnOperationOrdering = new Return("%27", rowOperationOrdering.result(), rowOperationOrdering.attributes());

        AggregateCall aggregateCallOperation = new AggregateCall(
                "%11",
                aggregateParameter,
                BIGINT,
                new Block(
                        Optional.of("^arguments"),
                        ImmutableList.of(argumentsParameter),
                        ImmutableList.of(
                                fieldReferenceOperationArgument,
                                rowOperationArgument,
                                returnOperationArgument)),
                new Block(
                        Optional.of("^filterSelector"),
                        ImmutableList.of(filterParameter),
                        ImmutableList.of(
                                fieldReferenceOperationFilter,
                                rowOperationFilter,
                                returnOperationFilter)),
                new Block(
                        Optional.of("^maskSelector"),
                        ImmutableList.of(maskParameter),
                        ImmutableList.of(
                                constantOperationMask,
                                returnOperationMask)),
                new Block(
                        Optional.of("^orderingSelector"),
                        ImmutableList.of(orderingParameter),
                        ImmutableList.of(
                                fieldReferenceOperationOrderingA,
                                fieldReferenceOperationOrderingB,
                                rowOperationOrdering,
                                returnOperationOrdering)),
                Optional.of(new SortOrderList(ImmutableList.of(DESC_NULLS_LAST, ASC_NULLS_FIRST))),
                sumFunction,
                false,
                SINGLE);

        // collecting aggregate functions in a row
        Row aggregatesRowOperation = new Row("%28", ImmutableList.of(aggregateCallOperation.result()), ImmutableList.of(aggregateCallOperation.attributes()));
        Return aggregatesReturnOperation = new Return("%29", aggregatesRowOperation.result(), aggregatesRowOperation.attributes());

        // grouping keys
        Block.Parameter groupingKeysParameter = new Block.Parameter(
                "%30",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationGroupingKeys = new FieldReference("%31", groupingKeysParameter, 1, ImmutableMap.of());
        Row rowOperationGroupingKeys = new Row("%32", ImmutableList.of(fieldReferenceOperationGroupingKeys.result()), ImmutableList.of(fieldReferenceOperationGroupingKeys.attributes()));
        Return returnOperationGroupingKeys = new Return("%33", rowOperationGroupingKeys.result(), rowOperationGroupingKeys.attributes());

        // hash
        Block.Parameter hashParameter = new Block.Parameter(
                "%34",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationHash = new FieldReference("%35", hashParameter, 0, ImmutableMap.of());
        Row rowOperationHash = new Row("%36", ImmutableList.of(fieldReferenceOperationHash.result()), ImmutableList.of(fieldReferenceOperationHash.attributes()));
        Return returnOperationHash = new Return("%37", rowOperationHash.result(), rowOperationHash.attributes());

        Aggregation aggregationOperation = new Aggregation(
                "%9",
                VALUES_OPERATION.result(),
                new Block(
                        Optional.of("^aggregates"),
                        ImmutableList.of(aggregateParameter),
                        ImmutableList.of(
                                aggregateCallOperation,
                                aggregatesRowOperation,
                                aggregatesReturnOperation)),
                new Block(
                        Optional.of("^groupingKeysSelector"),
                        ImmutableList.of(groupingKeysParameter),
                        ImmutableList.of(
                                fieldReferenceOperationGroupingKeys,
                                rowOperationGroupingKeys,
                                returnOperationGroupingKeys)),
                new Block(
                        Optional.of("^hashSelector"),
                        ImmutableList.of(hashParameter),
                        ImmutableList.of(
                                fieldReferenceOperationHash,
                                rowOperationHash,
                                returnOperationHash)),
                1,
                ImmutableList.of(),
                OptionalInt.empty(),
                ImmutableList.of(0),
                SINGLE,
                true,
                VALUES_OPERATION.attributes());

        assertProgram(
                aggregationNode,
                ImmutableList.of(VALUES_OPERATION, aggregationOperation),
                new MultisetType(anonymousRow(BOOLEAN, BIGINT, BIGINT)),
                ImmutableMap.of(
                        new Symbol(BOOLEAN, "b"), 0,
                        new Symbol(BIGINT, "a"), 1,
                        new Symbol(BIGINT, "sum_agg"), 2));

        assertThat(aggregateCallOperation.attributes())
                .isEqualTo(ImmutableMap.builder()
                        .put(
                                new AttributeKey(TRINO, "sort_orders"),
                                new SortOrderList(ImmutableList.of(DESC_NULLS_LAST, ASC_NULLS_FIRST)))
                        .put(
                                new AttributeKey(TRINO, "resolved_function"),
                                sumFunction)
                        .put(
                                new AttributeKey(TRINO, "distinct"),
                                false)
                        .buildOrThrow());

        assertThat(aggregationOperation.attributes())
                .isEqualTo(ImmutableMap.builder()
                        .put(
                                new AttributeKey(TRINO, "grouping_sets_count"),
                                1)
                        .put(
                                new AttributeKey(TRINO, "global_grouping_sets"),
                                ImmutableList.of())
                        .put(
                                new AttributeKey(TRINO, "pre_grouped_indexes"),
                                ImmutableList.of(0))
                        .put(
                                new AttributeKey(TRINO, "aggregation_step"),
                                SINGLE)
                        .put(
                                new AttributeKey(TRINO, "input_reducing"),
                                true)
                        .buildOrThrow());
    }

    @Test
    public void testCorrelatedJoin()
    {
        CorrelatedJoinNode correlatedJoinNode = new CorrelatedJoinNode(
                new PlanNodeId("correlated_join"),
                VALUES_NODE,
                new ValuesNode(
                        new PlanNodeId("correlated_values"),
                        ImmutableList.of(new Symbol(BOOLEAN, "c")),
                        ImmutableList.of(new io.trino.sql.ir.Row(ImmutableList.of(new Reference(BOOLEAN, "b"))))), // correlated values
                ImmutableList.of(new Symbol(BOOLEAN, "b")),
                JoinType.LEFT,
                new Reference(BOOLEAN, "b"),
                new Identifier("origin subquery - whatever"));

        // correlation
        Block.Parameter correlationParameter = new Block.Parameter(
                "%10",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationCorrelation = new FieldReference("%11", correlationParameter, 1, ImmutableMap.of());
        Row rowOperationCorrelation = new Row("%12", ImmutableList.of(fieldReferenceOperationCorrelation.result()), ImmutableList.of(fieldReferenceOperationCorrelation.attributes()));
        Return returnOperationCorrelation = new Return("%13", rowOperationCorrelation.result(), rowOperationCorrelation.attributes());

        // subquery
        Block.Parameter subqueryParameter = new Block.Parameter(
                "%14",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationSubquery = new FieldReference("%16", subqueryParameter, 1, ImmutableMap.of());
        Row rowOperationSubquery = new Row("%17", ImmutableList.of(fieldReferenceOperationSubquery.result()), ImmutableList.of(fieldReferenceOperationSubquery.attributes()));
        Return returnOperationSubqueryRow = new Return("%18", rowOperationSubquery.result(), rowOperationSubquery.attributes());
        Values valuesOperationSubquery = new Values(
                "%15",
                RowType.anonymous(ImmutableList.of(BOOLEAN)),
                ImmutableList.of(new Block(
                        Optional.of("^row"),
                        ImmutableList.of(),
                        ImmutableList.of(
                                fieldReferenceOperationSubquery,
                                rowOperationSubquery,
                                returnOperationSubqueryRow))));
        Return returnOperationSubquery = new Return("%19", valuesOperationSubquery.result(), valuesOperationSubquery.attributes());

        // filter

        // input row
        Block.Parameter firstFilterParameter = new Block.Parameter(
                "%20",
                VALUES_OPERATION_ROW_TYPE);
        // subquery row
        Block.Parameter secondFilterParameter = new Block.Parameter(
                "%21",
                irType(anonymousRow(BOOLEAN)));
        FieldReference fieldReferenceOperationFilter = new FieldReference("%22", firstFilterParameter, 1, ImmutableMap.of());
        Return returnOperationFilter = new Return("%23", fieldReferenceOperationFilter.result(), fieldReferenceOperationFilter.attributes());

        CorrelatedJoin correlatedJoinOperation = new CorrelatedJoin(
                "%9",
                VALUES_OPERATION.result(),
                new Block(
                        Optional.of("^correlationSelector"),
                        ImmutableList.of(correlationParameter),
                        ImmutableList.of(
                                fieldReferenceOperationCorrelation,
                                rowOperationCorrelation,
                                returnOperationCorrelation)),
                new Block(
                        Optional.of("^subquery"),
                        ImmutableList.of(subqueryParameter),
                        ImmutableList.of(
                                valuesOperationSubquery,
                                returnOperationSubquery)),
                new Block(
                        Optional.of("^filter"),
                        ImmutableList.of(firstFilterParameter, secondFilterParameter),
                        ImmutableList.of(
                                fieldReferenceOperationFilter,
                                returnOperationFilter)),
                LEFT,
                VALUES_OPERATION.attributes(),
                returnOperationSubquery.attributes());

        assertProgram(
                correlatedJoinNode,
                ImmutableList.of(VALUES_OPERATION, correlatedJoinOperation),
                new MultisetType(anonymousRow(BIGINT, BOOLEAN, BOOLEAN)),
                ImmutableMap.of(
                        new Symbol(BIGINT, "a"), 0,
                        new Symbol(BOOLEAN, "b"), 1,
                        new Symbol(BOOLEAN, "c"), 2));
    }

    @Test
    public void testExchange()
    {
        ConnectorPartitioningHandle testingPartitioningHandle = new ConnectorPartitioningHandle() {};

        ExchangeNode exchangeNode = new ExchangeNode(
                new PlanNodeId("exchange"),
                ExchangeNode.Type.GATHER,
                ExchangeNode.Scope.REMOTE,
                new PartitioningScheme(
                        Partitioning.create(
                                new PartitioningHandle(
                                        Optional.of(CatalogHandle.fromId("bla:normal:1")),
                                        Optional.of(TestingConnectorTransactionHandle.INSTANCE),
                                        testingPartitioningHandle),
                                ImmutableList.of(new Symbol(BIGINT, "f"))),
                        ImmutableList.of(new Symbol(BIGINT, "f"), new Symbol(BOOLEAN, "g")),
                        Optional.of(new Symbol(BOOLEAN, "g")),
                        false,
                        Optional.of(new int[] {5, 6, 7}),
                        Optional.empty()),
                ImmutableList.of(
                        VALUES_NODE,
                        new ValuesNode(
                                new PlanNodeId("another values"),
                                ImmutableList.of(new Symbol(SMALLINT, "c"), new Symbol(BIGINT, "d"), new Symbol(BOOLEAN, "e")),
                                ImmutableList.of())),
                ImmutableList.of(
                        ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b")),
                        ImmutableList.of(new Symbol(BIGINT, "d"), new Symbol(BOOLEAN, "e"))),
                Optional.empty());

        // right source -- values with no rows
        Values rightSourceOperation = new Values(
                "%9",
                RowType.anonymous(ImmutableList.of(SMALLINT, BIGINT, BOOLEAN)),
                ImmutableList.of());
        Type rightRowType = irType(relationRowType(trinoType(rightSourceOperation.result().type())));

        // input field selectors for exchange
        Block.Parameter leftInputsParameter = new Block.Parameter(
                "%11",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationLeftInputsA = new FieldReference("%12", leftInputsParameter, 0, ImmutableMap.of());
        FieldReference fieldReferenceOperationLeftInputsB = new FieldReference("%13", leftInputsParameter, 1, ImmutableMap.of());
        Row rowOperationLeftInputs = new Row(
                "%14",
                ImmutableList.of(fieldReferenceOperationLeftInputsA.result(), fieldReferenceOperationLeftInputsB.result()),
                ImmutableList.of(fieldReferenceOperationLeftInputsA.attributes(), fieldReferenceOperationLeftInputsB.attributes()));
        Return returnOperationLeftInputs = new Return("%15", rowOperationLeftInputs.result(), rowOperationLeftInputs.attributes());

        Block.Parameter rightInputsParameter = new Block.Parameter(
                "%16",
                rightRowType);
        FieldReference fieldReferenceOperationRightInputsD = new FieldReference("%17", rightInputsParameter, 1, ImmutableMap.of());
        FieldReference fieldReferenceOperationRightInputsE = new FieldReference("%18", rightInputsParameter, 2, ImmutableMap.of());
        Row rowOperationRightInputs = new Row(
                "%19",
                ImmutableList.of(fieldReferenceOperationRightInputsD.result(), fieldReferenceOperationRightInputsE.result()),
                ImmutableList.of(fieldReferenceOperationRightInputsD.attributes(), fieldReferenceOperationRightInputsE.attributes()));
        Return returnOperationRightInputs = new Return("%20", rowOperationRightInputs.result(), rowOperationRightInputs.attributes());

        RowType exchangeOutputRowType = anonymousRow(BIGINT, BOOLEAN);

        // partitioning bound arguments
        Block.Parameter boundArgumentsParameter = new Block.Parameter(
                "%21",
                irType(exchangeOutputRowType));
        FieldReference fieldReferenceOperationF = new FieldReference("%22", boundArgumentsParameter, 0, ImmutableMap.of());
        Row rowOperationBoundArguments = new Row("%23", ImmutableList.of(fieldReferenceOperationF.result()), ImmutableList.of(fieldReferenceOperationF.attributes()));
        Return returnOperationBoundArguments = new Return("%24", rowOperationBoundArguments.result(), rowOperationBoundArguments.attributes());

        // partitioning hash
        Block.Parameter hashParameter = new Block.Parameter(
                "%25",
                irType(exchangeOutputRowType));
        FieldReference fieldReferenceOperationG = new FieldReference("%26", hashParameter, 1, ImmutableMap.of());
        Row rowOperationHashSymbol = new Row("%27", ImmutableList.of(fieldReferenceOperationG.result()), ImmutableList.of(fieldReferenceOperationG.attributes()));
        Return returnOperationHashSymbol = new Return("%28", rowOperationHashSymbol.result(), rowOperationHashSymbol.attributes());

        // order by
        Block.Parameter orderByParameter = new Block.Parameter(
                "%29",
                irType(exchangeOutputRowType));
        Constant constantOperationOrderBy = new Constant("%30", EMPTY_ROW, null);
        Return returnOperationOrderBy = new Return("%31", constantOperationOrderBy.result(), constantOperationOrderBy.attributes());

        Exchange exchangeOperation = new Exchange(
                "%10",
                ImmutableList.of(VALUES_OPERATION.result(), rightSourceOperation.result()),
                ImmutableList.of(
                        new Block(
                                Optional.of("^inputSelector"),
                                ImmutableList.of(leftInputsParameter),
                                ImmutableList.of(
                                        fieldReferenceOperationLeftInputsA,
                                        fieldReferenceOperationLeftInputsB,
                                        rowOperationLeftInputs,
                                        returnOperationLeftInputs)),
                        new Block(
                                Optional.of("^inputSelector"),
                                ImmutableList.of(rightInputsParameter),
                                ImmutableList.of(
                                        fieldReferenceOperationRightInputsD,
                                        fieldReferenceOperationRightInputsE,
                                        rowOperationRightInputs,
                                        returnOperationRightInputs))),
                new Block(
                        Optional.of("^boundArguments"),
                        ImmutableList.of(boundArgumentsParameter),
                        ImmutableList.of(
                                fieldReferenceOperationF,
                                rowOperationBoundArguments,
                                returnOperationBoundArguments)),
                new Block(
                        Optional.of("^hashSelector"),
                        ImmutableList.of(hashParameter),
                        ImmutableList.of(
                                fieldReferenceOperationG,
                                rowOperationHashSymbol,
                                returnOperationHashSymbol)),
                new Block(
                        Optional.of("^orderingSelector"),
                        ImmutableList.of(orderByParameter),
                        ImmutableList.of(
                                constantOperationOrderBy,
                                returnOperationOrderBy)),
                GATHER,
                REMOTE,
                new PartitioningHandle(
                        Optional.of(CatalogHandle.fromId("bla:normal:1")),
                        Optional.of(TestingConnectorTransactionHandle.INSTANCE),
                        testingPartitioningHandle),
                new NullableValues(new NullableValue[] {null}),
                false,
                Optional.of(ImmutableList.of(5, 6, 7)),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(VALUES_OPERATION.attributes(), rightSourceOperation.attributes()));

        assertProgram(
                exchangeNode,
                ImmutableList.of(VALUES_OPERATION, rightSourceOperation, exchangeOperation),
                new MultisetType(anonymousRow(BIGINT, BOOLEAN)),
                ImmutableMap.of(
                        new Symbol(BIGINT, "f"), 0,
                        new Symbol(BOOLEAN, "g"), 1));

        assertThat(exchangeOperation.attributes())
                .isEqualTo(ImmutableMap.builder()
                        .put(
                                new AttributeKey(TRINO, "exchange_type"),
                                GATHER)
                        .put(
                                new AttributeKey(TRINO, "exchange_scope"),
                                REMOTE)
                        .put(
                                new AttributeKey(TRINO, "partitioning_handle"),
                                new PartitioningHandle(
                                        Optional.of(CatalogHandle.fromId("bla:normal:1")),
                                        Optional.of(TestingConnectorTransactionHandle.INSTANCE),
                                        testingPartitioningHandle))
                        .put(
                                new AttributeKey(TRINO, "nullable_values"),
                                new NullableValues(new NullableValue[] {null}))
                        .put(
                                new AttributeKey(TRINO, "replicate_nulls_and_any"),
                                false)
                        .put(
                                new AttributeKey(TRINO, "bucket_to_partition"),
                                ImmutableList.of(5, 6, 7))
                        .buildOrThrow());
    }

    @Test
    public void testMergingExchange()
    {
        ExchangeNode exchangeNode = new ExchangeNode(
                new PlanNodeId("exchange"),
                ExchangeNode.Type.GATHER,
                ExchangeNode.Scope.REMOTE,
                new PartitioningScheme(
                        Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of(new Symbol(BIGINT, "f"))),
                        ImmutableList.of(new Symbol(BIGINT, "f"), new Symbol(BOOLEAN, "g")),
                        Optional.of(new Symbol(BOOLEAN, "g")),
                        false,
                        Optional.of(new int[] {5, 6, 7}),
                        Optional.of(10)),
                ImmutableList.of(VALUES_NODE),
                ImmutableList.of(ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b"))),
                Optional.of(new OrderingScheme(
                        ImmutableList.of(new Symbol(BIGINT, "f")),
                        ImmutableMap.of(new Symbol(BIGINT, "f"), DESC_NULLS_FIRST))));

        // input field selector for exchange
        Block.Parameter inputsParameter = new Block.Parameter(
                "%10",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationInputsA = new FieldReference("%11", inputsParameter, 0, ImmutableMap.of());
        FieldReference fieldReferenceOperationInputsB = new FieldReference("%12", inputsParameter, 1, ImmutableMap.of());
        Row rowOperationInputs = new Row(
                "%13",
                ImmutableList.of(fieldReferenceOperationInputsA.result(), fieldReferenceOperationInputsB.result()),
                ImmutableList.of(fieldReferenceOperationInputsA.attributes(), fieldReferenceOperationInputsB.attributes()));
        Return returnOperationInputs = new Return("%14", rowOperationInputs.result(), rowOperationInputs.attributes());

        RowType exchangeOutputRowType = anonymousRow(BIGINT, BOOLEAN);

        // partitioning bound arguments
        Block.Parameter boundArgumentsParameter = new Block.Parameter(
                "%15",
                irType(exchangeOutputRowType));
        FieldReference fieldReferenceOperationF = new FieldReference("%16", boundArgumentsParameter, 0, ImmutableMap.of());
        Row rowOperationBoundArguments = new Row("%17", ImmutableList.of(fieldReferenceOperationF.result()), ImmutableList.of(fieldReferenceOperationF.attributes()));
        Return returnOperationBoundArguments = new Return("%18", rowOperationBoundArguments.result(), rowOperationBoundArguments.attributes());

        // partitioning hash
        Block.Parameter hashParameter = new Block.Parameter(
                "%19",
                irType(exchangeOutputRowType));
        FieldReference fieldReferenceOperationG = new FieldReference("%20", hashParameter, 1, ImmutableMap.of());
        Row rowOperationHashSymbol = new Row("%21", ImmutableList.of(fieldReferenceOperationG.result()), ImmutableList.of(fieldReferenceOperationG.attributes()));
        Return returnOperationHashSymbol = new Return("%22", rowOperationHashSymbol.result(), rowOperationHashSymbol.attributes());

        // order by
        Block.Parameter orderByParameter = new Block.Parameter(
                "%23",
                irType(exchangeOutputRowType));
        FieldReference fieldReferenceOperationOrderBy = new FieldReference("%24", orderByParameter, 0, ImmutableMap.of());
        Row rowOperationOrderBy = new Row("%25", ImmutableList.of(fieldReferenceOperationOrderBy.result()), ImmutableList.of(fieldReferenceOperationOrderBy.attributes()));
        Return returnOperationOrderBy = new Return("%26", rowOperationOrderBy.result(), rowOperationOrderBy.attributes());

        Exchange exchangeOperation = new Exchange(
                "%9",
                ImmutableList.of(VALUES_OPERATION.result()),
                ImmutableList.of(
                        new Block(
                                Optional.of("^inputSelector"),
                                ImmutableList.of(inputsParameter),
                                ImmutableList.of(
                                        fieldReferenceOperationInputsA,
                                        fieldReferenceOperationInputsB,
                                        rowOperationInputs,
                                        returnOperationInputs))),
                new Block(
                        Optional.of("^boundArguments"),
                        ImmutableList.of(boundArgumentsParameter),
                        ImmutableList.of(
                                fieldReferenceOperationF,
                                rowOperationBoundArguments,
                                returnOperationBoundArguments)),
                new Block(
                        Optional.of("^hashSelector"),
                        ImmutableList.of(hashParameter),
                        ImmutableList.of(
                                fieldReferenceOperationG,
                                rowOperationHashSymbol,
                                returnOperationHashSymbol)),
                new Block(
                        Optional.of("^orderingSelector"),
                        ImmutableList.of(orderByParameter),
                        ImmutableList.of(
                                fieldReferenceOperationOrderBy,
                                rowOperationOrderBy,
                                returnOperationOrderBy)),
                GATHER,
                REMOTE,
                SINGLE_DISTRIBUTION,
                new NullableValues(new NullableValue[] {null}),
                false,
                Optional.of(ImmutableList.of(5, 6, 7)),
                Optional.of(10),
                Optional.of(new SortOrderList(ImmutableList.of(DESC_NULLS_FIRST))),
                ImmutableList.of(VALUES_OPERATION.attributes()));

        assertProgram(
                exchangeNode,
                ImmutableList.of(VALUES_OPERATION, exchangeOperation),
                new MultisetType(anonymousRow(BIGINT, BOOLEAN)),
                ImmutableMap.of(
                        new Symbol(BIGINT, "f"), 0,
                        new Symbol(BOOLEAN, "g"), 1));

        assertThat(exchangeOperation.attributes())
                .isEqualTo(ImmutableMap.builder()
                        .put(
                                new AttributeKey(TRINO, "exchange_type"),
                                GATHER)
                        .put(
                                new AttributeKey(TRINO, "exchange_scope"),
                                REMOTE)
                        .put(
                                new AttributeKey(TRINO, "partitioning_handle"),
                                SINGLE_DISTRIBUTION)
                        .put(
                                new AttributeKey(TRINO, "nullable_values"),
                                new NullableValues(new NullableValue[] {null}))
                        .put(
                                new AttributeKey(TRINO, "replicate_nulls_and_any"),
                                false)
                        .put(
                                new AttributeKey(TRINO, "bucket_to_partition"),
                                ImmutableList.of(5, 6, 7))
                        .put(
                                new AttributeKey(TRINO, "partition_count"),
                                10)
                        .put(
                                new AttributeKey(TRINO, "sort_orders"),
                                new SortOrderList(ImmutableList.of(DESC_NULLS_FIRST)))
                        .buildOrThrow());
    }

    @Test
    public void testFilter()
    {
        FilterNode filterNode = new FilterNode(
                new PlanNodeId("filter"),
                VALUES_NODE,
                new io.trino.sql.ir.Comparison(
                        io.trino.sql.ir.Comparison.Operator.GREATER_THAN,
                        new Reference(BIGINT, "a"),
                        new io.trino.sql.ir.Constant(BIGINT, 5L)));

        Block.Parameter predicateParameter = new Block.Parameter(
                "%10",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperation = new FieldReference("%11", predicateParameter, 0, ImmutableMap.of());
        Constant constantOperation = new Constant("%12", BIGINT, 5L);
        Comparison comparisonOperation = new Comparison(
                "%13",
                fieldReferenceOperation.result(),
                constantOperation.result(),
                GREATER_THAN,
                ImmutableList.of(fieldReferenceOperation.attributes(), constantOperation.attributes()));
        Return returnOperation = new Return("%14", comparisonOperation.result(), comparisonOperation.attributes());
        Filter filterOperation = new Filter(
                "%9",
                VALUES_OPERATION.result(),
                new Block(
                        Optional.of("^predicate"),
                        ImmutableList.of(predicateParameter),
                        ImmutableList.of(
                                fieldReferenceOperation,
                                constantOperation,
                                comparisonOperation,
                                returnOperation)),
                VALUES_OPERATION.attributes());

        assertProgram(
                filterNode,
                ImmutableList.of(VALUES_OPERATION, filterOperation),
                new MultisetType(anonymousRow(BIGINT, BOOLEAN)),
                ImmutableMap.of(
                        new Symbol(BIGINT, "a"), 0,
                        new Symbol(BOOLEAN, "b"), 1));
    }

    @Test
    public void testJoin()
    {
        ValuesNode left = VALUES_NODE;
        ValuesNode right = new ValuesNode(
                new PlanNodeId("values_right"),
                ImmutableList.of(new Symbol(BIGINT, "c")),
                ImmutableList.of(new io.trino.sql.ir.Row(ImmutableList.of(new io.trino.sql.ir.Constant(BIGINT, 5L)))));

        PlanNodeStatsAndCostSummary statsAndCost = new PlanNodeStatsAndCostSummary(1, 2, 3, 4, 5);

        JoinNode joinNode = new JoinNode(
                new PlanNodeId("join"),
                JoinType.LEFT,
                left,
                right,
                ImmutableList.of(new JoinNode.EquiJoinClause(new Symbol(BIGINT, "a"), new Symbol(BIGINT, "c"))),
                ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b")),
                ImmutableList.of(new Symbol(BIGINT, "c")),
                false,
                Optional.empty(),
                Optional.of(new Symbol(BOOLEAN, "b")),
                Optional.empty(),
                Optional.of(JoinNode.DistributionType.REPLICATED),
                Optional.of(true),
                ImmutableMap.of(
                        new DynamicFilterId("first_dynamic_filter"), new Symbol(BIGINT, "c"),
                        new DynamicFilterId("second_dynamic_filter"), new Symbol(BIGINT, "c")),
                Optional.of(statsAndCost));

        // right source
        Constant constantOperation = new Constant("%10", BIGINT, 5L);
        Row rowOperation = new Row("%11", ImmutableList.of(constantOperation.result()), ImmutableList.of(constantOperation.attributes()));
        Return returnOperation = new Return("%12", rowOperation.result(), rowOperation.attributes());
        Values rightSourceOperation = new Values(
                "%9",
                RowType.anonymous(ImmutableList.of(BIGINT)),
                ImmutableList.of(
                        new Block(
                                Optional.of("^row"),
                                ImmutableList.of(),
                                ImmutableList.of(
                                        constantOperation,
                                        rowOperation,
                                        returnOperation))));
        Type rightRowType = irType(relationRowType(trinoType(rightSourceOperation.result().type())));

        // left join criteria
        Block.Parameter leftCriteriaParameter = new Block.Parameter(
                "%14",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationLeftCriteria = new FieldReference("%15", leftCriteriaParameter, 0, ImmutableMap.of());
        Row rowOperationLeftCriteria = new Row("%16", ImmutableList.of(fieldReferenceOperationLeftCriteria.result()), ImmutableList.of(fieldReferenceOperationLeftCriteria.attributes()));
        Return returnOperationLeftCriteria = new Return("%17", rowOperationLeftCriteria.result(), rowOperationLeftCriteria.attributes());

        // right join criteria
        Block.Parameter rightCriteriaParameter = new Block.Parameter(
                "%18",
                rightRowType);
        FieldReference fieldReferenceOperationRightCriteria = new FieldReference("%19", rightCriteriaParameter, 0, ImmutableMap.of());
        Row rowOperationRightCriteria = new Row("%20", ImmutableList.of(fieldReferenceOperationRightCriteria.result()), ImmutableList.of(fieldReferenceOperationRightCriteria.attributes()));
        Return returnOperationRightCriteria = new Return("%21", rowOperationRightCriteria.result(), rowOperationRightCriteria.attributes());

        // join filter
        Block.Parameter leftFilterParameter = new Block.Parameter(
                "%22",
                VALUES_OPERATION_ROW_TYPE);
        Block.Parameter rightFilterParameter = new Block.Parameter(
                "%23",
                rightRowType);
        // JoinNode has empty filter, so default filter is created: constant true
        Constant constantOperationFilter = new Constant("%24", BOOLEAN, true);
        Return returnOperationFilter = new Return("%25", constantOperationFilter.result(), constantOperationFilter.attributes());

        // left hash symbol
        Block.Parameter leftHashSymbolParameter = new Block.Parameter(
                "%26",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationLeftHashSymbol = new FieldReference("%27", leftHashSymbolParameter, 1, ImmutableMap.of());
        Row rowOperationLeftHashSymbol = new Row("%28", ImmutableList.of(fieldReferenceOperationLeftHashSymbol.result()), ImmutableList.of(fieldReferenceOperationLeftHashSymbol.attributes()));
        Return returnOperationLeftHashSymbol = new Return("%29", rowOperationLeftHashSymbol.result(), rowOperationLeftHashSymbol.attributes());

        // right hash symbol
        Block.Parameter rightHashSymbolParameter = new Block.Parameter(
                "%30",
                rightRowType);
        Constant constantOperationRightHashSymbol = new Constant("%31", EMPTY_ROW, null);
        Return returnOperationRightHashSymbol = new Return("%32", constantOperationRightHashSymbol.result(), constantOperationRightHashSymbol.attributes());

        // left outputs
        Block.Parameter leftOutputsParameter = new Block.Parameter(
                "%33",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationLeftOutputsA = new FieldReference("%34", leftOutputsParameter, 0, ImmutableMap.of());
        FieldReference fieldReferenceOperationLeftOutputsB = new FieldReference("%35", leftOutputsParameter, 1, ImmutableMap.of());
        Row rowOperationLeftOutputs = new Row(
                "%36",
                ImmutableList.of(fieldReferenceOperationLeftOutputsA.result(), fieldReferenceOperationLeftOutputsB.result()),
                ImmutableList.of(fieldReferenceOperationLeftOutputsA.attributes(), fieldReferenceOperationLeftOutputsB.attributes()));
        Return returnOperationLeftOutputs = new Return("%37", rowOperationLeftOutputs.result(), rowOperationLeftOutputs.attributes());

        // right outputs
        Block.Parameter rightOutputsParameter = new Block.Parameter(
                "%38",
                rightRowType);
        FieldReference fieldReferenceOperationRightOutputsC = new FieldReference("%39", rightOutputsParameter, 0, ImmutableMap.of());
        Row rowOperationRightOutputs = new Row("%40", ImmutableList.of(fieldReferenceOperationRightOutputsC.result()), ImmutableList.of(fieldReferenceOperationRightOutputsC.attributes()));
        Return returnOperationRightOutputs = new Return("%41", rowOperationRightOutputs.result(), rowOperationRightOutputs.attributes());

        // dynamic filter targets
        Block.Parameter dynamicFilterTargetsParameter = new Block.Parameter(
                "%42",
                rightRowType);
        FieldReference fieldReferenceOperationDynamicFilterTargetsC1 = new FieldReference("%43", dynamicFilterTargetsParameter, 0, ImmutableMap.of());
        FieldReference fieldReferenceOperationDynamicFilterTargetsC2 = new FieldReference("%44", dynamicFilterTargetsParameter, 0, ImmutableMap.of());
        Row rowOperationDynamicFilterTargets = new Row(
                "%45",
                ImmutableList.of(fieldReferenceOperationDynamicFilterTargetsC1.result(), fieldReferenceOperationDynamicFilterTargetsC2.result()),
                ImmutableList.of(fieldReferenceOperationDynamicFilterTargetsC1.attributes(), fieldReferenceOperationDynamicFilterTargetsC2.attributes()));
        Return returnOperationDynamicFilterTargets = new Return("%46", rowOperationDynamicFilterTargets.result(), rowOperationDynamicFilterTargets.attributes());

        Join joinOperation = new Join(
                "%13",
                VALUES_OPERATION.result(),
                rightSourceOperation.result(),
                new Block(
                        Optional.of("^leftCriteriaSelector"),
                        ImmutableList.of(leftCriteriaParameter),
                        ImmutableList.of(
                                fieldReferenceOperationLeftCriteria,
                                rowOperationLeftCriteria,
                                returnOperationLeftCriteria)),
                new Block(
                        Optional.of("^rightCriteriaSelector"),
                        ImmutableList.of(rightCriteriaParameter),
                        ImmutableList.of(
                                fieldReferenceOperationRightCriteria,
                                rowOperationRightCriteria,
                                returnOperationRightCriteria)),
                new Block(
                        Optional.of("^filter"),
                        ImmutableList.of(leftFilterParameter, rightFilterParameter),
                        ImmutableList.of(
                                constantOperationFilter,
                                returnOperationFilter)),
                new Block(
                        Optional.of("^leftHashSelector"),
                        ImmutableList.of(leftHashSymbolParameter),
                        ImmutableList.of(
                                fieldReferenceOperationLeftHashSymbol,
                                rowOperationLeftHashSymbol,
                                returnOperationLeftHashSymbol)),
                new Block(
                        Optional.of("^rightHashSelector"),
                        ImmutableList.of(rightHashSymbolParameter),
                        ImmutableList.of(
                                constantOperationRightHashSymbol,
                                returnOperationRightHashSymbol)),
                new Block(
                        Optional.of("^leftOutputSelector"),
                        ImmutableList.of(leftOutputsParameter),
                        ImmutableList.of(
                                fieldReferenceOperationLeftOutputsA,
                                fieldReferenceOperationLeftOutputsB,
                                rowOperationLeftOutputs,
                                returnOperationLeftOutputs)),
                new Block(
                        Optional.of("^rightOutputSelector"),
                        ImmutableList.of(rightOutputsParameter),
                        ImmutableList.of(
                                fieldReferenceOperationRightOutputsC,
                                rowOperationRightOutputs,
                                returnOperationRightOutputs)),
                new Block(
                        Optional.of("^dynamicFilterTargetSelector"),
                        ImmutableList.of(dynamicFilterTargetsParameter),
                        ImmutableList.of(
                                fieldReferenceOperationDynamicFilterTargetsC1,
                                fieldReferenceOperationDynamicFilterTargetsC2,
                                rowOperationDynamicFilterTargets,
                                returnOperationDynamicFilterTargets)),
                LEFT,
                false,
                Optional.of(REPLICATED),
                Optional.of(true),
                ImmutableList.of("first_dynamic_filter", "second_dynamic_filter"),
                Optional.of(statsAndCost),
                VALUES_OPERATION.attributes(),
                rightSourceOperation.attributes());

        assertProgram(
                joinNode,
                ImmutableList.of(VALUES_OPERATION, rightSourceOperation, joinOperation),
                new MultisetType(anonymousRow(BIGINT, BOOLEAN, BIGINT)),
                ImmutableMap.of(
                        new Symbol(BIGINT, "a"), 0,
                        new Symbol(BOOLEAN, "b"), 1,
                        new Symbol(BIGINT, "c"), 2));

        assertThat(joinOperation.attributes())
                .isEqualTo(ImmutableMap.builder()
                        .put(
                                new AttributeKey(TRINO, "join_type"),
                                LEFT)
                        .put(
                                new AttributeKey(TRINO, "may_skip_output_duplicates"),
                                false)
                        .put(
                                new AttributeKey(TRINO, "distribution_type"),
                                REPLICATED)
                        .put(
                                new AttributeKey(TRINO, "spillable"),
                                true)
                        .put(
                                new AttributeKey(TRINO, "dynamic_filter_ids"),
                                ImmutableList.of("first_dynamic_filter", "second_dynamic_filter"))
                        .put(
                                new AttributeKey(TRINO, "statistics_and_cost_summary"),
                                statsAndCost)
                        .buildOrThrow());
    }

    @Test
    public void testLimit()
    {
        LimitNode limitNode = new LimitNode(new PlanNodeId("limit"), VALUES_NODE, 5L, true);

        Block.Parameter orderingParameter = new Block.Parameter(
                "%10",
                VALUES_OPERATION_ROW_TYPE);
        Constant constantNull = new Constant("%11", EMPTY_ROW, null);
        Return returnOperation = new Return("%12", constantNull.result(), constantNull.attributes());

        Limit limitOperation = new Limit(
                "%9",
                VALUES_OPERATION.result(),
                new Block(
                        Optional.of("^orderingSelector"),
                        ImmutableList.of(orderingParameter),
                        ImmutableList.of(
                                constantNull,
                                returnOperation)),
                Optional.empty(),
                5L,
                true,
                ImmutableList.of(),
                VALUES_OPERATION.attributes());

        assertProgram(
                limitNode,
                ImmutableList.of(VALUES_OPERATION, limitOperation),
                new MultisetType(anonymousRow(BIGINT, BOOLEAN)),
                ImmutableMap.of(
                        new Symbol(BIGINT, "a"), 0,
                        new Symbol(BOOLEAN, "b"), 1));

        assertThat(limitOperation.attributes())
                .isEqualTo(ImmutableMap.builder()
                        .put(
                                new AttributeKey(TRINO, "limit"),
                                5L)
                        .put(
                                new AttributeKey(TRINO, "partial"),
                                true)
                        .put(
                                new AttributeKey(TRINO, "pre_sorted_indexes"),
                                ImmutableList.of())
                        .buildOrThrow());
    }

    @Test
    public void testLimitWithTies()
    {
        LimitNode limitNode = new LimitNode(
                new PlanNodeId("limit"),
                VALUES_NODE,
                5L,
                Optional.of(new OrderingScheme(ImmutableList.of(new Symbol(BOOLEAN, "b")), ImmutableMap.of(new Symbol(BOOLEAN, "b"), ASC_NULLS_FIRST))),
                false,
                ImmutableList.of(new Symbol(BOOLEAN, "b")));

        Block.Parameter orderingParameter = new Block.Parameter(
                "%10",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationOrderingB = new FieldReference("%11", orderingParameter, 1, ImmutableMap.of());
        Row rowOperationOrdering = new Row("%12", ImmutableList.of(fieldReferenceOperationOrderingB.result()), ImmutableList.of(fieldReferenceOperationOrderingB.attributes()));
        Return returnOperationOrdering = new Return("%13", rowOperationOrdering.result(), rowOperationOrdering.attributes());

        Limit limitOperation = new Limit(
                "%9",
                VALUES_OPERATION.result(),
                new Block(
                        Optional.of("^orderingSelector"),
                        ImmutableList.of(orderingParameter),
                        ImmutableList.of(
                                fieldReferenceOperationOrderingB,
                                rowOperationOrdering,
                                returnOperationOrdering)),
                Optional.of(new SortOrderList(ImmutableList.of(ASC_NULLS_FIRST))),
                5L,
                false,
                ImmutableList.of(0),
                VALUES_OPERATION.attributes());

        assertProgram(
                limitNode,
                ImmutableList.of(VALUES_OPERATION, limitOperation),
                new MultisetType(anonymousRow(BIGINT, BOOLEAN)),
                ImmutableMap.of(
                        new Symbol(BIGINT, "a"), 0,
                        new Symbol(BOOLEAN, "b"), 1));

        assertThat(limitOperation.attributes())
                .isEqualTo(ImmutableMap.builder()
                        .put(
                                new AttributeKey(TRINO, "sort_orders"),
                                new SortOrderList(ImmutableList.of(ASC_NULLS_FIRST)))
                        .put(
                                new AttributeKey(TRINO, "limit"),
                                5L)
                        .put(
                                new AttributeKey(TRINO, "partial"),
                                false)
                        .put(
                                new AttributeKey(TRINO, "pre_sorted_indexes"),
                                ImmutableList.of(0))
                        .buildOrThrow());
    }

    @Test
    public void testOutput()
    {
        OutputNode outputNode = new OutputNode(
                new PlanNodeId("output"),
                VALUES_NODE,
                ImmutableList.of("col_b", "col_a"),
                ImmutableList.of(new Symbol(BOOLEAN, "b"), new Symbol(BIGINT, "a")));

        Block.Parameter fieldReferenceParameter = new Block.Parameter(
                "%10",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationB = new FieldReference("%11", fieldReferenceParameter, 1, ImmutableMap.of());
        FieldReference fieldReferenceOperationA = new FieldReference("%12", fieldReferenceParameter, 0, ImmutableMap.of());
        Row rowOperation = new Row(
                "%13",
                ImmutableList.of(fieldReferenceOperationB.result(), fieldReferenceOperationA.result()),
                ImmutableList.of(fieldReferenceOperationB.attributes(), fieldReferenceOperationA.attributes()));
        Return returnOperation = new Return("%14", rowOperation.result(), rowOperation.attributes());

        Output outputOperation = new Output(
                "%9",
                VALUES_OPERATION.result(),
                new Block(
                        Optional.of("^outputFieldSelector"),
                        ImmutableList.of(fieldReferenceParameter),
                        ImmutableList.of(
                                fieldReferenceOperationB,
                                fieldReferenceOperationA,
                                rowOperation,
                                returnOperation)),
                ImmutableList.of("col_b", "col_a"));

        RelationalProgramBuilder relationalProgramBuilder = new RelationalProgramBuilder(new ValueNameAllocator(), ImmutableMap.builder());
        Block.Builder blockBuilder = new Block.Builder(Optional.empty(), ImmutableList.of());

        outputNode.accept(relationalProgramBuilder, new Context(blockBuilder));
        Block block = blockBuilder.build();

        assertThat(block.operations()).isEqualTo(ImmutableList.of(VALUES_OPERATION, outputOperation));

        assertThat(trinoType(block.getReturnedType())).isEqualTo(BOOLEAN);
    }

    @Test
    public void testOutputWithSpoolingMetadata()
    {
        OutputNode outputNode = new OutputNode(
                new PlanNodeId("output"),
                VALUES_NODE,
                ImmutableList.of("col_b", "col_a", SPOOLING_METADATA_SYMBOL.name()),
                ImmutableList.of(new Symbol(BOOLEAN, "b"), new Symbol(BIGINT, "a"), SPOOLING_METADATA_SYMBOL));

        Block.Parameter fieldReferenceParameter = new Block.Parameter(
                "%10",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationB = new FieldReference("%11", fieldReferenceParameter, 1, ImmutableMap.of());
        FieldReference fieldReferenceOperationA = new FieldReference("%12", fieldReferenceParameter, 0, ImmutableMap.of());
        Row rowOperation = new Row(
                "%13",
                ImmutableList.of(fieldReferenceOperationB.result(), fieldReferenceOperationA.result()),
                ImmutableList.of(fieldReferenceOperationB.attributes(), fieldReferenceOperationA.attributes()));
        Return returnOperation = new Return("%14", rowOperation.result(), rowOperation.attributes());

        Output outputOperation = new Output(
                "%9",
                VALUES_OPERATION.result(),
                new Block(
                        Optional.of("^outputFieldSelector"),
                        ImmutableList.of(fieldReferenceParameter),
                        ImmutableList.of(
                                fieldReferenceOperationB,
                                fieldReferenceOperationA,
                                rowOperation,
                                returnOperation)),
                ImmutableList.of("col_b", "col_a", SPOOLING_METADATA_SYMBOL.name()));

        RelationalProgramBuilder relationalProgramBuilder = new RelationalProgramBuilder(new ValueNameAllocator(), ImmutableMap.builder());
        Block.Builder blockBuilder = new Block.Builder(Optional.empty(), ImmutableList.of());

        outputNode.accept(relationalProgramBuilder, new Context(blockBuilder));
        Block block = blockBuilder.build();

        assertThat(block.operations()).isEqualTo(ImmutableList.of(VALUES_OPERATION, outputOperation));

        assertThat(trinoType(block.getReturnedType())).isEqualTo(BOOLEAN);
    }

    @Test
    public void testProject()
    {
        ProjectNode projectNode = new ProjectNode(
                new PlanNodeId("project"),
                VALUES_NODE,
                Assignments.copyOf(ImmutableMap.of(
                        new Symbol(BOOLEAN, "b"),
                        new Reference(BOOLEAN, "b"),
                        new Symbol(BOOLEAN, "c"),
                        new io.trino.sql.ir.Comparison(
                                io.trino.sql.ir.Comparison.Operator.GREATER_THAN,
                                new Reference(BIGINT, "a"),
                                new io.trino.sql.ir.Constant(BIGINT, 5L)))));

        Block.Parameter assignmentsParameter = new Block.Parameter(
                "%10",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationB = new FieldReference("%11", assignmentsParameter, 1, ImmutableMap.of());
        FieldReference fieldReferenceOperationA = new FieldReference("%12", assignmentsParameter, 0, ImmutableMap.of());
        Constant constantOperation = new Constant("%13", BIGINT, 5L);
        Comparison comparisonOperation = new Comparison(
                "%14",
                fieldReferenceOperationA.result(),
                constantOperation.result(),
                GREATER_THAN,
                ImmutableList.of(fieldReferenceOperationA.attributes(), constantOperation.attributes()));
        Row rowOperation = new Row(
                "%15",
                ImmutableList.of(fieldReferenceOperationB.result(), comparisonOperation.result()),
                ImmutableList.of(fieldReferenceOperationB.attributes(), comparisonOperation.attributes()));
        Return returnOperation = new Return("%16", rowOperation.result(), rowOperation.attributes());
        Project projectOperation = new Project(
                "%9",
                VALUES_OPERATION.result(),
                new Block(
                        Optional.of("^assignments"),
                        ImmutableList.of(assignmentsParameter),
                        ImmutableList.of(
                                fieldReferenceOperationB,
                                fieldReferenceOperationA,
                                constantOperation,
                                comparisonOperation,
                                rowOperation,
                                returnOperation)),
                VALUES_OPERATION.attributes());

        assertProgram(
                projectNode,
                ImmutableList.of(VALUES_OPERATION, projectOperation),
                new MultisetType(anonymousRow(BOOLEAN, BOOLEAN)),
                ImmutableMap.of(
                        new Symbol(BOOLEAN, "b"), 0,
                        new Symbol(BOOLEAN, "c"), 1));
    }

    @Test
    public void testEmptyProject()
    {
        ValuesNode valuesNode = new ValuesNode(new PlanNodeId("values_empty_row"), 5);

        ProjectNode projectNode = new ProjectNode(
                new PlanNodeId("project"),
                valuesNode,
                Assignments.of());

        Values valuesOperation = valuesWithoutFields("%0", 5);

        Block.Parameter assignmentsParameter = new Block.Parameter("%2", irType(EMPTY_ROW));
        Constant constantOperation = new Constant("%3", EMPTY_ROW, null);
        Return returnOperation = new Return("%4", constantOperation.result(), constantOperation.attributes());

        Project projectOperation = new Project(
                "%1",
                valuesOperation.result(),
                new Block(
                        Optional.of("^assignments"),
                        ImmutableList.of(assignmentsParameter),
                        ImmutableList.of(
                                constantOperation,
                                returnOperation)),
                valuesOperation.attributes());

        assertProgram(
                projectNode,
                ImmutableList.of(valuesOperation, projectOperation),
                new MultisetType(EMPTY_ROW),
                ImmutableMap.of());
    }

    @Test
    public void testTableScan()
    {
        ConnectorTableHandle testingConnectorTableHandle = new ConnectorTableHandle() {};

        TableScanNode tableScanNode = new TableScanNode(
                new PlanNodeId("table_scan"),
                new TableHandle(CatalogHandle.fromId("bla:normal:1"), testingConnectorTableHandle, TestingConnectorTransactionHandle.INSTANCE),
                ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b")),
                ImmutableMap.of(
                        new Symbol(BIGINT, "a"), new TestingColumnHandle("a_handle"),
                        new Symbol(BOOLEAN, "b"), new TestingColumnHandle("b_handle")),
                TupleDomain.withColumnDomains(ImmutableMap.of(new TestingColumnHandle("b_handle"), Domain.singleValue(BOOLEAN, true))),
                Optional.of(PlanNodeStatsEstimate.unknown()),
                false,
                Optional.of(TRUE));

        TableScan tableScanOperation = new TableScan(
                "%0",
                RowType.anonymous(ImmutableList.of(BIGINT, BOOLEAN)),
                new TableHandle(CatalogHandle.fromId("bla:normal:1"), testingConnectorTableHandle, TestingConnectorTransactionHandle.INSTANCE),
                ImmutableList.of(new TestingColumnHandle("a_handle"), new TestingColumnHandle("b_handle")),
                TupleDomain.withColumnDomains(ImmutableMap.of(new TestingColumnHandle("b_handle"), Domain.singleValue(BOOLEAN, true))),
                Optional.of(mapStatistics(PlanNodeStatsEstimate.unknown(), ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b")))),
                false,
                Optional.of(TRUE));

        assertProgram(
                tableScanNode,
                ImmutableList.of(tableScanOperation),
                new MultisetType(anonymousRow(BIGINT, BOOLEAN)),
                ImmutableMap.of(
                        new Symbol(BIGINT, "a"), 0,
                        new Symbol(BOOLEAN, "b"), 1));

        assertThat(tableScanOperation.attributes())
                .isEqualTo(ImmutableMap.builder()
                        .put(
                                new AttributeKey(TRINO, "table_handle"),
                                new TableHandle(CatalogHandle.fromId("bla:normal:1"), testingConnectorTableHandle, TestingConnectorTransactionHandle.INSTANCE))
                        .put(
                                new AttributeKey(TRINO, "column_handles"),
                                ImmutableList.of(new TestingColumnHandle("a_handle"), new TestingColumnHandle("b_handle")))
                        .put(
                                new AttributeKey(TRINO, "constraint"),
                                TupleDomain.withColumnDomains(ImmutableMap.of(new TestingColumnHandle("b_handle"), Domain.singleValue(BOOLEAN, true))))
                        .put(
                                new AttributeKey(TRINO, "statistics"),
                                new Statistics(NaN, ImmutableMap.of()))
                        .put(
                                new AttributeKey(TRINO, "update_target"),
                                false)
                        .put(
                                new AttributeKey(TRINO, "use_connector_node_partitioning"),
                                true)
                        .buildOrThrow());
    }

    @Test
    public void testTopN()
    {
        TopNNode topNNode = new TopNNode(
                new PlanNodeId("topN"),
                VALUES_NODE,
                10,
                new OrderingScheme(ImmutableList.of(new Symbol(BOOLEAN, "b")), ImmutableMap.of(new Symbol(BOOLEAN, "b"), ASC_NULLS_FIRST)),
                TopNNode.Step.FINAL);

        Block.Parameter orderingParameter = new Block.Parameter(
                "%10",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationOrderingB = new FieldReference("%11", orderingParameter, 1, ImmutableMap.of());
        Row rowOperationOrdering = new Row("%12", ImmutableList.of(fieldReferenceOperationOrderingB.result()), ImmutableList.of(fieldReferenceOperationOrderingB.attributes()));
        Return returnOperationOrdering = new Return("%13", rowOperationOrdering.result(), rowOperationOrdering.attributes());

        TopN topNOperation = new TopN(
                "%9",
                VALUES_OPERATION.result(),
                new Block(
                        Optional.of("^orderingSelector"),
                        ImmutableList.of(orderingParameter),
                        ImmutableList.of(
                                fieldReferenceOperationOrderingB,
                                rowOperationOrdering,
                                returnOperationOrdering)),
                new SortOrderList(ImmutableList.of(ASC_NULLS_FIRST)),
                10,
                FINAL,
                VALUES_OPERATION.attributes());

        assertProgram(
                topNNode,
                ImmutableList.of(VALUES_OPERATION, topNOperation),
                new MultisetType(anonymousRow(BIGINT, BOOLEAN)),
                ImmutableMap.of(
                        new Symbol(BIGINT, "a"), 0,
                        new Symbol(BOOLEAN, "b"), 1));
    }

    @Test
    public void testValues()
    {
        assertProgram(
                VALUES_NODE,
                ImmutableList.of(VALUES_OPERATION),
                new MultisetType(anonymousRow(BIGINT, BOOLEAN)),
                ImmutableMap.of(
                        new Symbol(BIGINT, "a"), 0,
                        new Symbol(BOOLEAN, "b"), 1));
    }

    @Test
    public void testValuesEmptyRow()
    {
        ValuesNode valuesNode = new ValuesNode(new PlanNodeId("values_empty_row"), 5);

        Values valuesOperation = valuesWithoutFields("%0", 5);

        RelationalProgramBuilder relationalProgramBuilder = new RelationalProgramBuilder(new ValueNameAllocator(), ImmutableMap.builder());
        Block.Builder blockBuilder = new Block.Builder(Optional.empty(), ImmutableList.of());

        OperationAndMapping operationAndMapping = valuesNode.accept(relationalProgramBuilder, new Context(blockBuilder));
        relationalProgramBuilder.addReturnOperation(blockBuilder);
        Block block = blockBuilder.build();

        // remove the Return operation for comparison
        assertThat(block.operations().subList(0, block.operations().size() - 1)).isEqualTo(ImmutableList.of(valuesOperation));

        // assert relation type
        assertThat(trinoType(block.getReturnedType())).isEqualTo(new MultisetType(EMPTY_ROW));

        // assert symbol mapping
        assertThat(operationAndMapping.mapping()).isEqualTo(ImmutableMap.of());
    }

    @Test
    public void testRelationRowType()
    {
        assertThat(relationRowType(new MultisetType(EMPTY_ROW))).isEqualTo(EMPTY_ROW);

        RowType rowType = anonymousRow(BIGINT, BOOLEAN);
        assertThat(relationRowType(new MultisetType(rowType))).isEqualTo(rowType);

        assertThatThrownBy(() -> relationRowType(new MultisetType(rowType(
                new RowType.Field(Optional.of("a"), BIGINT),
                new RowType.Field(Optional.of("b"), BOOLEAN)))))
                .hasMessage("not a relation type. expected multiset of row with anonymous fields");

        assertThatThrownBy(() -> relationRowType(rowType))
                .hasMessage("not a relation type. expected multiset of row with anonymous fields");

        assertThatThrownBy(() -> relationRowType(BIGINT))
                .hasMessage("not a relation type. expected multiset of row with anonymous fields");
    }

    @Test
    public void testDeriveOutputMapping()
    {
        assertThatThrownBy(() -> deriveOutputMapping(BIGINT, ImmutableList.of(new Symbol(BIGINT, "a"))))
                .hasMessage("not a relation row type. expected RowType with anonymous fields or EmptyRowType");

        assertThatThrownBy(() -> deriveOutputMapping(EMPTY_ROW, ImmutableList.of(new Symbol(BIGINT, "a"))))
                .hasMessage("relation row type mismatch: output symbols present for EmptyRowType");

        assertThat(deriveOutputMapping(EMPTY_ROW, ImmutableList.of())).isEqualTo(ImmutableMap.of());

        assertThatThrownBy(() -> deriveOutputMapping(rowType(new RowType.Field(Optional.of("x"), BIGINT)), ImmutableList.of(new Symbol(BIGINT, "a"))))
                .hasMessage("not a relation row type. expected RowType with anonymous fields or EmptyRowType");

        assertThatThrownBy(() -> deriveOutputMapping(anonymousRow(BIGINT), ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b"))))
                .hasMessage("relation RowType does not match output symbols");

        assertThatThrownBy(() -> deriveOutputMapping(anonymousRow(BOOLEAN), ImmutableList.of(new Symbol(BIGINT, "a"))))
                .hasMessage("symbol type does not match field type");

        assertThat(deriveOutputMapping(
                anonymousRow(BIGINT, BOOLEAN),
                ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b"))))
                .isEqualTo(ImmutableMap.of(
                        new Symbol(BIGINT, "a"), 0,
                        new Symbol(BOOLEAN, "b"), 1));

        // duplicate output symbols
        assertThat(deriveOutputMapping(
                anonymousRow(BIGINT, BOOLEAN, BIGINT),
                ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b"), new Symbol(BIGINT, "a"))))
                .isEqualTo(ImmutableMap.of(
                        new Symbol(BIGINT, "a"), 0, // mapped to the first field
                        new Symbol(BOOLEAN, "b"), 1));
    }

    @Test
    public void testFieldSelectorBlock()
    {
        String name = "^field_selector";

        Block.Parameter firstParameter = new Block.Parameter("%first", irType(EMPTY_ROW));
        Block.Parameter secondParameter = new Block.Parameter("%second", irType(anonymousRow(BIGINT, BOOLEAN)));
        Block.Parameter thirdParameter = new Block.Parameter("%third", irType(anonymousRow(BOOLEAN)));
        List<Block.Parameter> inputRows = ImmutableList.of(firstParameter, secondParameter, thirdParameter);

        List<Map<Symbol, Integer>> symbolMappings = ImmutableList.of(
                ImmutableMap.of(),
                ImmutableMap.of(new Symbol(BIGINT, "X"), 0, new Symbol(BOOLEAN, "Y"), 1),
                ImmutableMap.of(new Symbol(BOOLEAN, "Z"), 0));

        // select symbol X from second parameter and symbol Z from third parameter
        Block selectSomeFields = new RelationalProgramBuilder(new ValueNameAllocator(), ImmutableMap.builder())
                .fieldSelectorBlock(
                        name,
                        inputRows,
                        symbolMappings,
                        ImmutableList.of(
                                ImmutableList.of(),
                                ImmutableList.of(new Symbol(BIGINT, "X")),
                                ImmutableList.of(new Symbol(BOOLEAN, "Z"))));

        FieldReference fieldReferenceOperationX = new FieldReference("%0", secondParameter, 0, ImmutableMap.of());
        FieldReference fieldReferenceOperationZ = new FieldReference("%1", thirdParameter, 0, ImmutableMap.of());
        Row rowOperation = new Row(
                "%2",
                ImmutableList.of(fieldReferenceOperationX.result(), fieldReferenceOperationZ.result()),
                ImmutableList.of(fieldReferenceOperationX.attributes(), fieldReferenceOperationZ.attributes()));
        Return returnRow = new Return("%3", rowOperation.result(), rowOperation.attributes());

        assertThat(selectSomeFields)
                .isEqualTo(new Block(
                        Optional.of("^field_selector"),
                        inputRows,
                        ImmutableList.of(
                                fieldReferenceOperationX,
                                fieldReferenceOperationZ,
                                rowOperation,
                                returnRow)));

        // do not select any fields -- return constant null of type EMPTY_ROW
        Block selectNoFields = new RelationalProgramBuilder(new ValueNameAllocator(), ImmutableMap.builder())
                .fieldSelectorBlock(
                        name,
                        inputRows,
                        symbolMappings,
                        ImmutableList.of(
                                ImmutableList.of(),
                                ImmutableList.of(),
                                ImmutableList.of()));

        Constant constantOperation = new Constant("%0", EMPTY_ROW, null);
        Return returnEmptyRow = new Return("%1", constantOperation.result(), constantOperation.attributes());

        assertThat(selectNoFields)
                .isEqualTo(new Block(
                        Optional.of("^field_selector"),
                        inputRows,
                        ImmutableList.of(
                                constantOperation,
                                returnEmptyRow)));

        // failure when selecting a non-existent symbol
        assertThatThrownBy(() -> new RelationalProgramBuilder(new ValueNameAllocator(), ImmutableMap.builder())
                .fieldSelectorBlock(
                        name,
                        inputRows,
                        symbolMappings,
                        ImmutableList.of(
                                ImmutableList.of(new Symbol(BOOLEAN, "non_existent_symbol")),
                                ImmutableList.of(),
                                ImmutableList.of())))
                .hasMessage("fieldIndex is null");

        // failure when input rows do not match input mappings
        assertThatThrownBy(() -> new RelationalProgramBuilder(new ValueNameAllocator(), ImmutableMap.builder())
                .fieldSelectorBlock(
                        name,
                        ImmutableList.of(firstParameter, secondParameter), // two input rows
                        symbolMappings, // three input mappings
                        ImmutableList.of(
                                ImmutableList.of(new Symbol(BOOLEAN, "non_existent_symbol")),
                                ImmutableList.of(),
                                ImmutableList.of())))
                .hasMessage("inputs and input symbol mappings do not match");

        // failure when input rows do not match selected symbol lists
        assertThatThrownBy(() -> new RelationalProgramBuilder(new ValueNameAllocator(), ImmutableMap.builder())
                .fieldSelectorBlock(
                        name,
                        inputRows, // three input rows
                        symbolMappings,
                        ImmutableList.of(
                                // four selected symbols lists
                                ImmutableList.of(new Symbol(BOOLEAN, "non_existent_symbol")),
                                ImmutableList.of(),
                                ImmutableList.of(),
                                ImmutableList.of())))
                .hasMessage("inputs and symbol lists do not match");
    }

    private static ValuesNode valuesNode()
    {
        return new ValuesNode(
                new PlanNodeId("values"),
                ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b")),
                ImmutableList.of(
                        new io.trino.sql.ir.Row(ImmutableList.of(
                                new io.trino.sql.ir.Constant(BIGINT, 1L),
                                new io.trino.sql.ir.Constant(BOOLEAN, true))),
                        new io.trino.sql.ir.Row(ImmutableList.of(
                                new io.trino.sql.ir.Constant(BIGINT, 2L),
                                new io.trino.sql.ir.Constant(BOOLEAN, false)))));
    }

    private static Values valuesOperation()
    {
        Constant constantOperation1Row1 = new Constant("%1", BIGINT, 1L);
        Constant constantOperation2Row1 = new Constant("%2", BOOLEAN, true);
        Row rowOperation1 = new Row(
                "%3",
                ImmutableList.of(constantOperation1Row1.result(), constantOperation2Row1.result()),
                ImmutableList.of(constantOperation1Row1.attributes(), constantOperation2Row1.attributes()));
        Return returnOperation1 = new Return("%4", rowOperation1.result(), rowOperation1.attributes());
        Constant constantOperation1Row2 = new Constant("%5", BIGINT, 2L);
        Constant constantOperation2Row2 = new Constant("%6", BOOLEAN, false);
        Row rowOperation2 = new Row(
                "%7",
                ImmutableList.of(constantOperation1Row2.result(), constantOperation2Row2.result()),
                ImmutableList.of(constantOperation1Row2.attributes(), constantOperation2Row2.attributes()));
        Return returnOperation2 = new Return("%8", rowOperation2.result(), rowOperation2.attributes());

        return new Values(
                "%0",
                RowType.anonymous(ImmutableList.of(BIGINT, BOOLEAN)),
                ImmutableList.of(
                        new Block(
                                Optional.of("^row"),
                                ImmutableList.of(),
                                ImmutableList.of(
                                        constantOperation1Row1,
                                        constantOperation2Row1,
                                        rowOperation1,
                                        returnOperation1)),
                        new Block(
                                Optional.of("^row"),
                                ImmutableList.of(),
                                ImmutableList.of(
                                        constantOperation1Row2,
                                        constantOperation2Row2,
                                        rowOperation2,
                                        returnOperation2))));
    }

    private void assertProgram(PlanNode plan, List<Operation> expected, io.trino.spi.type.Type expectedType, Map<Symbol, Integer> expectedMapping)
    {
        RelationalProgramBuilder relationalProgramBuilder = new RelationalProgramBuilder(new ValueNameAllocator(), ImmutableMap.builder());
        Block.Builder blockBuilder = new Block.Builder(Optional.empty(), ImmutableList.of());

        OperationAndMapping operationAndMapping = plan.accept(relationalProgramBuilder, new Context(blockBuilder));

        // Hack: add a terminal Return operation. It is required to build the Block.
        // Usually, the plan should have OutputNode as root, and that would result in Output terminal operation ending the Block.
        // For testing purpose, we don't apply the Output operation so that we can assert output symbol mapping.
        relationalProgramBuilder.addReturnOperation(blockBuilder);
        Block block = blockBuilder.build();
        // remove the Return operation
        List<Operation> actual = block.operations().subList(0, block.operations().size() - 1);

        assertThat(actual).isEqualTo(expected);

        // assert relation type
        if (plan.getOutputSymbols().isEmpty()) {
            assertThat(expectedType).isEqualTo(new MultisetType(EMPTY_ROW));
        }
        else {
            assertThat(expectedType)
                    .isEqualTo(new MultisetType(RowType.anonymous(plan.getOutputSymbols().stream()
                            .map(Symbol::type)
                            .collect(toImmutableList()))));
        }
        assertThat(expectedType).isEqualTo(trinoType(actual.getLast().result().type()));

        // assert symbol mapping
        assertThat(expectedMapping).isEqualTo(operationAndMapping.mapping());
    }
}
