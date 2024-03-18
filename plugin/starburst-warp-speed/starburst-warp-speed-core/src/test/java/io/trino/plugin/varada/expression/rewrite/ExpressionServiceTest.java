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
package io.trino.plugin.varada.expression.rewrite;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.varada.TestingTxService;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.configuration.NativeConfiguration;
import io.trino.plugin.varada.connector.TestingConnectorColumnHandle;
import io.trino.plugin.varada.connector.TestingConnectorProxiedConnectorTransformer;
import io.trino.plugin.varada.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.expression.NativeExpression;
import io.trino.plugin.varada.expression.TransformFunction;
import io.trino.plugin.varada.expression.VaradaCall;
import io.trino.plugin.varada.expression.VaradaConstant;
import io.trino.plugin.varada.expression.VaradaExpression;
import io.trino.plugin.varada.expression.VaradaExpressionData;
import io.trino.plugin.varada.expression.VaradaPrimitiveConstant;
import io.trino.plugin.varada.expression.VaradaSliceConstant;
import io.trino.plugin.varada.expression.VaradaVariable;
import io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.ExperimentSupportedFunction;
import io.trino.plugin.varada.expression.rewrite.coordinator.varadatonative.NativeExpressionRulesHandler;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.storage.engine.StubsStorageEngineConstants;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.stats.VaradaStatsPushdownPredicates;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;
import io.trino.type.RealOperators;
import io.varada.tools.util.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

import static io.trino.plugin.varada.VaradaSessionProperties.ENABLE_OR_PUSHDOWN;
import static io.trino.plugin.varada.expression.rewrite.ExpressionService.PUSHDOWN_PREDICATES_STAT_GROUP;
import static io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions.CEIL;
import static io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions.CONTAINS;
import static io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions.IS_NAN;
import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.CAST_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IN_PREDICATE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IS_NULL_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExpressionServiceTest
{
    private static final FunctionName MOD = new FunctionName("mod");
    private final Variable doubleVariable1 = new Variable("double1", DoubleType.DOUBLE);
    private final Variable doubleVariable2 = new Variable("double2", DoubleType.DOUBLE);
    private final Variable longDecimalVariable = new Variable("longDecimal", createDecimalType(30, 2));
    private final VarcharType varcharType = VarcharType.createVarcharType(10);
    private final Variable varcharVariable = new Variable("varchar1", varcharType);
    private final Variable realVariable = new Variable("real1", RealType.REAL);
    private ExpressionService expressionService;
    private GlobalConfiguration globalConfiguration;
    private ConnectorSession connectorSession;
    private Map<String, ColumnHandle> assignments;
    private MetricsManager metricsManager;
    private Map<String, Long> customStats;

    static Stream<Arguments> nonDefaultFormatDoubleParams()
    {
        return Stream.of(
                arguments("01111111111111111111E0"),
                // '5' would be casted to 5.0E0, then, we'll gen an exception because 5.0E0 can't be casted back into varchar(1)
                arguments("5"));
    }

    static Stream<Arguments> arrayTypes()
    {
        return Stream.of(
                arguments(new ArrayType(VarcharType.VARCHAR), true),
                arguments(new ArrayType(VarcharType.createVarcharType(10)), true),
                arguments(new ArrayType(VarcharType.VARCHAR), true),
                arguments(new ArrayType(IntegerType.INTEGER), false),
                arguments(new ArrayType(DoubleType.DOUBLE), false),
                arguments(new ArrayType(BigintType.BIGINT), false));
    }

    @BeforeEach
    public void beforeEach()
    {
        this.customStats = new HashMap<>();
        globalConfiguration = new GlobalConfiguration();
        metricsManager = TestingTxService.createMetricsManager();
        StorageEngineConstants storageEngineConstants = new StubsStorageEngineConstants();
        NativeExpressionRulesHandler nativeExpressionRulesHandler = new NativeExpressionRulesHandler(storageEngineConstants, metricsManager);
        DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer = new TestingConnectorProxiedConnectorTransformer();
        expressionService = new ExpressionService(dispatcherProxiedConnectorTransformer,
                new ExperimentSupportedFunction(metricsManager),
                globalConfiguration,
                new NativeConfiguration(),
                metricsManager,
                nativeExpressionRulesHandler);
        connectorSession = mock(ConnectorSession.class);
        when(connectorSession.getProperty(ENABLE_OR_PUSHDOWN, Boolean.class)).thenReturn(true);
        assignments = createAssignments(doubleVariable1, doubleVariable2, varcharVariable, realVariable);
    }

    @Test
    public void testEmptyExpression()
    {
        ConnectorExpression expression = new Constant(true, BOOLEAN);
        Optional<WarpExpression> actual = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats);
        assertThat(actual.isEmpty()).isTrue();
        assertPushdownStatsSum(0);
    }

    /**
     * is_nan(longDecimal)
     * decimal isn't valid type
     */
    @Test
    public void testInvalidType()
    {
        Call expression = new Call(
                BooleanType.BOOLEAN,
                IS_NAN,
                List.of(longDecimalVariable));
        Optional<WarpExpression> actual = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats);
        assertThat(actual).isEmpty();
        assertPushdownStatsSum(1);
    }

    /**
     * is_nan(double1)
     */
    @Test
    public void testVaradaFunctionsIs_Nan()
    {
        VaradaExpression expectedResult = new VaradaCall(IS_NAN.getName(),
                List.of(createExpectedVariable(doubleVariable1)), BOOLEAN);

        Call expression = new Call(
                BooleanType.BOOLEAN,
                IS_NAN,
                List.of(doubleVariable1));
        List<VaradaExpressionData> actual = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats).orElseThrow().varadaExpressionDataLeaves();
        assertThat(actual.size()).isEqualTo(1);
        assertThat(actual.get(0).getExpression()).isEqualTo(expectedResult);
        assertPushdownStatsSum(0);
    }

    /**
     * double1 > double2
     */
    @Test
    public void testFunctionContains2DistinctColumns_NotSupported()
    {
        ConnectorExpression call = new Call(
                BOOLEAN,
                StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME,
                List.of(
                        doubleVariable1,
                        doubleVariable2));
        Optional<WarpExpression> actual = expressionService.convertToWarpExpression(connectorSession, call, assignments, customStats);
        assertThat(actual).isEmpty();
        assertPushdownStatsSum(1);
        VaradaStatsPushdownPredicates pushdownPredicatesStats = (VaradaStatsPushdownPredicates) metricsManager.get(PUSHDOWN_PREDICATES_STAT_GROUP);
        assertThat(pushdownPredicatesStats.getunsupported_functions()).isEqualTo(1);
    }

    /**
     * is_nan(double1) = false
     */
    @Test
    public void testInverseIsNan()
    {
        Pair<Call, VaradaCall> isNanExpression = createCallExpression(IS_NAN,
                EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                Constant.FALSE);
        List<VaradaExpressionData> expectedResult = List.of(
                new VaradaExpressionData(isNanExpression.getValue(),
                        doubleVariable1.getType(),
                        false,
                        Optional.empty(),
                        new RegularColumn(doubleVariable1.getName())));
        List<VaradaExpressionData> actual = expressionService.convertToWarpExpression(connectorSession, isNanExpression.getKey(), assignments, customStats).orElseThrow().varadaExpressionDataLeaves();
        assertThat(actual).isEqualTo(expectedResult);
        assertPushdownStatsSum(1);
        VaradaStatsPushdownPredicates pushdownPredicatesStats = (VaradaStatsPushdownPredicates) metricsManager.get(PUSHDOWN_PREDICATES_STAT_GROUP);
        assertThat(pushdownPredicatesStats.getunsupported_functions_native()).isEqualTo(1);
    }

    /**
     * is_nan(double1) = true
     */
    @Test
    public void testValidIsNan()
    {
        Pair<Call, VaradaCall> isNanExpression = createCallExpression(IS_NAN,
                EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                Constant.TRUE);
        NativeExpression expectedNativeExpression = new NativeExpression(PredicateType.PREDICATE_TYPE_VALUES,
                FunctionType.FUNCTION_TYPE_IS_NAN,
                Domain.singleValue(BOOLEAN, true),
                false,
                true,
                Collections.emptyList(), TransformFunction.NONE);
        List<VaradaExpressionData> expectedResult = List.of(
                new VaradaExpressionData(isNanExpression.getValue(),
                        doubleVariable1.getType(),
                        false,
                        Optional.of(expectedNativeExpression),
                        new RegularColumn(doubleVariable1.getName())));
        List<VaradaExpressionData> actual = expressionService.convertToWarpExpression(connectorSession, isNanExpression.getKey(), assignments, customStats).orElseThrow().varadaExpressionDataLeaves();
        assertThat(actual).isEqualTo(expectedResult);
        assertPushdownStatsSum(0);
    }

    /**
     * ceil(double1) > 5
     */
    @Test
    public void testVaradaFunctionsClientCeil()
    {
        Pair<Call, VaradaCall> ceilExpression = createCallExpression(CEIL,
                GREATER_THAN_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                new Constant(5D, doubleVariable1.getType()));

        List<VaradaExpressionData> actual = expressionService.convertToWarpExpression(connectorSession, ceilExpression.getKey(), assignments, customStats).orElseThrow().varadaExpressionDataLeaves();
        assertThat(actual.size()).isEqualTo(1);
        assertThat(actual.get(0).getExpression()).isEqualTo(ceilExpression.getValue());
        assertPushdownStatsSum(0);
    }

    /**
     * where lower(v1) == null
     * translated by trino to:
     * VaradaConstant Boolean:null
     */
    @Test
    public void testConstantExpression()
    {
        Constant constant = new Constant(null, BOOLEAN);
        Optional<WarpExpression> actual = expressionService.convertToWarpExpression(connectorSession, constant, assignments, customStats);
        assertThat(actual).isEmpty();
    }

    /**
     * where double1 is null or is_nan(double1) = true or ceil(double1) > 5
     */
    @Test
    public void testExpressionWithNull2()
    {
        Pair<Call, VaradaCall> isNanExpression = createCallExpression(IS_NAN,
                EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                Constant.TRUE);
        Pair<Call, VaradaCall> right = createCallExpression(CEIL,
                GREATER_THAN_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                new Constant(5D, DoubleType.DOUBLE));

        Call isNull = new Call(doubleVariable1.getType(),
                IS_NULL_FUNCTION_NAME,
                List.of(new Variable(doubleVariable1.getName(), doubleVariable1.getType())));

        ConnectorExpression expression = new Call(
                BOOLEAN,
                OR_FUNCTION_NAME,
                List.of(isNull, isNanExpression.getKey(), right.getKey()));
        List<VaradaExpressionData> actual = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats).orElseThrow().varadaExpressionDataLeaves();
        assertThat(actual.size()).isEqualTo(3);
    }

    /**
     * contains(arr1 , is_nan(col1))
     */
    @Test
    public void testNestedFunctionWith2differentColumns_notSupported()
    {
        String arr1 = "arr1";
        TestingConnectorColumnHandle columnHandle = new TestingConnectorColumnHandle(IntegerType.INTEGER, arr1);
        assignments.put(arr1, columnHandle);
        ConnectorExpression call = new Call(
                BOOLEAN,
                new FunctionName("contains"),
                List.of(
                        new Variable(arr1, new ArrayType(BOOLEAN)),
                        new Call(
                                BooleanType.BOOLEAN,
                                IS_NAN,
                                List.of(doubleVariable1))));
        Optional<WarpExpression> actual = expressionService.convertToWarpExpression(connectorSession, call, assignments, customStats);
        assertThat(actual).isEmpty();
        assertPushdownStatsSum(1);
        VaradaStatsPushdownPredicates pushdownPredicatesStats = (VaradaStatsPushdownPredicates) metricsManager.get(PUSHDOWN_PREDICATES_STAT_GROUP);
        assertThat(pushdownPredicatesStats.getunsupported_functions()).isEqualTo(1);
    }

    /**
     * ceil(double1) = 5  or unsupported(double1)=true
     */
    @Test
    public void testFunctionWithOrSameColumnOneFunctionNotSupported()
    {
        Pair<Call, VaradaCall> expression = createCallExpression(CEIL,
                EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                new Constant(5D, DoubleType.DOUBLE));
        ConnectorExpression connectorExpression = new Call(
                BOOLEAN,
                OR_FUNCTION_NAME,
                List.of(expression.getKey(),
                        new Call(
                                BOOLEAN,
                                StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME,
                                List.of(
                                        new Call(
                                                BOOLEAN,
                                                new FunctionName("unsupported"),
                                                List.of(doubleVariable1)),
                                        new Constant(true, BOOLEAN)))));
        Optional<WarpExpression> actual = expressionService.convertToWarpExpression(connectorSession, connectorExpression, assignments, customStats);
        assertThat(actual).isEmpty();
        assertPushdownStatsSum(1);
        VaradaStatsPushdownPredicates pushdownPredicatesStats = (VaradaStatsPushdownPredicates) metricsManager.get(PUSHDOWN_PREDICATES_STAT_GROUP);
        assertThat(pushdownPredicatesStats.getunsupported_functions()).isEqualTo(1);
    }

    /**
     * is_nan(double1) = true  and unsupported(double1) > 5
     */
    @Test
    public void testFunctionWith_And_UnsupportedFunction()
    {
        Pair<Call, VaradaCall> isNanExpression = createCallExpression(IS_NAN,
                EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                Constant.TRUE);

        Optional<NativeExpression> expectedNativeExpression = Optional.of(NativeExpression
                .builder()
                .predicateType(PredicateType.PREDICATE_TYPE_VALUES)
                .functionType(FunctionType.FUNCTION_TYPE_IS_NAN)
                .domain(Domain.create(ValueSet.ofRanges(Range.equal(BOOLEAN, true)), false))
                .collectNulls(false)
                .build());
        List<VaradaExpressionData> expectedResult = List.of(new VaradaExpressionData(isNanExpression.getValue(),
                doubleVariable1.getType(),
                false,
                expectedNativeExpression,
                new RegularColumn(doubleVariable1.getName())));
        ConnectorExpression expression = new Call(
                BOOLEAN,
                AND_FUNCTION_NAME,
                List.of(new Call(
                                BOOLEAN,
                                StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME,
                                List.of(
                                        new Call(
                                                BOOLEAN,
                                                new FunctionName("unsupported"),
                                                List.of(doubleVariable1)),
                                        new Constant(5, IntegerType.INTEGER))),
                        isNanExpression.getKey()));
        List<VaradaExpressionData> actual = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats).orElseThrow().varadaExpressionDataLeaves();
        assertThat(actual).isEqualTo(expectedResult);
        assertPushdownStatsSum(1);
        VaradaStatsPushdownPredicates pushdownPredicatesStats = (VaradaStatsPushdownPredicates) metricsManager.get(PUSHDOWN_PREDICATES_STAT_GROUP);
        assertThat(pushdownPredicatesStats.getunsupported_functions()).isEqualTo(1);
    }

    /**
     * ceil(double1) > 5  or is_nan(double1)=true
     */
    @Test
    public void testFunctionWithOrFunctionSameColumn()
    {
        Pair<Call, VaradaCall> left = createCallExpression(CEIL, GREATER_THAN_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                new Constant(5D, DoubleType.DOUBLE));

        Pair<Call, VaradaCall> isNanExpression = createCallExpression(IS_NAN,
                EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                Constant.TRUE);
        ConnectorExpression expression = new Call(
                BOOLEAN,
                OR_FUNCTION_NAME,
                List.of(left.getKey(), isNanExpression.getKey()));
        List<VaradaExpressionData> actual = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats).orElseThrow().varadaExpressionDataLeaves();
        assertThat(actual.size()).isEqualTo(2);
    }

    /**
     * is_nan(double1) = true  or unsupported(double1) > 5
     */
    @Test
    public void testFunctionWith_OR_UnsupportedFunction()
    {
        Call left = new Call(
                BOOLEAN,
                StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME,
                List.of(
                        new Call(
                                BOOLEAN,
                                new FunctionName("unsupported"),
                                List.of(doubleVariable1)),
                        new Constant(5D, DoubleType.DOUBLE)));
        Call right = new Call(
                BOOLEAN,
                EQUAL_OPERATOR_FUNCTION_NAME,
                List.of(
                        new Call(
                                BOOLEAN,
                                IS_NAN,
                                List.of(doubleVariable1)),
                        new Constant(true, BOOLEAN)));
        ConnectorExpression expression = new Call(
                BOOLEAN,
                OR_FUNCTION_NAME,
                List.of(left, right));
        Optional<WarpExpression> actual = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats);
        assertThat(actual).isEmpty();
        assertPushdownStatsSum(1);
        VaradaStatsPushdownPredicates pushdownPredicatesStats = (VaradaStatsPushdownPredicates) metricsManager.get(PUSHDOWN_PREDICATES_STAT_GROUP);
        assertThat(pushdownPredicatesStats.getunsupported_functions()).isEqualTo(1);
    }

    /**
     * ceil(double1) = 5  and is_nan(double1)=true
     */
    @Test
    public void testFunctionWithAndFunctionSameColumn()
    {
        Pair<Call, VaradaCall> left = createCallExpression(CEIL, EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                new Constant(5D, DoubleType.DOUBLE));
        Pair<Call, VaradaCall> isNanExpression = createCallExpression(IS_NAN,
                EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                Constant.TRUE);
        ConnectorExpression expression = new Call(
                BOOLEAN,
                AND_FUNCTION_NAME,
                List.of(left.getKey(), isNanExpression.getKey()));
        List<VaradaExpressionData> actual = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats).orElseThrow().varadaExpressionDataLeaves();
        assertThat(actual.size()).isEqualTo(2);
    }

    /**
     * hypothetical case of And of OR's - native does not support this
     * (double1 > 50 AND double2 < 10) OR (varcharVariable = 'bla' AND double2 > 5)
     */
    @Test
    public void testOrOfAnds()
    {
        ConnectorSession orEnabled = mock(ConnectorSession.class);
        when(orEnabled.getProperty(ENABLE_OR_PUSHDOWN, Boolean.class)).thenReturn(true);
        double leftValue = 5D;
        double rightValue = 10D;
        Pair<Call, VaradaCall> left = createCallExpression(GREATER_THAN_OPERATOR_FUNCTION_NAME, doubleVariable1, new Constant(leftValue, DoubleType.DOUBLE));
        Pair<Call, VaradaCall> right = createCallExpression(LESS_THAN_OPERATOR_FUNCTION_NAME, doubleVariable2, new Constant(rightValue, DoubleType.DOUBLE));
        ConnectorExpression leftOrExpression = new Call(BOOLEAN, AND_FUNCTION_NAME, List.of(left.getKey(), right.getKey()));
        left = createCallExpression(EQUAL_OPERATOR_FUNCTION_NAME, varcharVariable, new Constant(Slices.utf8Slice("bla"), VarcharType.VARCHAR));
        right = createCallExpression(GREATER_THAN_OPERATOR_FUNCTION_NAME, doubleVariable2, new Constant(leftValue, DoubleType.DOUBLE));
        ConnectorExpression rightOrExpression = new Call(BOOLEAN, AND_FUNCTION_NAME, List.of(left.getKey(), right.getKey()));
        ConnectorExpression connectorExpression = new Call(BOOLEAN, OR_FUNCTION_NAME, List.of(leftOrExpression, rightOrExpression));
        Optional<WarpExpression> warpExpression = expressionService.convertToWarpExpression(orEnabled, connectorExpression, assignments, customStats);
        assertThat(warpExpression.orElseThrow().varadaExpressionDataLeaves().size()).isEqualTo(4);
    }

    /**
     * double1 > 50 OR double2 < 10
     */
    @Test
    public void testWarpExpression()
    {
        ConnectorSession orEnabled = mock(ConnectorSession.class);
        when(orEnabled.getProperty(ENABLE_OR_PUSHDOWN, Boolean.class)).thenReturn(true);
        double leftValue = 5D;
        double rightValue = 10D;
        Pair<Call, VaradaCall> left = createCallExpression(GREATER_THAN_OPERATOR_FUNCTION_NAME, doubleVariable1, new Constant(leftValue, DoubleType.DOUBLE));
        Pair<Call, VaradaCall> right = createCallExpression(LESS_THAN_OPERATOR_FUNCTION_NAME, doubleVariable2, new Constant(rightValue, DoubleType.DOUBLE));
        ConnectorExpression connectorExpression = new Call(BOOLEAN, OR_FUNCTION_NAME, List.of(left.getKey(), right.getKey()));
        VaradaExpression varadaExpression = new VaradaCall(OR_FUNCTION_NAME.getName(), List.of(left.getValue(), right.getValue()), BOOLEAN);

        VaradaExpressionData leftVaradaExpressionData = new VaradaExpressionData(left.getValue(),
                doubleVariable1.getType(),
                false,
                Optional.of(new NativeExpression(PredicateType.PREDICATE_TYPE_RANGES,
                        FunctionType.FUNCTION_TYPE_NONE,
                        Domain.create(ValueSet.ofRanges(Range.greaterThan(doubleVariable1.getType(), leftValue)), false),
                        false,
                        false,
                        Collections.emptyList(), TransformFunction.NONE)),
                new RegularColumn(doubleVariable1.getName()));
        VaradaExpressionData rightVaradaExpressionData = new VaradaExpressionData(right.getValue(),
                doubleVariable2.getType(),
                false,
                Optional.of(new NativeExpression(PredicateType.PREDICATE_TYPE_RANGES,
                        FunctionType.FUNCTION_TYPE_NONE,
                        Domain.create(ValueSet.ofRanges(Range.lessThan(doubleVariable2.getType(), rightValue)), false),
                        false,
                        false,
                        Collections.emptyList(), TransformFunction.NONE)),
                new RegularColumn(doubleVariable2.getName()));
        WarpExpression expectedSiacExpression = new WarpExpression(varadaExpression, List.of(leftVaradaExpressionData, rightVaradaExpressionData));
        WarpExpression warpExpression = expressionService.convertToWarpExpression(orEnabled, connectorExpression, assignments, customStats).orElseThrow();
        assertThat(warpExpression).isEqualTo(expectedSiacExpression);
    }

    /**
     * ceil(double1) = 5  or ceil(double1) > 10
     */
    @Test
    public void testAggregateFunctionSameFunctionSameColumn()
    {
        Pair<Call, VaradaCall> left = createCallExpression(CEIL, EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                new Constant(5D, DoubleType.DOUBLE));
        Pair<Call, VaradaCall> right = createCallExpression(CEIL, GREATER_THAN_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                new Constant(10D, DoubleType.DOUBLE));
        ConnectorExpression expression = new Call(
                BOOLEAN,
                OR_FUNCTION_NAME,
                List.of(left.getKey(), right.getKey()));
        List<VaradaExpressionData> varadaExpressions = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats).orElseThrow().varadaExpressionDataLeaves();
        assertThat(varadaExpressions.size()).isEqualTo(2);
    }

    /**
     * ceil(double1) > 5  and is_nan(double2)=true
     */
    @Test
    public void testFunction2DifferentColumns_Allowed()
    {
        Pair<Call, VaradaCall> left = createCallExpression(CEIL, GREATER_THAN_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                new Constant(5D, DoubleType.DOUBLE));
        Pair<Call, VaradaCall> isNanExpression = createCallExpression(IS_NAN,
                EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable2,
                Constant.TRUE);
        Optional<NativeExpression> expectedNativeExpression1 = Optional.of(NativeExpression
                .builder()
                .predicateType(PredicateType.PREDICATE_TYPE_RANGES)
                .functionType(FunctionType.FUNCTION_TYPE_CEIL)
                .domain(Domain.create(ValueSet.ofRanges(Range.greaterThan(DoubleType.DOUBLE, 5D)), false))
                .collectNulls(false)
                .build());
        Optional<NativeExpression> expectedNativeExpression2 = Optional.of(NativeExpression
                .builder()
                .predicateType(PredicateType.PREDICATE_TYPE_VALUES)
                .functionType(FunctionType.FUNCTION_TYPE_IS_NAN)
                .domain(Domain.create(ValueSet.ofRanges(Range.equal(BOOLEAN, true)), false))
                .collectNulls(false)
                .build());
        RegularColumn varadaColumn1 = new RegularColumn(doubleVariable1.getName());
        RegularColumn varadaColumn2 = new RegularColumn(doubleVariable2.getName());
        List<VaradaExpressionData> expectedResult = List.of(new VaradaExpressionData(left.getValue(), doubleVariable1.getType(), false, expectedNativeExpression1, varadaColumn1),
                new VaradaExpressionData(isNanExpression.getValue(), doubleVariable2.getType(), false, expectedNativeExpression2, varadaColumn2));

        ConnectorExpression expression = new Call(
                BOOLEAN,
                AND_FUNCTION_NAME,
                List.of(left.getKey(), isNanExpression.getKey()));
        List<VaradaExpressionData> actual = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats).orElseThrow().varadaExpressionDataLeaves();
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expectedResult);
        assertPushdownStatsSum(0);
    }

    /**
     * (ceil(double1) > 10 AND (mode(double1, 3) = 1 OR mod(double1, 2) = 0))
     */
    @Test
    public void testComplex1()
    {
        Pair<Call, VaradaCall> left = createCallExpression(CEIL, GREATER_THAN_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                new Constant(10D, DoubleType.DOUBLE));
        Pair<Call, VaradaCall> middle = createModVaradaCall(EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                new Constant(3D, DoubleType.DOUBLE),
                new Constant(1D, DoubleType.DOUBLE));
        Pair<Call, VaradaCall> right = createModVaradaCall(EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                new Constant(2D, DoubleType.DOUBLE),
                new Constant(0D, DoubleType.DOUBLE));
        ConnectorExpression rightSide = new Call(
                BOOLEAN,
                OR_FUNCTION_NAME,
                List.of(middle.getKey(), right.getKey()));
        ConnectorExpression expression = new Call(
                BOOLEAN,
                AND_FUNCTION_NAME,
                List.of(left.getKey(), rightSide));
        List<VaradaExpressionData> actual = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats).orElseThrow().varadaExpressionDataLeaves();
        assertThat(actual.size()).isEqualTo(3);
        assertPushdownStatsSum(2);
    }

    /**
     * (ceil(double2) > 10 AND (mode(double1, 3) = 1 OR mod(double1, 2) = 0))
     */
    @Test
    public void testComplex2()
    {
        Pair<Call, VaradaCall> expectedLeft = createCallExpression(CEIL, GREATER_THAN_OPERATOR_FUNCTION_NAME,
                doubleVariable2,
                new Constant(10D, DoubleType.DOUBLE));
        Pair<Call, VaradaCall> left = createModVaradaCall(EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                new Constant(3D, DoubleType.DOUBLE),
                new Constant(1D, DoubleType.DOUBLE));
        Pair<Call, VaradaCall> right = createModVaradaCall(EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                new Constant(2D, DoubleType.DOUBLE),
                new Constant(0D, DoubleType.DOUBLE));
        ConnectorExpression rightSide = new Call(
                BOOLEAN,
                OR_FUNCTION_NAME,
                List.of(left.getKey(), right.getKey()));

        ConnectorExpression expression = new Call(
                BOOLEAN,
                AND_FUNCTION_NAME,
                List.of(expectedLeft.getKey(), rightSide));
        List<VaradaExpressionData> actual = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats).orElseThrow().varadaExpressionDataLeaves();
        assertThat(actual.size()).isEqualTo(3);
    }

    /**
     * (ceil(double1) = 5  or is_nan(double1)=true) and (mod(double2 , 2) = 0 or mod(double2 , 3) = 0)
     */
    @Test
    public void testFunctionWithComplex()
    {
        Pair<Call, VaradaCall> expectedLeft1 = createCallExpression(CEIL, GREATER_THAN_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                new Constant(10D, DoubleType.DOUBLE));
        Pair<Call, VaradaCall> isNanExpression = createCallExpression(IS_NAN,
                EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                Constant.TRUE);
        Pair<Call, VaradaCall> right1 = createModVaradaCall(EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable2,
                new Constant(2D, DoubleType.DOUBLE),
                new Constant(0D, DoubleType.DOUBLE));
        Pair<Call, VaradaCall> right2 = createModVaradaCall(EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable2,
                new Constant(3D, DoubleType.DOUBLE),
                new Constant(0D, DoubleType.DOUBLE));
        ConnectorExpression leftSide = new Call(
                BOOLEAN,
                OR_FUNCTION_NAME,
                List.of(expectedLeft1.getKey(), isNanExpression.getKey()));
        ConnectorExpression rightSide = new Call(
                BOOLEAN,
                OR_FUNCTION_NAME,
                List.of(right1.getKey(), right2.getKey()));

        ConnectorExpression expression = new Call(
                BOOLEAN,
                AND_FUNCTION_NAME,
                List.of(leftSide, rightSide));
        List<VaradaExpressionData> actual = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats).orElseThrow().varadaExpressionDataLeaves();
        assertThat(actual.size()).isEqualTo(4);
    }

    /**
     * (ceil(double1) = 5  or is_nan(double1)=true) and (mod(double2 , 2) = 0 or mod(double2 , 3) = 0)
     */
    @Test
    public void testFunctionWithComplex2()
    {
        Pair<Call, VaradaCall> expectedLeft1 = createCallExpression(CEIL, EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                new Constant(5D, DoubleType.DOUBLE));
        Pair<Call, VaradaCall> isNanExpression = createCallExpression(IS_NAN,
                EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                Constant.TRUE);
        Pair<Call, VaradaCall> right1 = createModVaradaCall(EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable2,
                new Constant(2D, DoubleType.DOUBLE),
                new Constant(0D, DoubleType.DOUBLE));
        Pair<Call, VaradaCall> right2 = createModVaradaCall(EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable2,
                new Constant(3D, DoubleType.DOUBLE),
                new Constant(0D, DoubleType.DOUBLE));
        ConnectorExpression leftSide = new Call(
                BOOLEAN,
                OR_FUNCTION_NAME,
                List.of(expectedLeft1.getKey(), isNanExpression.getKey()));
        ConnectorExpression rightSide = new Call(
                BOOLEAN,
                OR_FUNCTION_NAME,
                List.of(right1.getKey(), right2.getKey()));

        ConnectorExpression expression = new Call(
                BOOLEAN,
                AND_FUNCTION_NAME,
                List.of(leftSide, rightSide));
        List<VaradaExpressionData> actual = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats).orElseThrow().varadaExpressionDataLeaves();
        assertThat(actual.size()).isEqualTo(4);
    }

    static Stream<Arguments> unsupportedTypes()
    {
        return Stream.of(
                arguments(new MapType(TimeWithTimeZoneType.createTimeWithTimeZoneType(12), VarcharType.VARCHAR, new TypeOperators())),
                arguments(new MapType(VarcharType.VARCHAR, TimeWithTimeZoneType.createTimeWithTimeZoneType(12), new TypeOperators())),
                arguments(TimeWithTimeZoneType.createTimeWithTimeZoneType(12)),
                arguments(TimestampWithTimeZoneType.createTimestampWithTimeZoneType(12)),
                arguments(DecimalType.createDecimalType(30, 10)));
    }

    /**
     * unsupportedType -> nothing
     */
    @ParameterizedTest
    @MethodSource("unsupportedTypes")
    public void testUnsupportedColumnType(Type type)
    {
        Variable column = new Variable("column", type);
        TestingConnectorColumnHandle columnHandle = new TestingConnectorColumnHandle(column.getType(), column.getName());
        assignments.put(columnHandle.name(), columnHandle);
        Constant constant = new Constant(5D, DoubleType.DOUBLE);
        ConnectorExpression expression = new Call(type, EQUAL_OPERATOR_FUNCTION_NAME, List.of(column, constant));
        Optional<WarpExpression> warpExpression = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats);
        assertThat(warpExpression).isEmpty();
    }

    /**
     * ceil(double1)  or unsupportedType -> nothing
     */
    @ParameterizedTest
    @MethodSource("unsupportedTypes")
    public void testUnsupportedColumnTypeWithOr(Type type)
    {
        Variable column = new Variable("column", type);
        TestingConnectorColumnHandle columnHandle = new TestingConnectorColumnHandle(column.getType(), column.getName());
        assignments.put(columnHandle.name(), columnHandle);
        Constant constant = new Constant(5D, DoubleType.DOUBLE);
        ConnectorExpression invalidExpression = new Call(type, EQUAL_OPERATOR_FUNCTION_NAME, List.of(column, constant));
        Pair<Call, VaradaCall> left = createCallExpression(CEIL, GREATER_THAN_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                new Constant(5D, DoubleType.DOUBLE));
        ConnectorExpression expression = new Call(
                BOOLEAN,
                OR_FUNCTION_NAME,
                List.of(left.getKey(), invalidExpression));
        Optional<WarpExpression> warpExpression = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats);
        assertThat(warpExpression).isEmpty();
    }

    /**
     * ceil(double1)  And unsupportedType -> ceil(double1)
     */
    @ParameterizedTest
    @MethodSource("unsupportedTypes")
    public void testUnsupportedColumnTypeWithAnd(Type type)
    {
        Variable column = new Variable("column", type);
        TestingConnectorColumnHandle columnHandle = new TestingConnectorColumnHandle(column.getType(), column.getName());
        assignments.put(columnHandle.name(), columnHandle);
        Constant constant = new Constant(5D, DoubleType.DOUBLE);
        ConnectorExpression invalidExpression = new Call(type, EQUAL_OPERATOR_FUNCTION_NAME, List.of(column, constant));
        Pair<Call, VaradaCall> left = createCallExpression(CEIL, GREATER_THAN_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                new Constant(5D, DoubleType.DOUBLE));
        ConnectorExpression expression = new Call(
                BOOLEAN,
                AND_FUNCTION_NAME,
                List.of(left.getKey(), invalidExpression));
        Optional<WarpExpression> warpExpression = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats);
        assertThat(warpExpression.isPresent()).isTrue();
        assertThat(warpExpression.orElseThrow().varadaExpressionDataLeaves().size()).isOne();
        assertThat(warpExpression.orElseThrow().rootExpression()).isEqualTo(left.getValue());
    }

    /**
     * (ceil(double1) = 5  or is_nan(double1)=true) and (mod(double2 , 2) = 0 or mod(double1 , 3) = 0)
     */
    @Test
    public void testFunctionWithComplexRightSideIsNotSupported()
    {
        Pair<Call, VaradaCall> expectedLeft1 = createCallExpression(CEIL, EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                new Constant(5D, DoubleType.DOUBLE));
        Pair<Call, VaradaCall> isNanExpression = createCallExpression(IS_NAN,
                EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                Constant.TRUE);
        ConnectorExpression leftSide = new Call(
                BOOLEAN,
                OR_FUNCTION_NAME,
                List.of(expectedLeft1.getKey(), isNanExpression.getKey()));
        Pair<Call, VaradaCall> right1 = createModVaradaCall(EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable2,
                new Constant(2D, DoubleType.DOUBLE),
                new Constant(0D, DoubleType.DOUBLE));
        Pair<Call, VaradaCall> right2 = createModVaradaCall(EQUAL_OPERATOR_FUNCTION_NAME,
                doubleVariable1,
                new Constant(3D, DoubleType.DOUBLE),
                new Constant(0D, DoubleType.DOUBLE));
        ConnectorExpression rightSide = new Call(
                BOOLEAN,
                OR_FUNCTION_NAME,
                List.of(right1.getKey(), right2.getKey()));

        ConnectorExpression expression = new Call(
                BOOLEAN,
                AND_FUNCTION_NAME,
                List.of(leftSide, rightSide));
        List<VaradaExpressionData> actual = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats).orElseThrow().varadaExpressionDataLeaves();
        assertThat(actual.size()).isEqualTo(4);
    }

    /**
     * ( doubleVariable1 = 4  AND  doubleVariable2 <= 6 )  OR  doubleVariable1 = 2
     */
    @Test
    public void testHypotheticalCaseOrROfANDs()
    {
        Call leftSide = new Call(
                BOOLEAN, AND_FUNCTION_NAME,
                List.of(new Call(
                                BOOLEAN,
                                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                                List.of(doubleVariable1, new Constant(4D, DoubleType.DOUBLE))),
                        new Call(
                                BOOLEAN,
                                StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME,
                                List.of(doubleVariable2, new Constant(6D, DoubleType.DOUBLE)))));

        Call rightSide = new Call(
                BOOLEAN,
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                List.of(doubleVariable1, new Constant(2D, DoubleType.DOUBLE)));

        Call expression = new Call(BOOLEAN, OR_FUNCTION_NAME, List.of(leftSide, rightSide));
        Optional<WarpExpression> result = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats);
        assertThat(result.orElseThrow().varadaExpressionDataLeaves().size()).isEqualTo(3);
    }

    /**
     * ( doubleVariable1 = 4  AND  doubleVariable2 <= 6 )  OR  varchar = 'b'
     */
    @Test
    public void testMultiLevelExp()
    {
        Call leftSide = new Call(
                BOOLEAN, AND_FUNCTION_NAME,
                List.of(new Call(
                                BOOLEAN,
                                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                                List.of(doubleVariable1, new Constant(4D, DoubleType.DOUBLE))),
                        new Call(
                                BOOLEAN,
                                StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME,
                                List.of(doubleVariable2, new Constant(6D, DoubleType.DOUBLE)))));

        Call rightSide = new Call(
                BOOLEAN,
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                List.of(varcharVariable, new Constant(Slices.utf8Slice("b"), VarcharType.VARCHAR)));

        Call expression = new Call(BOOLEAN, OR_FUNCTION_NAME, List.of(leftSide, rightSide));

        Optional<WarpExpression> result = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats);
        assertThat(result.orElseThrow().varadaExpressionDataLeaves().size()).isEqualTo(3);
    }

    @Test
    public void testUnsupportedTooManyLevels()
    {
        Call leftSide = new Call(
                BOOLEAN,
                StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME,
                List.of(doubleVariable2, new Constant(6D, DoubleType.DOUBLE)));
        Call rightSide = new Call(
                BOOLEAN,
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                List.of(varcharVariable, new Constant(Slices.utf8Slice("b"), VarcharType.VARCHAR)));

        Call l4Expression = new Call(BOOLEAN, AND_FUNCTION_NAME, List.of(leftSide, rightSide));
        Call l3Expression = new Call(BOOLEAN, OR_FUNCTION_NAME, List.of(l4Expression, rightSide));
        Call l2Expression = new Call(BOOLEAN, AND_FUNCTION_NAME, List.of(l3Expression, rightSide));
        Call l1Expression = new Call(BOOLEAN, OR_FUNCTION_NAME, List.of(l2Expression, rightSide));
        Call l0Expression = new Call(BOOLEAN, AND_FUNCTION_NAME, List.of(l1Expression, rightSide));

        Optional<WarpExpression> result = expressionService.convertToWarpExpression(connectorSession, l1Expression, assignments, customStats);
        assertThat(result).isNotEmpty();
        VaradaStatsPushdownPredicates pushdownPredicatesStats = (VaradaStatsPushdownPredicates) metricsManager.get(PUSHDOWN_PREDICATES_STAT_GROUP);
        assertThat(pushdownPredicatesStats.getunsupported_expression_depth()).isEqualTo(0);

        result = expressionService.convertToWarpExpression(connectorSession, l0Expression, assignments, customStats);
        assertThat(result).isEmpty();
        assertThat(pushdownPredicatesStats.getunsupported_expression_depth()).isEqualTo(1);
    }

    @Test
    public void testUnsupportAllExpression()
    {
        globalConfiguration.setUnsupportedFunctions("*");

        Call expression = new Call(
                BooleanType.BOOLEAN,
                IS_NAN,
                List.of(doubleVariable1));
        Optional<WarpExpression> actual = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats);
        assertThat(actual).isEmpty();
    }

    /**
     * where cast(s_real as varchar(20)) = '5'
     */
    @Test
    public void testCastRealToVarchar()
    {
        Call castCall = new Call(VarcharType.createVarcharType(20), CAST_FUNCTION_NAME, List.of(realVariable));
        Slice slice = RealOperators.castToVarchar(20, 5L);
        Call expression = new Call(
                BOOLEAN,
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                List.of(castCall, new Constant(slice, VarcharType.VARCHAR)));
        ColumnHandle realColumn = assignments.get(realVariable.getName());
        VaradaCall expectedCastCall = new VaradaCall(CAST_FUNCTION_NAME.getName(), List.of(new VaradaVariable(realColumn, realVariable.getType())), VarcharType.createVarcharType(20));
        VaradaCall expectedResult = new VaradaCall(EQUAL_OPERATOR_FUNCTION_NAME.getName(),
                List.of(expectedCastCall, new VaradaSliceConstant(slice, VarcharType.VARCHAR)),
                BOOLEAN);
        List<VaradaExpressionData> result = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats).orElseThrow().varadaExpressionDataLeaves();
        assertThat(result.get(0).getExpression()).isEqualTo(expectedResult);
        assertPushdownStatsSum(0);
    }

    /**
     * where cast(doubleVariable1 as real) > 10
     */
    @Test
    public void testCastDoubleToReal()
    {
        ColumnHandle realColumn = assignments.get(realVariable.getName());
        Call castCall = new Call(RealType.REAL, CAST_FUNCTION_NAME, List.of(realVariable));
        Call varadaExpression = new Call(BooleanType.BOOLEAN,
                GREATER_THAN_OPERATOR_FUNCTION_NAME,
                List.of(castCall, new Constant(10L, RealType.REAL)));
        VaradaCall expectedCastCall = new VaradaCall(CAST_FUNCTION_NAME.getName(), List.of(new VaradaVariable(realColumn, realVariable.getType())), RealType.REAL);
        VaradaCall expectedResult = new VaradaCall(GREATER_THAN_OPERATOR_FUNCTION_NAME.getName(),
                List.of(expectedCastCall, new VaradaPrimitiveConstant(10L, RealType.REAL)),
                BOOLEAN);
        List<VaradaExpressionData> result = expressionService.convertToWarpExpression(connectorSession, varadaExpression, assignments, customStats).orElseThrow().varadaExpressionDataLeaves();
        assertThat(result.get(0).getExpression()).isEqualTo(expectedResult);
        assertPushdownStatsSum(0);
    }

    /**
     * where cast(doubleVariable1 as real) > 10 and cast(doubleVariable1 as real) < 100
     */
    @Test
    public void testCastDoubleToRealWithAnd()
    {
        Call castCall = new Call(RealType.REAL, CAST_FUNCTION_NAME, List.of(realVariable));
        Call leftSide = new Call(BooleanType.BOOLEAN,
                GREATER_THAN_OPERATOR_FUNCTION_NAME,
                List.of(castCall, new Constant(10L, RealType.REAL)));
        Call rightSide = new Call(BooleanType.BOOLEAN,
                LESS_THAN_OPERATOR_FUNCTION_NAME,
                List.of(castCall, new Constant(100L, RealType.REAL)));
        Call expression = new Call(BOOLEAN, AND_FUNCTION_NAME, List.of(leftSide, rightSide));

        Domain domain = Domain.create(ValueSet.ofRanges(Range.greaterThan(RealType.REAL, 10L)), false);
        List<VaradaExpressionData> result = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats).orElseThrow().varadaExpressionDataLeaves();
        Optional<NativeExpression> expectedResult = Optional.of(new NativeExpression(PredicateType.PREDICATE_TYPE_RANGES,
                FunctionType.FUNCTION_TYPE_CAST,
                domain,
                false,
                false,
                List.of(RecTypeCode.REC_TYPE_REAL.ordinal()), TransformFunction.NONE));
        assertThat(result.get(0).getNativeExpressionOptional()).isEqualTo(expectedResult);
        assertPushdownStatsSum(0);
    }

    /**
     * The same double can be represented in different formats.
     * For example, 0.01111111111111111111E0 == 1.1111111111111112E-2
     * In Trino: cast(0.01111111111111111111E0 as varchar) = '1.1111111111111112E-2' is True
     * cast(0.01111111111111111111E0 as varchar) = '01111111111111111111E0' is False
     */
    @ParameterizedTest
    @MethodSource("nonDefaultFormatDoubleParams")
    public void testCastDoubleToVarcharNonDefaultFormat(String doubleAsString)
    {
        Call castCall = new Call(
                VarcharType.VARCHAR,
                CAST_FUNCTION_NAME,
                List.of(doubleVariable1));
        Constant nonDefaultFormatConstant = new Constant(
                Slices.utf8Slice(doubleAsString),
                VarcharType.VARCHAR);
        Call varadaExpression = new Call(
                BooleanType.BOOLEAN,
                EQUAL_OPERATOR_FUNCTION_NAME,
                List.of(castCall, nonDefaultFormatConstant));
        NativeExpression expectedResult = NativeExpression.builder()
                .predicateType(PredicateType.PREDICATE_TYPE_NONE)
                .functionType(FunctionType.FUNCTION_TYPE_NONE)
                .domain(Domain.none(DoubleType.DOUBLE))
                .collectNulls(false)
                .build();
        List<VaradaExpressionData> result = expressionService.convertToWarpExpression(connectorSession, varadaExpression, assignments, customStats).orElseThrow().varadaExpressionDataLeaves();
        assertPushdownStatsSum(0);
        assertThat(result.get(0).getNativeExpressionOptional().orElseThrow()).isEqualTo(expectedResult);
    }

    /**
     * exception during rewrite
     */
    @Test
    public void failedRewriteExpression()
    {
        Call castCall = new Call(VarcharType.createVarcharType(20), CAST_FUNCTION_NAME, List.of(realVariable));
        Slice slice = RealOperators.castToVarchar(20, 5L);
        Call expression = new Call(
                BOOLEAN,
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                List.of(castCall, new Constant(slice, VarcharType.VARCHAR)));
        Map<String, ColumnHandle> invalidAssignment = Collections.emptyMap();
        Optional<WarpExpression> result = expressionService.convertToWarpExpression(connectorSession, expression, invalidAssignment, customStats);
        assertThat(result).isEmpty();
        assertPushdownStatsSum(1);
        VaradaStatsPushdownPredicates pushdownPredicatesStats = (VaradaStatsPushdownPredicates) metricsManager.get(PUSHDOWN_PREDICATES_STAT_GROUP);
        assertThat(pushdownPredicatesStats.getfailed_rewrite_expression()).isEqualTo(1);
    }

    @ParameterizedTest
    @MethodSource("arrayTypes")
    public void testContainsOnArrayColumn(ArrayType arrayType, boolean expectedIsValid)
    {
        TestingConnectorColumnHandle columnHandle = new TestingConnectorColumnHandle(arrayType, "arr_column");
        Slice slice = Slices.utf8Slice("test");
        Call expression = new Call(
                BOOLEAN,
                CONTAINS,
                List.of(new Variable(columnHandle.name(), columnHandle.type()),
                        new Constant(slice, arrayType.getElementType())));
        assignments.put(columnHandle.name(), columnHandle);

        List<VaradaExpressionData> expectedResult;
        if (expectedIsValid) {
            VaradaExpression expectedVaradaExpression = new VaradaCall(CONTAINS.getName(),
                    List.of(new VaradaVariable(columnHandle, columnHandle.type()),
                            new VaradaSliceConstant(slice, arrayType.getElementType())),
                    BOOLEAN);
            RegularColumn regularColumn = new RegularColumn(columnHandle.name());
            expectedResult = List.of(new VaradaExpressionData(expectedVaradaExpression,
                    columnHandle.type(),
                    false,
                    Optional.empty(),
                    regularColumn));
        }
        else {
            expectedResult = List.of();
        }
        Optional<WarpExpression> actual = expressionService.convertToWarpExpression(connectorSession, expression, assignments, customStats);
        if (expectedResult.isEmpty()) {
            assertThat(actual).isEmpty();
        }
        else {
            assertThat(actual.orElseThrow().varadaExpressionDataLeaves()).isEqualTo(expectedResult);
        }
    }

    /**
     * select * from functionsTable where ceil(double1) = 5  or ceil(double1) = 9
     * translated by Trino into:
     * Call[functionName=name='$in',
     * arguments=[Call[functionName=name='ceil', arguments=[double1::double]],
     * Call[functionName=name='$array', arguments=[5.0::double, 9.0::double]]]]
     */
    @Test
    public void testInFunction()
    {
        Call ceilCall = new Call(DoubleType.DOUBLE,
                CEIL,
                List.of(doubleVariable1));

        ArrayType arrayType = new ArrayType(DoubleType.DOUBLE);
        Call arrayCall = new Call(arrayType,
                ARRAY_CONSTRUCTOR_FUNCTION_NAME,
                List.of(new Constant(5D, DoubleType.DOUBLE), new Constant(9D, DoubleType.DOUBLE)));
        Call callExpression = new Call(BooleanType.BOOLEAN,
                IN_PREDICATE_FUNCTION_NAME,
                List.of(ceilCall, arrayCall));

        Type doubleType = doubleVariable1.getType();
        VaradaCall expectedCeilCall = new VaradaCall(CEIL.getName(),
                List.of(new VaradaVariable(assignments.get(doubleVariable1.getName()), doubleType)),
                doubleType);

        VaradaCall expectedArrayCall = new VaradaCall(ARRAY_CONSTRUCTOR_FUNCTION_NAME.getName(),
                List.of(new VaradaPrimitiveConstant(5D, DoubleType.DOUBLE), new VaradaPrimitiveConstant(9D, DoubleType.DOUBLE)),
                arrayType);
        VaradaExpression expectedExpression = new VaradaCall(IN_PREDICATE_FUNCTION_NAME.getName(),
                List.of(expectedCeilCall, expectedArrayCall),
                BOOLEAN);
        NativeExpression expectedNativeExpression = NativeExpression
                .builder()
                .domain(Domain.create(ValueSet.ofRanges(Range.equal(doubleType, 5D), Range.equal(doubleType, 9D)), false))
                .predicateType(PredicateType.PREDICATE_TYPE_VALUES)
                .functionType(FunctionType.FUNCTION_TYPE_CEIL)
                .collectNulls(false)
                .functionParams(Collections.emptyList())
                .build();

        List<VaradaExpressionData> expectedResult = List.of(new VaradaExpressionData(expectedExpression,
                doubleType,
                false,
                Optional.of(expectedNativeExpression),
                new RegularColumn(doubleVariable1.getName())));

        List<VaradaExpressionData> actual = expressionService.convertToWarpExpression(connectorSession, callExpression, assignments, customStats).orElseThrow().varadaExpressionDataLeaves();
        assertThat(actual).isEqualTo(expectedResult);
    }

    private Map<String, ColumnHandle> createAssignments(Variable... variables)
    {
        Map<String, ColumnHandle> assignments = new HashMap<>();
        for (Variable variable : variables) {
            assignments.put(variable.getName(), new TestingConnectorColumnHandle(variable.getType(), variable.getName()));
        }
        return assignments;
    }

    private VaradaVariable createExpectedVariable(Variable variable)
    {
        return new VaradaVariable(assignments.get(variable.getName()), variable.getType());
    }

    private Pair<Call, VaradaCall> createCallExpression(FunctionName functionName,
            FunctionName operator,
            Variable variable,
            Constant operatorValue)
    {
        VaradaConstant varadaConstant = convertConstantToVaradaConstant(operatorValue);
        VaradaCall varadaCall = new VaradaCall(operator.getName(),
                List.of(new VaradaCall(functionName.getName(),
                                List.of(createExpectedVariable(variable)), operatorValue.getType()),
                        varadaConstant),
                BOOLEAN);
        Call call = new Call(
                BOOLEAN,
                operator,
                List.of(
                        new Call(
                                operatorValue.getType(),
                                functionName,
                                List.of(variable)),
                        operatorValue));
        return Pair.of(call, varadaCall);
    }

    private Pair<Call, VaradaCall> createCallExpression(FunctionName operator,
            Variable variable,
            Constant operatorValue)
    {
        VaradaConstant varadaConstant = convertConstantToVaradaConstant(operatorValue);
        VaradaCall varadaCall = new VaradaCall(operator.getName(),
                List.of(createExpectedVariable(variable), varadaConstant),
                BOOLEAN);
        Call call = new Call(
                BOOLEAN,
                operator,
                List.of(variable, operatorValue));
        return Pair.of(call, varadaCall);
    }

    private VaradaConstant convertConstantToVaradaConstant(Constant operatorValue)
    {
        VaradaConstant varadaConstant;
        if (operatorValue.getValue() instanceof Slice) {
            varadaConstant = new VaradaSliceConstant((Slice) operatorValue.getValue(), operatorValue.getType());
        }
        else {
            varadaConstant = new VaradaPrimitiveConstant(operatorValue.getValue(), operatorValue.getType());
        }
        return varadaConstant;
    }

    private Pair<Call, VaradaCall> createModVaradaCall(FunctionName operator, Variable variable, Constant modeValue, Constant operatorValue)
    {
        assertThat(variable.getType()).isEqualTo(modeValue.getType()).isEqualTo(operatorValue.getType());
        VaradaCall varadaCall = new VaradaCall(operator.getName(),
                List.of(new VaradaCall(MOD.getName(),
                                List.of(createExpectedVariable(variable),
                                        new VaradaPrimitiveConstant(modeValue.getValue(), modeValue.getType())),
                                modeValue.getType()),
                        new VaradaPrimitiveConstant(operatorValue.getValue(), operatorValue.getType())),
                BOOLEAN);
        Call call = new Call(
                BOOLEAN,
                operator,
                List.of(
                        new Call(
                                modeValue.getType(),
                                MOD,
                                List.of(variable, modeValue)),
                        operatorValue));
        return Pair.of(call, varadaCall);
    }

    private void assertPushdownStatsSum(int expectedCount)
    {
        VaradaStatsPushdownPredicates pushdownPredicatesStats = (VaradaStatsPushdownPredicates) metricsManager.get(PUSHDOWN_PREDICATES_STAT_GROUP);
        assertThat(pushdownPredicatesStats.getCounters().values().stream().mapToLong(LongAdder::longValue).sum()).isEqualTo(expectedCount);
    }
}
