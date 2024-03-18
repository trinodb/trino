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
package io.trino.plugin.varada.expression.rewrite.coordinator.varadatonative;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.varada.TestingTxService;
import io.trino.plugin.varada.connector.TestingConnectorColumnHandle;
import io.trino.plugin.varada.expression.NativeExpression;
import io.trino.plugin.varada.expression.TransformFunction;
import io.trino.plugin.varada.expression.VaradaCall;
import io.trino.plugin.varada.expression.VaradaExpression;
import io.trino.plugin.varada.expression.VaradaPrimitiveConstant;
import io.trino.plugin.varada.expression.VaradaSliceConstant;
import io.trino.plugin.varada.expression.VaradaVariable;
import io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.storage.engine.StubsStorageEngineConstants;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.stats.VaradaStatsPushdownPredicates;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;
import io.trino.type.RealOperators;
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

import static io.trino.plugin.varada.expression.rewrite.ExpressionService.PUSHDOWN_PREDICATES_STAT_GROUP;
import static io.trino.plugin.varada.expression.rewrite.coordinator.connectortovarada.SupportedFunctions.ELEMENT_AT;
import static io.trino.plugin.warp.gen.constants.PredicateType.PREDICATE_TYPE_RANGES;
import static io.trino.plugin.warp.gen.constants.PredicateType.PREDICATE_TYPE_STRING_VALUES;
import static io.trino.plugin.warp.gen.constants.PredicateType.PREDICATE_TYPE_VALUES;
import static io.trino.spi.expression.StandardFunctions.CAST_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.lang.Float.floatToIntBits;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class NativeExpressionRulesHandlerTest
{
    public static final FunctionName CEIL = new FunctionName("ceiling");
    public static final FunctionName IS_NAN = new FunctionName("is_nan");

    private final Variable doubleVariable1 = new Variable("double1", DoubleType.DOUBLE);
    private final Variable doubleVariable2 = new Variable("double2", DoubleType.DOUBLE);
    private final Variable realVariable = new Variable("real1", RealType.REAL);
    private final VarcharType varcharType = VarcharType.createVarcharType(10);
    private final Variable varcharVariable = new Variable("varchar1", varcharType);

    private Map<String, ColumnHandle> assignments;
    private NativeExpressionRulesHandler nativeExpressionRulesHandler;
    private MetricsManager metricsManager;
    private Map<String, Long> customStats;

    static Stream<Arguments> ceilWithOperators()
    {
        return Stream.of(
                arguments(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                        Range.equal(DoubleType.DOUBLE, 5D),
                        PREDICATE_TYPE_VALUES),
                arguments(LESS_THAN_OPERATOR_FUNCTION_NAME,
                        Range.lessThan(DoubleType.DOUBLE, 5D),
                        PredicateType.PREDICATE_TYPE_RANGES),
                arguments(LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME,
                        Range.lessThanOrEqual(DoubleType.DOUBLE, 5D),
                        PredicateType.PREDICATE_TYPE_RANGES),
                arguments(StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME,
                        Range.greaterThan(DoubleType.DOUBLE, 5D),
                        PredicateType.PREDICATE_TYPE_RANGES),
                arguments(GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME,
                        Range.greaterThanOrEqual(DoubleType.DOUBLE, 5D),
                        PredicateType.PREDICATE_TYPE_RANGES));
    }

    static Stream<Arguments> rangeOperatorParamsUnsupported()
    {
        return Stream.of(
                arguments(LESS_THAN_OPERATOR_FUNCTION_NAME),
                arguments(LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME),
                arguments(GREATER_THAN_OPERATOR_FUNCTION_NAME),
                arguments(GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME));
    }

    static Stream<Arguments> equalOperatorParamsSupported()
    {
        return Stream.of(
                arguments(VarcharType.VARCHAR),
                arguments(VarcharType.createVarcharType(20)),
                arguments(VarcharType.createVarcharType(10)));
    }

    static Stream<Arguments> castDoubleToReal()
    {
        Type type = RealType.REAL;
        long value = 5L;
        return Stream.of(
                arguments(EQUAL_OPERATOR_FUNCTION_NAME, Domain.singleValue(type, value)),
                arguments(LESS_THAN_OPERATOR_FUNCTION_NAME, Domain.create(ValueSet.ofRanges(Range.lessThan(type, value)), false)),
                arguments(LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(type, value)), false)),
                arguments(GREATER_THAN_OPERATOR_FUNCTION_NAME, Domain.create(ValueSet.ofRanges(Range.greaterThan(type, value)), false)),
                arguments(GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, value)), false)));
    }

    public static Map<String, ColumnHandle> createAssignments(Variable... variables)
    {
        Map<String, ColumnHandle> assignments = new HashMap<>();
        for (Variable variable : variables) {
            TestingConnectorColumnHandle columnHandle = new TestingConnectorColumnHandle(variable.getType(), variable.getName());
            assignments.put(variable.getName(), columnHandle);
        }
        return assignments;
    }

    @BeforeEach
    public void beforeEach()
    {
        StorageEngineConstants storageEngineConstants = new StubsStorageEngineConstants();
        metricsManager = TestingTxService.createMetricsManager();
        nativeExpressionRulesHandler = new NativeExpressionRulesHandler(storageEngineConstants, metricsManager);
        assignments = createAssignments(doubleVariable1, doubleVariable2, varcharVariable, realVariable);
        customStats = new HashMap<>();
    }

    @Test
    public void failedRewriteExpression()
    {
        VaradaExpression invalidVaradaExpression = new VaradaCall(CEIL.getName(), List.of(createExpectedVariable(doubleVariable1)), BOOLEAN);
        Optional<NativeExpression> result = nativeExpressionRulesHandler.rewrite(invalidVaradaExpression, doubleVariable1.getType(), Collections.emptySet(), customStats);
        assertThat(result).isEqualTo(Optional.empty());
        assertPushdownStatsSum(1);
        VaradaStatsPushdownPredicates pushdownPredicatesStats = (VaradaStatsPushdownPredicates) metricsManager.get(PUSHDOWN_PREDICATES_STAT_GROUP);
        assertThat(pushdownPredicatesStats.getfailed_rewrite_to_native_expression()).isEqualTo(1);
    }

    /**
     * is_nan(double1) = false
     */
    @Test
    public void testVaradaExpressionIsNanWithEqual()
    {
        VaradaExpression varadaExpression = new VaradaCall(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME.getName(),
                List.of(new VaradaCall(IS_NAN.getName(),
                                List.of(createExpectedVariable(doubleVariable1)), BOOLEAN),
                        VaradaPrimitiveConstant.FALSE), BOOLEAN);
        Optional<NativeExpression> result = nativeExpressionRulesHandler.rewrite(varadaExpression, doubleVariable1.getType(), Collections.emptySet(), customStats);
        assertThat(result).isEqualTo(Optional.empty());
        assertPushdownStatsSum(1);
        VaradaStatsPushdownPredicates pushdownPredicatesStats = (VaradaStatsPushdownPredicates) metricsManager.get(PUSHDOWN_PREDICATES_STAT_GROUP);
        assertThat(pushdownPredicatesStats.getunsupported_functions_native()).isEqualTo(1);
    }

    /**
     * is_nan(double1)
     */
    @Test
    public void testVaradaExpressionIsNan()
    {
        VaradaExpression varadaExpression = new VaradaCall(IS_NAN.getName(), List.of(createExpectedVariable(doubleVariable1)), BOOLEAN);
        Range range = Range.equal(BOOLEAN, true);
        Domain domain = Domain.create(ValueSet.ofRanges(range), false);
        NativeExpression expectedResult = new NativeExpression(PREDICATE_TYPE_VALUES,
                FunctionType.FUNCTION_TYPE_IS_NAN,
                domain,
                false,
                false,
                Collections.emptyList(), TransformFunction.NONE);
        Optional<NativeExpression> result = nativeExpressionRulesHandler.rewrite(varadaExpression, doubleVariable1.getType(), Collections.emptySet(), customStats);
        assertThat(result).isEqualTo(Optional.of(expectedResult));
        assertPushdownStatsSum(0);
    }

    /**
     * element_at(mapColumn, 'key1') = 'val1'
     */
    @Test
    public void testMapElementAtExpression()
    {
        MapType mapType = new MapType(VarcharType.VARCHAR, VarcharType.VARCHAR, new TypeOperators());
        TestingConnectorColumnHandle columnHandle = new TestingConnectorColumnHandle(mapType, "mapColumn");
        VaradaVariable varadaVariable = new VaradaVariable(columnHandle, mapType);
        VaradaSliceConstant sliceConstant = new VaradaSliceConstant(Slices.utf8Slice("val1"), VarcharType.VARCHAR);
        VaradaExpression varadaExpression = new VaradaCall(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME.getName(),
                List.of(new VaradaCall(ELEMENT_AT.getName(),
                                List.of(varadaVariable, new VaradaSliceConstant(Slices.utf8Slice("key1"), mapType)),
                                BOOLEAN),
                        sliceConstant),
                BOOLEAN);
        NativeExpression expectedResult = new NativeExpression(PREDICATE_TYPE_STRING_VALUES,
                FunctionType.FUNCTION_TYPE_TRANSFORMED,
                Domain.create(ValueSet.ofRanges(Range.equal(sliceConstant.getType(), sliceConstant.getValue())), false),
                false,
                true,
                Collections.emptyList(),
                new TransformFunction(TransformFunction.TransformType.ELEMENT_AT,
                        List.of(new VaradaSliceConstant(Slices.utf8Slice("key1"), mapType))));
        Optional<NativeExpression> result = nativeExpressionRulesHandler.rewrite(varadaExpression, mapType, Collections.emptySet(), customStats);
        assertThat(result.orElseThrow()).isEqualTo(expectedResult);
    }

    /**
     * json_extract_scalar(varcharCol, '$.number') = '12.345678'
     */
    @Test
    void testJsonExtractScalarExpression()
    {
        TestingConnectorColumnHandle columnHandle = new TestingConnectorColumnHandle(VarcharType.VARCHAR, "varcharCol");
        VaradaVariable varadaVariable = new VaradaVariable(columnHandle, VarcharType.VARCHAR);
        VaradaSliceConstant sliceConstant = new VaradaSliceConstant(Slices.utf8Slice("12.345678"), VarcharType.VARCHAR);
        VaradaExpression varadaExpression = new VaradaCall(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME.getName(),
                List.of(new VaradaCall(SupportedFunctions.JSON_EXTRACT_SCALAR.getName(),
                                List.of(varadaVariable, new VaradaSliceConstant(Slices.utf8Slice("$.number"), VarcharType.VARCHAR)),
                                BOOLEAN),
                        sliceConstant),
                BOOLEAN);
        NativeExpression expectedResult = new NativeExpression(PREDICATE_TYPE_STRING_VALUES,
                FunctionType.FUNCTION_TYPE_TRANSFORMED,
                Domain.create(ValueSet.ofRanges(Range.equal(sliceConstant.getType(), sliceConstant.getValue())), false),
                false,
                true,
                Collections.emptyList(),
                new TransformFunction(TransformFunction.TransformType.JSON_EXTRACT_SCALAR,
                        List.of(new VaradaPrimitiveConstant("$.number", VarcharType.VARCHAR))));
        Optional<NativeExpression> result = nativeExpressionRulesHandler.rewrite(varadaExpression, VarcharType.VARCHAR, Collections.emptySet(), customStats);
        assertThat(result.orElseThrow()).isEqualTo(expectedResult);
    }

    /**
     * unsupported(double1) = false
     */
    @Test
    public void testUnsupportedExpression()
    {
        VaradaExpression varadaExpression = new VaradaCall(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME.getName(),
                List.of(new VaradaCall("unsupported",
                                List.of(createExpectedVariable(doubleVariable1)), BOOLEAN),
                        VaradaPrimitiveConstant.FALSE), BOOLEAN);
        Optional<NativeExpression> result = nativeExpressionRulesHandler.rewrite(varadaExpression, doubleVariable1.getType(), Collections.emptySet(), customStats);
        assertThat(result).isEmpty();
        assertPushdownStatsSum(1);
        VaradaStatsPushdownPredicates pushdownPredicatesStats = (VaradaStatsPushdownPredicates) metricsManager.get(PUSHDOWN_PREDICATES_STAT_GROUP);
        assertThat(pushdownPredicatesStats.getunsupported_functions_native()).isEqualTo(1);
    }

    @ParameterizedTest
    @MethodSource("ceilWithOperators")
    public void testCeilWithOperators(FunctionName functionName,
            Range expectedRange,
            PredicateType expectedPredicateType)
    {
        VaradaExpression varadaExpression = new VaradaCall(functionName.getName(),
                List.of(new VaradaCall(CEIL.getName(),
                                List.of(createExpectedVariable(doubleVariable1)),
                                BOOLEAN),
                        new VaradaPrimitiveConstant(5D, DoubleType.DOUBLE)), BOOLEAN);
        ValueSet valueSet = ValueSet.ofRanges(expectedRange);
        Domain domain = Domain.create(valueSet, false);
        NativeExpression expectedResult = new NativeExpression(expectedPredicateType,
                FunctionType.FUNCTION_TYPE_CEIL,
                domain,
                false,
                false,
                Collections.emptyList(), TransformFunction.NONE);
        Optional<NativeExpression> result = nativeExpressionRulesHandler.rewrite(varadaExpression, doubleVariable1.getType(), Collections.emptySet(), customStats);
        assertThat(result).isEqualTo(Optional.of(expectedResult));
        assertPushdownStatsSum(0);
    }

    /**
     * (ceil(double1) > 5) = false
     */
    @Test
    public void testDomainAlreadySet()
    {
        VaradaExpression varadaExpression = new VaradaCall(EQUAL_OPERATOR_FUNCTION_NAME.getName(),
                List.of(new VaradaCall(GREATER_THAN_OPERATOR_FUNCTION_NAME.getName(),
                                List.of(new VaradaCall(CEIL.getName(),
                                                List.of(createExpectedVariable(doubleVariable1)), BOOLEAN),
                                        new VaradaPrimitiveConstant(5L, IntegerType.INTEGER)), BOOLEAN),
                        new VaradaPrimitiveConstant(false, BOOLEAN)), BOOLEAN);
        Optional<NativeExpression> result = nativeExpressionRulesHandler.rewrite(varadaExpression, doubleVariable1.getType(), Collections.emptySet(), customStats);
        assertThat(result).isEmpty();
        VaradaStatsPushdownPredicates pushdownPredicatesStats = (VaradaStatsPushdownPredicates) metricsManager.get(PUSHDOWN_PREDICATES_STAT_GROUP);
        assertPushdownStatsSum(1);
        assertThat(pushdownPredicatesStats.getunsupported_functions_native()).isEqualTo(1);
    }

    /**
     * where cast(s_real as varchar) rangesOperator '5'
     */
    @ParameterizedTest
    @MethodSource("rangeOperatorParamsUnsupported")
    public void testCastRealToVarchar_unsupported(FunctionName operator)
    {
        String stringValue = "5";
        Slice slice = Slices.utf8Slice(stringValue);
        Variable columnType = realVariable;
        ColumnHandle realColumn = assignments.get(columnType.getName());
        VaradaCall expectedCastCall = new VaradaCall(CAST_FUNCTION_NAME.getName(),
                List.of(new VaradaVariable(realColumn, columnType.getType())),
                VarcharType.VARCHAR);
        VaradaCall varadaExpression = new VaradaCall(operator.getName(),
                List.of(expectedCastCall, new VaradaSliceConstant(slice, VarcharType.VARCHAR)),
                BOOLEAN);

        Optional<NativeExpression> result = nativeExpressionRulesHandler.rewrite(varadaExpression, columnType.getType(), Collections.emptySet(), customStats);
        assertThat(result).isEmpty();
        assertPushdownStatsSum(1);
        VaradaStatsPushdownPredicates pushdownPredicatesStats = (VaradaStatsPushdownPredicates) metricsManager.get(PUSHDOWN_PREDICATES_STAT_GROUP);
        assertThat(pushdownPredicatesStats.getunsupported_functions_native()).isEqualTo(1);
    }

    /**
     * where cast(s_real as varchar) = '2.6248186293E2'
     */
    @ParameterizedTest
    @MethodSource("equalOperatorParamsSupported")
    public void testCastRealToVarcharSupported(VarcharType varcharType)
    {
        float value = 2.624819E2f;
        long intBits = floatToIntBits(value);
        Slice slice = RealOperators.castToVarchar(varcharType.getLength().orElse(VarcharType.UNBOUNDED_LENGTH), intBits);
        Variable columnType = realVariable;
        ColumnHandle realColumn = assignments.get(columnType.getName());
        VaradaCall castCall = new VaradaCall(CAST_FUNCTION_NAME.getName(),
                List.of(new VaradaVariable(realColumn, columnType.getType())),
                varcharType);
        VaradaCall varadaExpression = new VaradaCall(EQUAL_OPERATOR_FUNCTION_NAME.getName(),
                List.of(castCall, new VaradaSliceConstant(slice, varcharType)),
                BOOLEAN);

        Domain domain = Domain.singleValue(RealType.REAL, intBits);

        NativeExpression expectedResult = new NativeExpression(PREDICATE_TYPE_VALUES,
                FunctionType.FUNCTION_TYPE_NONE,
                domain,
                false,
                false,
                Collections.emptyList(), TransformFunction.NONE);

        Optional<NativeExpression> result = nativeExpressionRulesHandler.rewrite(varadaExpression, columnType.getType(), Collections.emptySet(), customStats);
        assertThat(result).isEqualTo(Optional.of(expectedResult));
        assertPushdownStatsSum(0);
    }

    /**
     * * where cast(doubleVariable1 as real) operator 5
     */
    @ParameterizedTest
    @MethodSource("castDoubleToReal")
    public void testCastDoubleToReal(FunctionName functionName, Domain domain)
    {
        ColumnHandle doubleColumn = assignments.get(doubleVariable1.getName());
        VaradaCall expectedCastCall = new VaradaCall(CAST_FUNCTION_NAME.getName(),
                List.of(new VaradaVariable(doubleColumn, doubleVariable1.getType())),
                RealType.REAL);
        VaradaCall varadaExpression = new VaradaCall(functionName.getName(),
                List.of(expectedCastCall, new VaradaPrimitiveConstant(5L, RealType.REAL)),
                BOOLEAN);

        PredicateType expectedPredicateType;
        if (domain.isSingleValue()) {
            expectedPredicateType = PREDICATE_TYPE_VALUES;
        }
        else {
            expectedPredicateType = PREDICATE_TYPE_RANGES;
        }
        NativeExpression expectedResult = new NativeExpression(expectedPredicateType,
                FunctionType.FUNCTION_TYPE_CAST,
                domain,
                false,
                false,
                List.of(RecTypeCode.REC_TYPE_REAL.ordinal()), TransformFunction.NONE);
        Optional<NativeExpression> result = nativeExpressionRulesHandler.rewrite(varadaExpression, doubleVariable1.getType(), Collections.emptySet(), customStats);
        assertThat(result).isEqualTo(Optional.of(expectedResult));
        assertPushdownStatsSum(0);
    }

    private VaradaVariable createExpectedVariable(Variable variable)
    {
        return new VaradaVariable(assignments.get(variable.getName()), variable.getType());
    }

    private void assertPushdownStatsSum(int expectedCount)
    {
        VaradaStatsPushdownPredicates pushdownPredicatesStats = (VaradaStatsPushdownPredicates) metricsManager.get(PUSHDOWN_PREDICATES_STAT_GROUP);
        assertThat(pushdownPredicatesStats.getCounters().values().stream().mapToLong(LongAdder::longValue).sum()).isEqualTo(expectedCount);
    }
}
