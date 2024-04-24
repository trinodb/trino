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
package io.trino.sql.planner.plan;

import com.fasterxml.jackson.databind.KeyDeserializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.block.BlockJsonSerde;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.block.Block;
import io.trino.spi.block.TestingBlockEncodingSerde;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolKeyDeserializer;
import io.trino.sql.planner.plan.PatternRecognitionNode.Measure;
import io.trino.sql.planner.plan.WindowNode.Frame;
import io.trino.sql.planner.plan.WindowNode.Function;
import io.trino.sql.planner.rowpattern.AggregatedSetDescriptor;
import io.trino.sql.planner.rowpattern.AggregationValuePointer;
import io.trino.sql.planner.rowpattern.ClassifierValuePointer;
import io.trino.sql.planner.rowpattern.ExpressionAndValuePointers;
import io.trino.sql.planner.rowpattern.LogicalIndexPointer;
import io.trino.sql.planner.rowpattern.MatchNumberValuePointer;
import io.trino.sql.planner.rowpattern.ScalarValuePointer;
import io.trino.sql.planner.rowpattern.ValuePointer;
import io.trino.sql.planner.rowpattern.ir.IrConcatenation;
import io.trino.sql.planner.rowpattern.ir.IrLabel;
import io.trino.type.TypeDeserializer;
import io.trino.type.TypeSignatureKeyDeserializer;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.IrExpressions.ifExpression;
import static io.trino.sql.planner.plan.FrameBoundType.CURRENT_ROW;
import static io.trino.sql.planner.plan.FrameBoundType.UNBOUNDED_FOLLOWING;
import static io.trino.sql.planner.plan.RowsPerMatch.WINDOW;
import static io.trino.sql.planner.plan.SkipToPosition.LAST;
import static io.trino.sql.planner.plan.WindowFrameType.ROWS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPatternRecognitionNodeSerialization
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction RANDOM = FUNCTIONS.resolveFunction("random", fromTypes());
    private static final ResolvedFunction NEGATION_BIGINT = FUNCTIONS.resolveOperator(OperatorType.NEGATION, ImmutableList.of(BIGINT));
    private static final ResolvedFunction NEGATION_INTEGER = FUNCTIONS.resolveOperator(OperatorType.NEGATION, ImmutableList.of(INTEGER));

    private static final JsonCodec<ValuePointer> VALUE_POINTER_CODEC;
    private static final JsonCodec<ExpressionAndValuePointers> EXPRESSION_AND_VALUE_POINTERS_CODEC;
    private static final JsonCodec<Measure> MEASURE_CODEC;
    private static final JsonCodec<PatternRecognitionNode> PATTERN_RECOGNITION_NODE_CODEC;

    static {
        ObjectMapperProvider provider = new ObjectMapperProvider();
        provider.setKeyDeserializers(ImmutableMap.<Class<?>, KeyDeserializer>builder()
                .put(TypeSignature.class, new TypeSignatureKeyDeserializer())
                .put(Symbol.class, new SymbolKeyDeserializer(new TestingTypeManager()))
                .buildOrThrow());

        provider.setJsonDeserializers(ImmutableMap.of(
                Type.class, new TypeDeserializer(new TestingTypeManager()),
                Block.class, new BlockJsonSerde.Deserializer(new TestingBlockEncodingSerde())));

        provider.setJsonSerializers(ImmutableMap.of(
                Block.class, new BlockJsonSerde.Serializer(new TestingBlockEncodingSerde())));

        VALUE_POINTER_CODEC = new JsonCodecFactory(provider).jsonCodec(ValuePointer.class);
        EXPRESSION_AND_VALUE_POINTERS_CODEC = new JsonCodecFactory(provider).jsonCodec(ExpressionAndValuePointers.class);
        MEASURE_CODEC = new JsonCodecFactory(provider).jsonCodec(Measure.class);
        PATTERN_RECOGNITION_NODE_CODEC = new JsonCodecFactory(provider).jsonCodec(PatternRecognitionNode.class);
    }

    @Test
    public void testScalarValuePointerRoundtrip()
    {
        assertJsonRoundTrip(VALUE_POINTER_CODEC, new ScalarValuePointer(
                new LogicalIndexPointer(ImmutableSet.of(), false, false, 5, 5),
                new Symbol(BIGINT, "input_symbol")));

        assertJsonRoundTrip(VALUE_POINTER_CODEC, new ScalarValuePointer(
                new LogicalIndexPointer(ImmutableSet.of(new IrLabel("A"), new IrLabel("B")), true, true, 1, -1),
                new Symbol(BIGINT, "input_symbol")));
    }

    @Test
    public void testAggregationValuePointerRoundtrip()
    {
        ResolvedFunction countFunction = createTestMetadataManager().resolveBuiltinFunction("count", ImmutableList.of());
        assertJsonRoundTrip(VALUE_POINTER_CODEC, new AggregationValuePointer(
                countFunction,
                new AggregatedSetDescriptor(ImmutableSet.of(), false),
                ImmutableList.of(),
                Optional.of(new Symbol(VARCHAR, "classifier")),
                Optional.of(new Symbol(BIGINT, "match_number"))));

        ResolvedFunction maxFunction = createTestMetadataManager().resolveBuiltinFunction("max", fromTypes(BIGINT));
        assertJsonRoundTrip(VALUE_POINTER_CODEC, new AggregationValuePointer(
                maxFunction,
                new AggregatedSetDescriptor(ImmutableSet.of(new IrLabel("A"), new IrLabel("B")), true),
                ImmutableList.of(new Constant(BIGINT, null)),
                Optional.of(new Symbol(VARCHAR, "classifier")),
                Optional.of(new Symbol(BIGINT, "match_number"))));
    }

    @Test
    public void testExpressionAndValuePointersRoundtrip()
    {
        assertJsonRoundTrip(EXPRESSION_AND_VALUE_POINTERS_CODEC, new ExpressionAndValuePointers(new Constant(BIGINT, null), ImmutableList.of()));

        assertJsonRoundTrip(EXPRESSION_AND_VALUE_POINTERS_CODEC, new ExpressionAndValuePointers(
                ifExpression(
                        new Comparison(GREATER_THAN, new Reference(VARCHAR, "classifier"), new Reference(VARCHAR, "x")),
                        new Cast(new Call(RANDOM, ImmutableList.of()), INTEGER),
                        new Call(NEGATION_INTEGER, ImmutableList.of(new Reference(INTEGER, "match_number")))),
                ImmutableList.of(
                        new ExpressionAndValuePointers.Assignment(
                                new Symbol(VARCHAR, "classifier"),
                                new ClassifierValuePointer(
                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("A"), new IrLabel("B")), false, true, 1, -1))),
                        new ExpressionAndValuePointers.Assignment(
                                new Symbol(BIGINT, "x"),
                                new ScalarValuePointer(
                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("B")), true, false, 2, 1),
                                        new Symbol(BIGINT, "input_symbol_a"))),
                        new ExpressionAndValuePointers.Assignment(
                                new Symbol(BIGINT, "match_number"),
                                new MatchNumberValuePointer()))));
    }

    @Test
    public void testMeasureRoundtrip()
    {
        assertJsonRoundTrip(MEASURE_CODEC, new Measure(
                new ExpressionAndValuePointers(new Constant(BIGINT, null), ImmutableList.of()),
                BOOLEAN));

        assertJsonRoundTrip(MEASURE_CODEC, new Measure(
                new ExpressionAndValuePointers(
                        ifExpression(
                                new Comparison(GREATER_THAN, new Reference(INTEGER, "match_number"), new Reference(INTEGER, "x")),
                                new Constant(BIGINT, 10L),
                                new Call(NEGATION_BIGINT, ImmutableList.of(new Reference(BIGINT, "y")))),
                        ImmutableList.of(
                                new ExpressionAndValuePointers.Assignment(
                                        new Symbol(BIGINT, "match_number"),
                                        new MatchNumberValuePointer()),
                                new ExpressionAndValuePointers.Assignment(
                                        new Symbol(BIGINT, "x"),
                                        new ScalarValuePointer(
                                                new LogicalIndexPointer(ImmutableSet.of(), true, true, 0, 0),
                                                new Symbol(BIGINT, "input_symbol_a"))),
                                new ExpressionAndValuePointers.Assignment(
                                        new Symbol(BIGINT, "y"),
                                        new ScalarValuePointer(
                                                new LogicalIndexPointer(ImmutableSet.of(new IrLabel("B")), false, true, 1, -1),
                                                new Symbol(BIGINT, "input_symbol_b"))))),
                BIGINT));
    }

    @Test
    public void testPatternRecognitionNodeRoundtrip()
    {
        ResolvedFunction rankFunction = createTestMetadataManager().resolveBuiltinFunction("rank", ImmutableList.of());

        // test remaining fields inside PatternRecognitionNode specific to pattern recognition:
        // windowFunctions, measures, commonBaseFrame, rowsPerMatch, skipToLabel, skipToPosition, initial, pattern, subsets, variableDefinitions
        PatternRecognitionNode node = new PatternRecognitionNode(
                new PlanNodeId("0"),
                new ValuesNode(new PlanNodeId("1"), 1),
                new DataOrganizationSpecification(ImmutableList.of(), Optional.empty()),
                Optional.empty(),
                ImmutableSet.of(),
                0,
                ImmutableMap.of(
                        new Symbol(BIGINT, "rank"),
                        new Function(
                                rankFunction,
                                ImmutableList.of(),
                                new Frame(ROWS, CURRENT_ROW, Optional.empty(), Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty()),
                                false)),
                ImmutableMap.of(
                        new Symbol(BOOLEAN, "measure"),
                        new Measure(new ExpressionAndValuePointers(new Constant(BOOLEAN, null), ImmutableList.of()), BOOLEAN)),
                Optional.of(new Frame(ROWS, CURRENT_ROW, Optional.empty(), Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty())),
                WINDOW,
                ImmutableSet.of(new IrLabel("B")),
                LAST,
                true,
                new IrConcatenation(ImmutableList.of(new IrLabel("A"), new IrLabel("B"), new IrLabel("C"))),
                ImmutableMap.of(
                        new IrLabel("B"), new ExpressionAndValuePointers(new Constant(BIGINT, null), ImmutableList.of()),
                        new IrLabel("C"), new ExpressionAndValuePointers(new Constant(BIGINT, null), ImmutableList.of())));

        PatternRecognitionNode roundtripNode = PATTERN_RECOGNITION_NODE_CODEC.fromJson(PATTERN_RECOGNITION_NODE_CODEC.toJson(node));

        assertThat(roundtripNode.getMeasures()).isEqualTo(node.getMeasures());
        assertThat(roundtripNode.getRowsPerMatch()).isEqualTo(node.getRowsPerMatch());
        assertThat(roundtripNode.getSkipToLabels()).isEqualTo(node.getSkipToLabels());
        assertThat(roundtripNode.getSkipToPosition()).isEqualTo(node.getSkipToPosition());
        assertThat(roundtripNode.isInitial()).isEqualTo(node.isInitial());
        assertThat(roundtripNode.getPattern()).isEqualTo(node.getPattern());
        assertThat(roundtripNode.getVariableDefinitions()).isEqualTo(node.getVariableDefinitions());
    }

    public static <T> void assertJsonRoundTrip(JsonCodec<T> codec, T object)
    {
        String json = codec.toJson(object);
        T copy = codec.fromJson(json);
        assertThat(copy).isEqualTo(object);
    }
}
