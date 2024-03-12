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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.metadata.ResolvedFunction;
import io.trino.server.ExpressionSerialization;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.planner.Symbol;
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
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.IfExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SymbolReference;
import io.trino.type.TypeDeserializer;
import io.trino.type.TypeSignatureKeyDeserializer;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.plan.FrameBoundType.CURRENT_ROW;
import static io.trino.sql.planner.plan.FrameBoundType.UNBOUNDED_FOLLOWING;
import static io.trino.sql.planner.plan.RowsPerMatch.WINDOW;
import static io.trino.sql.planner.plan.SkipToPosition.LAST;
import static io.trino.sql.planner.plan.WindowFrameType.ROWS;
import static io.trino.sql.tree.ArithmeticUnaryExpression.Sign.MINUS;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPatternRecognitionNodeSerialization
{
    @Test
    public void testScalarValuePointerRoundtrip()
    {
        JsonCodec<ValuePointer> codec = new JsonCodecFactory(new ObjectMapperProvider()).jsonCodec(ValuePointer.class);

        assertJsonRoundTrip(codec, new ScalarValuePointer(
                new LogicalIndexPointer(ImmutableSet.of(), false, false, 5, 5),
                new Symbol("input_symbol")));

        assertJsonRoundTrip(codec, new ScalarValuePointer(
                new LogicalIndexPointer(ImmutableSet.of(new IrLabel("A"), new IrLabel("B")), true, true, 1, -1),
                new Symbol("input_symbol")));
    }

    @Test
    public void testAggregationValuePointerRoundtrip()
    {
        ObjectMapperProvider provider = new ObjectMapperProvider();
        provider.setJsonSerializers(ImmutableMap.of(Expression.class, new ExpressionSerialization.ExpressionSerializer()));
        provider.setJsonDeserializers(ImmutableMap.of(
                Expression.class, new ExpressionSerialization.ExpressionDeserializer(new SqlParser()),
                Type.class, new TypeDeserializer(TESTING_TYPE_MANAGER)));
        provider.setKeyDeserializers(ImmutableMap.of(
                TypeSignature.class, new TypeSignatureKeyDeserializer()));
        JsonCodec<ValuePointer> codec = new JsonCodecFactory(provider).jsonCodec(ValuePointer.class);

        ResolvedFunction countFunction = createTestMetadataManager().resolveBuiltinFunction("count", ImmutableList.of());
        assertJsonRoundTrip(codec, new AggregationValuePointer(
                countFunction,
                new AggregatedSetDescriptor(ImmutableSet.of(), false),
                ImmutableList.of(),
                Optional.of(new Symbol("classifier")),
                Optional.of(new Symbol("match_number"))));

        ResolvedFunction maxFunction = createTestMetadataManager().resolveBuiltinFunction("max", fromTypes(BIGINT));
        assertJsonRoundTrip(codec, new AggregationValuePointer(
                maxFunction,
                new AggregatedSetDescriptor(ImmutableSet.of(new IrLabel("A"), new IrLabel("B")), true),
                ImmutableList.of(new NullLiteral()),
                Optional.of(new Symbol("classifier")),
                Optional.of(new Symbol("match_number"))));
    }

    @Test
    public void testExpressionAndValuePointersRoundtrip()
    {
        ObjectMapperProvider provider = new ObjectMapperProvider();
        provider.setJsonSerializers(ImmutableMap.of(Expression.class, new ExpressionSerialization.ExpressionSerializer()));
        provider.setJsonDeserializers(ImmutableMap.of(Expression.class, new ExpressionSerialization.ExpressionDeserializer(new SqlParser())));
        JsonCodec<ExpressionAndValuePointers> codec = new JsonCodecFactory(provider).jsonCodec(ExpressionAndValuePointers.class);

        assertJsonRoundTrip(codec, new ExpressionAndValuePointers(new NullLiteral(), ImmutableList.of()));

        assertJsonRoundTrip(codec, new ExpressionAndValuePointers(
                new IfExpression(
                        new ComparisonExpression(GREATER_THAN, new SymbolReference("classifier"), new SymbolReference("x")),
                        new FunctionCall(QualifiedName.of("rand"), ImmutableList.of()),
                        new ArithmeticUnaryExpression(MINUS, new SymbolReference("match_number"))),
                ImmutableList.of(
                        new ExpressionAndValuePointers.Assignment(
                                new Symbol("classifier"),
                                new ClassifierValuePointer(
                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("A"), new IrLabel("B")), false, true, 1, -1))),
                        new ExpressionAndValuePointers.Assignment(
                                new Symbol("x"),
                                new ScalarValuePointer(
                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("B")), true, false, 2, 1),
                                        new Symbol("input_symbol_a"))),
                        new ExpressionAndValuePointers.Assignment(
                                new Symbol("match_number"),
                                new MatchNumberValuePointer()))));
    }

    @Test
    public void testMeasureRoundtrip()
    {
        ObjectMapperProvider provider = new ObjectMapperProvider();
        provider.setJsonSerializers(ImmutableMap.of(Expression.class, new ExpressionSerialization.ExpressionSerializer()));
        provider.setJsonDeserializers(ImmutableMap.of(
                Expression.class, new ExpressionSerialization.ExpressionDeserializer(new SqlParser()),
                Type.class, new TypeDeserializer(TESTING_TYPE_MANAGER)));
        JsonCodec<Measure> codec = new JsonCodecFactory(provider).jsonCodec(Measure.class);

        assertJsonRoundTrip(codec, new Measure(
                new ExpressionAndValuePointers(new NullLiteral(), ImmutableList.of()),
                BOOLEAN));

        assertJsonRoundTrip(codec, new Measure(
                new ExpressionAndValuePointers(
                        new IfExpression(
                                new ComparisonExpression(GREATER_THAN, new SymbolReference("match_number"), new SymbolReference("x")),
                                new GenericLiteral("BIGINT", "10"),
                                new ArithmeticUnaryExpression(MINUS, new SymbolReference("y"))),
                        ImmutableList.of(
                                new ExpressionAndValuePointers.Assignment(
                                        new Symbol("match_number"),
                                        new MatchNumberValuePointer()),
                                new ExpressionAndValuePointers.Assignment(
                                        new Symbol("x"),
                                        new ScalarValuePointer(
                                                new LogicalIndexPointer(ImmutableSet.of(), true, true, 0, 0),
                                                new Symbol("input_symbol_a"))),
                                new ExpressionAndValuePointers.Assignment(
                                        new Symbol("y"),
                                        new ScalarValuePointer(
                                                new LogicalIndexPointer(ImmutableSet.of(new IrLabel("B")), false, true, 1, -1),
                                                new Symbol("input_symbol_b"))))),
                BIGINT));
    }

    @Test
    public void testPatternRecognitionNodeRoundtrip()
    {
        ObjectMapperProvider provider = new ObjectMapperProvider();
        provider.setJsonSerializers(ImmutableMap.of(Expression.class, new ExpressionSerialization.ExpressionSerializer()));
        provider.setJsonDeserializers(ImmutableMap.of(
                Expression.class, new ExpressionSerialization.ExpressionDeserializer(new SqlParser()),
                Type.class, new TypeDeserializer(TESTING_TYPE_MANAGER)));
        provider.setKeyDeserializers(ImmutableMap.of(
                TypeSignature.class, new TypeSignatureKeyDeserializer()));
        JsonCodec<PatternRecognitionNode> codec = new JsonCodecFactory(provider).jsonCodec(PatternRecognitionNode.class);

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
                        new Symbol("rank"),
                        new Function(
                                rankFunction,
                                ImmutableList.of(),
                                new Frame(ROWS, CURRENT_ROW, Optional.empty(), Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()),
                                false)),
                ImmutableMap.of(
                        new Symbol("measure"),
                        new Measure(new ExpressionAndValuePointers(new NullLiteral(), ImmutableList.of()), BOOLEAN)),
                Optional.of(new Frame(ROWS, CURRENT_ROW, Optional.empty(), Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty())),
                WINDOW,
                ImmutableSet.of(new IrLabel("B")),
                LAST,
                true,
                new IrConcatenation(ImmutableList.of(new IrLabel("A"), new IrLabel("B"), new IrLabel("C"))),
                ImmutableMap.of(
                        new IrLabel("B"), new ExpressionAndValuePointers(new NullLiteral(), ImmutableList.of()),
                        new IrLabel("C"), new ExpressionAndValuePointers(new NullLiteral(), ImmutableList.of())));

        PatternRecognitionNode roundtripNode = codec.fromJson(codec.toJson(node));

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
