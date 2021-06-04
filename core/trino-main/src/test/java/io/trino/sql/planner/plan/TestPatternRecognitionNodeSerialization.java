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
import io.trino.sql.planner.plan.WindowNode.Specification;
import io.trino.sql.planner.rowpattern.LogicalIndexExtractor.ExpressionAndValuePointers;
import io.trino.sql.planner.rowpattern.LogicalIndexExtractor.ValuePointer;
import io.trino.sql.planner.rowpattern.LogicalIndexPointer;
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
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.tree.ArithmeticUnaryExpression.Sign.MINUS;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.tree.FrameBound.Type.CURRENT_ROW;
import static io.trino.sql.tree.FrameBound.Type.UNBOUNDED_FOLLOWING;
import static io.trino.sql.tree.PatternRecognitionRelation.RowsPerMatch.WINDOW;
import static io.trino.sql.tree.SkipTo.Position.LAST;
import static io.trino.sql.tree.WindowFrame.Type.ROWS;
import static org.testng.Assert.assertEquals;

public class TestPatternRecognitionNodeSerialization
{
    @Test
    public void testValuePointerRoundtrip()
    {
        JsonCodec<ValuePointer> codec = new JsonCodecFactory(new ObjectMapperProvider()).jsonCodec(ValuePointer.class);

        assertJsonRoundTrip(codec, new ValuePointer(
                new LogicalIndexPointer(ImmutableSet.of(), false, false, 5, 5),
                new Symbol("input_symbol")));

        assertJsonRoundTrip(codec, new ValuePointer(
                new LogicalIndexPointer(ImmutableSet.of(new IrLabel("A"), new IrLabel("B")), true, true, 1, -1),
                new Symbol("input_symbol")));
    }

    @Test
    public void testExpressionAndValuePointersRoundtrip()
    {
        ObjectMapperProvider provider = new ObjectMapperProvider();
        provider.setJsonSerializers(ImmutableMap.of(Expression.class, new ExpressionSerialization.ExpressionSerializer()));
        provider.setJsonDeserializers(ImmutableMap.of(Expression.class, new ExpressionSerialization.ExpressionDeserializer(new SqlParser())));
        JsonCodec<ExpressionAndValuePointers> codec = new JsonCodecFactory(provider).jsonCodec(ExpressionAndValuePointers.class);

        assertJsonRoundTrip(codec, new ExpressionAndValuePointers(new NullLiteral(), ImmutableList.of(), ImmutableList.of(), ImmutableSet.of(), ImmutableSet.of()));

        assertJsonRoundTrip(codec, new ExpressionAndValuePointers(
                new IfExpression(
                        new ComparisonExpression(GREATER_THAN, new SymbolReference("classifier"), new SymbolReference("x")),
                        new FunctionCall(QualifiedName.of("rand"), ImmutableList.of()),
                        new ArithmeticUnaryExpression(MINUS, new SymbolReference("match_number"))),
                ImmutableList.of(new Symbol("classifier"), new Symbol("x"), new Symbol("match_number")),
                ImmutableList.of(new ValuePointer(
                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("A"), new IrLabel("B")), false, true, 1, -1),
                        new Symbol("input_symbol_a"))),
                ImmutableSet.of(new Symbol("classifier")),
                ImmutableSet.of(new Symbol("match_number"))));
    }

    @Test
    public void testMeasureRoundtrip()
    {
        ObjectMapperProvider provider = new ObjectMapperProvider();
        provider.setJsonSerializers(ImmutableMap.of(Expression.class, new ExpressionSerialization.ExpressionSerializer()));
        provider.setJsonDeserializers(ImmutableMap.of(
                Expression.class, new ExpressionSerialization.ExpressionDeserializer(new SqlParser()),
                Type.class, new TypeDeserializer(createTestMetadataManager())));
        JsonCodec<Measure> codec = new JsonCodecFactory(provider).jsonCodec(Measure.class);

        assertJsonRoundTrip(codec, new Measure(
                new ExpressionAndValuePointers(new NullLiteral(), ImmutableList.of(), ImmutableList.of(), ImmutableSet.of(), ImmutableSet.of()),
                BOOLEAN));

        assertJsonRoundTrip(codec, new Measure(
                new ExpressionAndValuePointers(
                        new IfExpression(
                                new ComparisonExpression(GREATER_THAN, new SymbolReference("match_number"), new SymbolReference("x")),
                                new GenericLiteral("BIGINT", "10"),
                                new ArithmeticUnaryExpression(MINUS, new SymbolReference("y"))),
                        ImmutableList.of(new Symbol("match_number"), new Symbol("x"), new Symbol("y")),
                        ImmutableList.of(
                                new ValuePointer(
                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("A")), false, true, 1, -1),
                                        new Symbol("input_symbol_a")),
                                new ValuePointer(
                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("B")), false, true, 1, -1),
                                        new Symbol("input_symbol_b"))),
                        ImmutableSet.of(),
                        ImmutableSet.of(new Symbol("match_number"))),
                BIGINT));
    }

    @Test
    public void testPatternRecognitionNodeRoundtrip()
    {
        ObjectMapperProvider provider = new ObjectMapperProvider();
        provider.setJsonSerializers(ImmutableMap.of(Expression.class, new ExpressionSerialization.ExpressionSerializer()));
        provider.setJsonDeserializers(ImmutableMap.of(
                Expression.class, new ExpressionSerialization.ExpressionDeserializer(new SqlParser()),
                Type.class, new TypeDeserializer(createTestMetadataManager())));
        provider.setKeyDeserializers(ImmutableMap.of(
                TypeSignature.class, new TypeSignatureKeyDeserializer()));
        JsonCodec<PatternRecognitionNode> codec = new JsonCodecFactory(provider).jsonCodec(PatternRecognitionNode.class);

        ResolvedFunction rankFunction = createTestMetadataManager().resolveFunction(QualifiedName.of("rank"), ImmutableList.of());

        // test remaining fields inside PatternRecognitionNode specific to pattern recognition:
        // windowFunctions, measures, commonBaseFrame, rowsPerMatch, skipToLabel, skipToPosition, initial, pattern, subsets, variableDefinitions
        PatternRecognitionNode node = new PatternRecognitionNode(
                new PlanNodeId("0"),
                new ValuesNode(new PlanNodeId("1"), 1),
                new Specification(ImmutableList.of(), Optional.empty()),
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
                        new Measure(new ExpressionAndValuePointers(new NullLiteral(), ImmutableList.of(), ImmutableList.of(), ImmutableSet.of(), ImmutableSet.of()), BOOLEAN)),
                Optional.of(new Frame(ROWS, CURRENT_ROW, Optional.empty(), Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty())),
                WINDOW,
                Optional.of(new IrLabel("B")),
                LAST,
                true,
                new IrConcatenation(ImmutableList.of(new IrLabel("A"), new IrLabel("B"), new IrLabel("C"))),
                ImmutableMap.of(
                        new IrLabel("U"), ImmutableSet.of(new IrLabel("A"), new IrLabel("B")),
                        new IrLabel("V"), ImmutableSet.of(new IrLabel("B"), new IrLabel("C"))),
                ImmutableMap.of(
                        new IrLabel("B"), new ExpressionAndValuePointers(new NullLiteral(), ImmutableList.of(), ImmutableList.of(), ImmutableSet.of(), ImmutableSet.of()),
                        new IrLabel("C"), new ExpressionAndValuePointers(new NullLiteral(), ImmutableList.of(), ImmutableList.of(), ImmutableSet.of(), ImmutableSet.of())));

        PatternRecognitionNode roundtripNode = codec.fromJson(codec.toJson(node));

        assertEquals(roundtripNode.getMeasures(), node.getMeasures());
        assertEquals(roundtripNode.getRowsPerMatch(), node.getRowsPerMatch());
        assertEquals(roundtripNode.getSkipToLabel(), node.getSkipToLabel());
        assertEquals(roundtripNode.getSkipToPosition(), node.getSkipToPosition());
        assertEquals(roundtripNode.isInitial(), node.isInitial());
        assertEquals(roundtripNode.getPattern(), node.getPattern());
        assertEquals(roundtripNode.getSubsets(), node.getSubsets());
        assertEquals(roundtripNode.getVariableDefinitions(), node.getVariableDefinitions());
    }

    public static <T> void assertJsonRoundTrip(JsonCodec<T> codec, T object)
    {
        String json = codec.toJson(object);
        T copy = codec.fromJson(json);
        assertEquals(copy, object);
    }
}
