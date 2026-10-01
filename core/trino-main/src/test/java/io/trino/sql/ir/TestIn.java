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
package io.trino.sql.ir;

import com.google.common.collect.ImmutableList;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.project.PageProjection;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.ir.optimizer.IrExpressionOptimizer;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.operator.project.SelectedPositions.positionsRange;
import static io.trino.spi.block.MapHashTables.HashBuildMode.STRICT_EQUALS;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingSession.testSession;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestIn
{
    private final TestingFunctionResolution functions = new TestingFunctionResolution();

    @Test
    void testTypeValidation()
    {
        Expression value = new Reference(BIGINT, "value");
        assertThatThrownBy(() -> new In(value, new Reference(BIGINT, "values")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new In(value, new Array(DOUBLE, List.of())))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(new In(value, new Reference(new ArrayType(BIGINT), "values")).type()).isEqualTo(BOOLEAN);
    }

    @Test
    void testArrayOperandTraversalAndRewriting()
    {
        Reference value = new Reference(BIGINT, "value");
        Array array = new Array(BIGINT, List.of(new Reference(BIGINT, "candidate")));
        In expression = new In(value, array);
        assertThat(expression.children()).isEqualTo(List.of(value, array));
        ImmutableList.Builder<Expression> visited = ImmutableList.builder();
        new DefaultTraversalVisitor<Void>()
        {
            @Override
            protected Void visitArray(Array node, Void context)
            {
                visited.add(node);
                return super.visitArray(node, context);
            }
        }.process(expression);
        assertThat(visited.build()).containsExactly(array);

        Reference replacement = new Reference(array.type(), "values");
        assertThat(ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteArray(Array node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                return replacement;
            }
        }, expression)).isEqualTo(new In(value, replacement));
    }

    @Test
    void testArrayExpressions()
    {
        assertIn(BIGINT, 1L, values(BIGINT, 1L, 2L), true);
        assertIn(BIGINT, 3L, values(BIGINT, 1L, 2L), false);
        assertIn(BIGINT, 1L, values(BIGINT, null, 1L), true);
        assertIn(BIGINT, 3L, values(BIGINT, null, 1L), null);
        assertIn(BIGINT, null, values(BIGINT, 1L), null);
        assertIn(BIGINT, null, values(BIGINT), false);
        assertIn(BIGINT, 1L, values(BIGINT), false);
        assertIn(BIGINT, 1L, null, null);
        assertIn(BIGINT, null, null, null);
        assertIn(BOOLEAN, true, values(BOOLEAN, false, true), true);
        assertIn(DOUBLE, 1.5, values(DOUBLE, 1.5, 2.5), true);
        assertIn(DOUBLE, Double.NaN, values(DOUBLE, Double.NaN), false);
        assertIn(VARCHAR, utf8Slice("a"), values(VARCHAR, utf8Slice("a")), true);

        ArrayType nested = new ArrayType(BIGINT);
        assertIn(nested, values(BIGINT, 1L, null), values(nested, values(BIGINT, 1L, null)), null);
        assertIn(nested, values(BIGINT, 1L), values(nested, values(BIGINT, (Object) null), values(BIGINT, 1L)), true);
    }

    @Test
    void testArrayConstantIdentity()
    {
        assertConstantIdentity(BIGINT, 1L, 1L);
        assertConstantIdentity(new ArrayType(BIGINT), values(BIGINT, 1L, null), values(BIGINT, 1L, null));
        RowType rowType = RowType.anonymous(List.of(BIGINT, BIGINT));
        assertConstantIdentity(
                rowType,
                new SqlRow(0, new Block[] {values(BIGINT, 1L), values(BIGINT, (Object) null)}),
                new SqlRow(0, new Block[] {values(BIGINT, 1L), values(BIGINT, (Object) null)}));
        MapType mapType = new MapType(BIGINT, BIGINT, PLANNER_CONTEXT.getTypeOperators());
        assertConstantIdentity(
                mapType,
                new SqlMap(mapType, STRICT_EQUALS, values(BIGINT, 1L), values(BIGINT, (Object) null)),
                new SqlMap(mapType, STRICT_EQUALS, values(BIGINT, 1L), values(BIGINT, (Object) null)));

        ArrayType doubles = new ArrayType(DOUBLE);
        assertThat(new Constant(doubles, values(DOUBLE, 0.0)))
                .isNotEqualTo(new Constant(doubles, values(DOUBLE, -0.0)));
        assertThat(new Constant(new ArrayType(BIGINT), values(BIGINT, 1L, 2L)))
                .isNotEqualTo(new Constant(new ArrayType(BIGINT), values(BIGINT, 2L, 1L)));
    }

    @Test
    void testReusableCandidates()
    {
        Reference value = new Reference(BIGINT, "value");
        ArrayType type = new ArrayType(BIGINT);
        In expression = new In(value, new Constant(type, values(BIGINT, 1000L, null, 3000L)));
        List<Expression> candidates = expression.valueListElements().orElseThrow();
        assertThat(candidates).containsExactly(new Constant(BIGINT, 1000L), new Constant(BIGINT, null), new Constant(BIGINT, 3000L));
        assertThat(expression.valueListElements().orElseThrow()).isSameAs(candidates);
        assertThatThrownBy(() -> candidates.add(new Constant(BIGINT, 4000L)))
                .isInstanceOf(UnsupportedOperationException.class);

        In equivalent = new In(value, new Constant(type, values(BIGINT, 1000L, null, 3000L)));
        assertThat(expression).isEqualTo(equivalent).hasSameHashCodeAs(equivalent);
        assertThat(PLANNER_CONTEXT.getExpressionCodec().toJson(expression))
                .isEqualTo(PLANNER_CONTEXT.getExpressionCodec().toJson(equivalent));
        In decoded = (In) PLANNER_CONTEXT.getExpressionCodec().fromJson(PLANNER_CONTEXT.getExpressionCodec().toJson(expression));
        assertThat(decoded).isEqualTo(expression);
        assertThat(decoded.valueListElements()).contains(candidates);

        Array array = new Array(BIGINT, candidates);
        assertThat(new In(value, array).valueListElements().orElseThrow()).isSameAs(array.elements());
        assertThat(new In(value, new Constant(type, values(BIGINT))).valueListElements()).contains(List.of());
        assertThat(new In(value, new Constant(type, null)).valueListElements()).isEmpty();
        assertThat(new In(value, new Reference(type, "candidates")).valueListElements()).isEmpty();
    }

    private static void assertConstantIdentity(Type elementType, Object left, Object right)
    {
        Constant first = new Constant(new ArrayType(elementType), values(elementType, left));
        Constant second = new Constant(new ArrayType(elementType), values(elementType, right));
        assertThat(first).isEqualTo(second).hasSameHashCodeAs(second);
    }

    @Test
    void testColumnarFallback()
    {
        Reference value = new Reference(BIGINT, "value");
        Reference array = new Reference(new ArrayType(BIGINT), "values");
        assertThat(functions.getColumnarFilterCompiler(0).generateFilter(
                SQL_STANDARD, new In(value, array), Map.of(new Symbol(BIGINT, "value"), 0, new Symbol(array.type(), "values"), 1)))
                .isEmpty();
        assertThat(functions.getColumnarFilterCompiler(0).generateFilter(
                SQL_STANDARD, new In(value, new Constant(array.type(), values(BIGINT, 1L, 3L))), Map.of(new Symbol(BIGINT, "value"), 0)))
                .isPresent();
    }

    @Test
    @Timeout(10)
    void testOptimizeEmptyArray()
    {
        In expression = new In(new Reference(BIGINT, "value"), new Array(BIGINT, List.of()));
        Expression result = IrExpressionOptimizer.newOptimizer(PLANNER_CONTEXT).process(
                        expression, testSession(), new SymbolAllocator(List.of(new Symbol(BIGINT, "value"))), Map.of())
                .orElse(expression);
        assertThat(result).isInstanceOfSatisfying(In.class, in -> assertThat(in.valueListElements()).contains(List.of()));
    }

    @Test
    void testOptimizeBoundArray()
    {
        ArrayType arrayType = new ArrayType(BIGINT);
        Reference array = new Reference(arrayType, "values");
        In expression = new In(new Constant(BIGINT, 3L), array);
        assertThat(IrExpressionOptimizer.newOptimizer(PLANNER_CONTEXT).process(
                expression,
                testSession(),
                new SymbolAllocator(List.of(new Symbol(arrayType, "values"))),
                Map.of(new Symbol(arrayType, "values"), new Constant(arrayType, values(BIGINT, 1L, 3L)))))
                .contains(Booleans.TRUE);
    }

    private void assertIn(Type type, Object value, Block candidates, Boolean expected)
    {
        ArrayType arrayType = new ArrayType(type);
        Reference valueReference = new Reference(type, "value");
        Reference arrayReference = new Reference(arrayType, "values");
        Map<Symbol, Integer> layout = Map.of(new Symbol(type, "value"), 0, new Symbol(arrayType, "values"), 1);
        Page page = new Page(values(type, value), values(arrayType, candidates));
        Map<String, Object> bindings = new HashMap<>();
        bindings.put("value", value);
        bindings.put("values", candidates);
        assertEvaluates(new In(valueReference, arrayReference), layout, page, bindings, expected);
        assertEvaluates(new In(valueReference, new Call(functions.resolveFunction("reverse", fromTypes(arrayType)), List.of(arrayReference))), layout, page, bindings, expected);
        assertEvaluates(new In(new Constant(type, value), new Constant(arrayType, candidates)), Map.of(), new Page(1), Map.of(), expected);
    }

    private void assertEvaluates(In expression, Map<Symbol, Integer> layout, Page page, Map<String, Object> bindings, Boolean expected)
    {
        // Exercise the coordinator/worker serialization boundary as well as both execution paths.
        Expression decoded = PLANNER_CONTEXT.getExpressionCodec().fromJson(PLANNER_CONTEXT.getExpressionCodec().toJson(expression));
        assertThat(PLANNER_CONTEXT.getExpressionEvaluator().evaluate(decoded, testSession(), bindings))
                .describedAs("interpreted %s", expression)
                .isEqualTo(expected);
        PageProjection projection = functions.getPageFunctionCompiler()
                .compileProjection(decoded, layout, SQL_STANDARD, Optional.empty())
                .get();
        SourcePage input = projection.getInputChannels().getInputChannels(SourcePage.create(page));
        Block result = projection.project(SESSION, input, positionsRange(0, 1));
        assertThat(BOOLEAN.getObjectValue(result, 0))
                .describedAs("compiled %s", expression)
                .isEqualTo(expected);
    }

    private static Block values(Type type, Object... values)
    {
        BlockBuilder builder = type.createBlockBuilder(null, values.length);
        for (Object value : values) {
            writeNativeValue(type, builder, value);
        }
        return builder.build();
    }
}
