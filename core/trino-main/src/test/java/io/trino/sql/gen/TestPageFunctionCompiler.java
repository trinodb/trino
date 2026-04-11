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
package io.trino.sql.gen;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.bytecode.FieldDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.slice.Slice;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.SqlScalarFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.project.PageFilter;
import io.trino.operator.project.PageProjection;
import io.trino.operator.project.SelectedPositions;
import io.trino.operator.scalar.ChoicesSpecializedSqlScalarFunction;
import io.trino.operator.scalar.SpecializedSqlScalarFunction;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.InvocationConvention.InvocationArgumentConvention;
import io.trino.spi.function.Signature;
import io.trino.spi.type.AbstractVariableWidthType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.transaction.TransactionManager;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.STATIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.slice.Slices.allocate;
import static io.trino.block.BlockAssertions.createRepeatedValuesBlock;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.IrExpressions.call;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static io.trino.util.CompilerUtils.defineClass;
import static io.trino.util.CompilerUtils.makeClassName;
import static io.trino.util.Reflection.constructorMethodHandle;
import static io.trino.util.Reflection.field;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.invoke.MethodHandles.insertArguments;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPageFunctionCompiler
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();
    private static final Map<Symbol, Integer> LAYOUT = ImmutableMap.of(new Symbol(BIGINT, "$col_0"), 2);
    private static final Call ADD_10_EXPRESSION = call(
            FUNCTION_RESOLUTION.resolveOperator(ADD, ImmutableList.of(BIGINT, BIGINT)),
            new Reference(BIGINT, "$col_0"), new Constant(BIGINT, 10L));

    @Test
    public void testFailureDoesNotCorruptFutureResults()
    {
        PageFunctionCompiler functionCompiler = FUNCTION_RESOLUTION.getPageFunctionCompiler();

        Supplier<PageProjection> projectionSupplier = functionCompiler.compileProjection(ADD_10_EXPRESSION, LAYOUT, Optional.empty());
        PageProjection projection = projectionSupplier.get();

        // process good page and verify we got the expected number of result rows
        Page goodPage = createPageWithDataAtChannel2(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        Block goodResult = project(projection, goodPage, SelectedPositions.positionsRange(0, goodPage.getPositionCount()));
        assertThat(goodPage.getPositionCount()).isEqualTo(goodResult.getPositionCount());

        // addition will throw due to integer overflow
        Page badPage = createPageWithDataAtChannel2(0, 1, 2, 3, 4, Long.MAX_VALUE);
        assertTrinoExceptionThrownBy(() -> project(projection, badPage, SelectedPositions.positionsRange(0, 100)))
                .hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);

        // running the good page should still work
        // if block builder in generated code was not reset properly, we could get junk results after the failure
        goodResult = project(projection, goodPage, SelectedPositions.positionsRange(0, goodPage.getPositionCount()));
        assertThat(goodPage.getPositionCount()).isEqualTo(goodResult.getPositionCount());
    }

    @Test
    public void testProjectionWithPrivateJavaType()
    {
        HiddenFunctions hiddenFunctions = createHiddenFunctions();
        Type hiddenType = hiddenFunctions.type();

        TransactionManager transactionManager = createTestTransactionManager();
        PlannerContext plannerContext = plannerContextBuilder()
                .withTransactionManager(transactionManager)
                .addType(hiddenType)
                .addFunctions(InternalFunctionBundle.builder()
                        .function(new HiddenFunction("test_hidden_constructor", hiddenType, hiddenFunctions.constructor(), ImmutableList.of()))
                        .function(new HiddenFunction("test_hidden_identity", hiddenType, hiddenFunctions.identity(), ImmutableList.of(hiddenType)))
                        .build())
                .build();
        TestingFunctionResolution functionResolution = new TestingFunctionResolution(transactionManager, plannerContext);

        ResolvedFunction constructor = functionResolution.resolveFunction("test_hidden_constructor", fromTypes());
        ResolvedFunction identity = functionResolution.resolveFunction("test_hidden_identity", fromTypes(hiddenType));
        PageProjection projection = functionResolution.getPageFunctionCompiler()
                .compileProjection(call(identity, call(constructor)), ImmutableMap.of(), Optional.empty())
                .get();

        Page page = createLongBlockPage(0, 1);
        Block result = project(projection, page, SelectedPositions.positionsRange(0, page.getPositionCount()));
        assertThat(result.getPositionCount()).isEqualTo(page.getPositionCount());
        assertThat(hiddenType.getObjectValue(result, 0)).isEqualTo(42);
        assertThat(hiddenType.getObjectValue(result, 1)).isEqualTo(42);
    }

    @Test
    public void testProjectionCache()
    {
        PageFunctionCompiler cacheCompiler = FUNCTION_RESOLUTION.getPageFunctionCompiler(100);
        Page page = createPageWithDataAtChannel2(0, 1, 2, 3);

        // First compile: cache miss → triggers class compilation
        cacheCompiler.compileProjection(ADD_10_EXPRESSION, LAYOUT, Optional.empty());
        assertThat(cacheCompiler.getProjectionCache().getRequestCount()).isEqualTo(1);
        assertThat(cacheCompiler.getProjectionCache().getLoadCount()).isEqualTo(1);

        // Second compile with same expression: cache hit → no new compilation
        cacheCompiler.compileProjection(ADD_10_EXPRESSION, LAYOUT, Optional.empty());
        assertThat(cacheCompiler.getProjectionCache().getRequestCount()).isEqualTo(2);
        assertThat(cacheCompiler.getProjectionCache().getLoadCount()).isEqualTo(1);

        // classNameSuffix does not affect cache key
        cacheCompiler.compileProjection(ADD_10_EXPRESSION, LAYOUT, Optional.of("hint"));
        assertThat(cacheCompiler.getProjectionCache().getRequestCount()).isEqualTo(3);
        assertThat(cacheCompiler.getProjectionCache().getLoadCount()).isEqualTo(1);

        // Cached projections produce correct results
        PageProjection projection = cacheCompiler.compileProjection(ADD_10_EXPRESSION, LAYOUT, Optional.empty()).get();
        assertThat(project(projection, page, SelectedPositions.positionsRange(0, 4)).getPositionCount()).isEqualTo(4);

        // No-cache compiler always compiles
        PageFunctionCompiler noCacheCompiler = FUNCTION_RESOLUTION.getPageFunctionCompiler();
        assertThat(noCacheCompiler.getProjectionCache()).isNull();
    }

    @Test
    public void testProjectionCacheWithDifferentLayouts()
    {
        // The column is at position 2 in the first layout and position 3 in the second.
        // Both should reuse the same cached compiled class but bind correct InputChannels.
        PageFunctionCompiler cacheCompiler = FUNCTION_RESOLUTION.getPageFunctionCompiler(100);
        Map<Symbol, Integer> layout1 = ImmutableMap.of(new Symbol(BIGINT, "$col_0"), 2);
        Map<Symbol, Integer> layout2 = ImmutableMap.of(new Symbol(BIGINT, "$col_0"), 3);

        PageProjection projection1 = cacheCompiler.compileProjection(ADD_10_EXPRESSION, layout1, Optional.empty()).get();
        PageProjection projection2 = cacheCompiler.compileProjection(ADD_10_EXPRESSION, layout2, Optional.empty()).get();

        // Verify cache hit: only one compilation despite two calls with different layouts
        assertThat(cacheCompiler.getProjectionCache().getRequestCount()).isEqualTo(2);
        assertThat(cacheCompiler.getProjectionCache().getLoadCount()).isEqualTo(1);

        // Page with four columns: channels 0-1 are padding, channel 2 is [100, 200, 300], channel 3 is [1, 2, 3]
        SourcePage sourcePage = SourcePage.create(new Page(
                createRepeatedValuesBlock(0L, 3),
                createRepeatedValuesBlock(0L, 3),
                createLongBlockPage(100, 200, 300).getBlock(0),
                createLongBlockPage(1, 2, 3).getBlock(0)));

        // projection1 reads from source column 2 via InputChannels: expects 110, 210, 310
        SourcePage inputPage1 = projection1.getInputChannels().getInputChannels(sourcePage);
        Block result1 = projection1.project(SESSION, inputPage1, SelectedPositions.positionsRange(0, 3));
        assertThat(BIGINT.getLong(result1, 0)).isEqualTo(110);
        assertThat(BIGINT.getLong(result1, 1)).isEqualTo(210);

        // projection2 reads from source column 3 via InputChannels: expects 11, 12, 13
        SourcePage inputPage2 = projection2.getInputChannels().getInputChannels(sourcePage);
        Block result2 = projection2.project(SESSION, inputPage2, SelectedPositions.positionsRange(0, 3));
        assertThat(BIGINT.getLong(result2, 0)).isEqualTo(11);
        assertThat(BIGINT.getLong(result2, 1)).isEqualTo(12);
    }

    @Test
    public void testFilterCache()
    {
        PageFunctionCompiler cacheCompiler = FUNCTION_RESOLUTION.getPageFunctionCompiler(100);
        Expression filter = new Comparison(GREATER_THAN, new Reference(BIGINT, "$col_0"), new Constant(BIGINT, 2L));
        Map<Symbol, Integer> layout = ImmutableMap.of(new Symbol(BIGINT, "$col_0"), 2);

        // First compile: cache miss
        cacheCompiler.compileFilter(filter, layout, Optional.empty());
        assertThat(cacheCompiler.getFilterCache().getRequestCount()).isEqualTo(1);
        assertThat(cacheCompiler.getFilterCache().getLoadCount()).isEqualTo(1);

        // Second compile: cache hit
        cacheCompiler.compileFilter(filter, layout, Optional.empty());
        assertThat(cacheCompiler.getFilterCache().getRequestCount()).isEqualTo(2);
        assertThat(cacheCompiler.getFilterCache().getLoadCount()).isEqualTo(1);

        // classNameSuffix does not affect cache key
        cacheCompiler.compileFilter(filter, layout, Optional.of("hint"));
        assertThat(cacheCompiler.getFilterCache().getRequestCount()).isEqualTo(3);
        assertThat(cacheCompiler.getFilterCache().getLoadCount()).isEqualTo(1);

        // Cached filter produces correct results
        Page page = createPageWithDataAtChannel2(0, 1, 2, 3, 4);
        PageFilter compiled = cacheCompiler.compileFilter(filter, layout, Optional.empty()).get();
        SourcePage inputPage = compiled.getInputChannels().getInputChannels(SourcePage.create(page));
        SelectedPositions result = compiled.filter(SESSION, inputPage);
        assertThat(result.size()).isEqualTo(2); // values > 2 at positions 3, 4
    }

    @Test
    public void testFilterCacheWithDifferentLayouts()
    {
        // Filter: $col_0 > 2, with column at different positions in each layout
        PageFunctionCompiler cacheCompiler = FUNCTION_RESOLUTION.getPageFunctionCompiler(100);
        Expression filter = new Comparison(GREATER_THAN, new Reference(BIGINT, "$col_0"), new Constant(BIGINT, 2L));

        Map<Symbol, Integer> layout1 = ImmutableMap.of(new Symbol(BIGINT, "$col_0"), 2);
        Map<Symbol, Integer> layout2 = ImmutableMap.of(new Symbol(BIGINT, "$col_0"), 3);

        PageFilter filter1 = cacheCompiler.compileFilter(filter, layout1, Optional.empty()).get();
        PageFilter filter2 = cacheCompiler.compileFilter(filter, layout2, Optional.empty()).get();

        // Verify cache hit: only one compilation despite two calls with different layouts
        assertThat(cacheCompiler.getFilterCache().getRequestCount()).isEqualTo(2);
        assertThat(cacheCompiler.getFilterCache().getLoadCount()).isEqualTo(1);

        // Page with four columns: channels 0-1 are padding, channel 2 is [0, 1, 2, 3, 4], channel 3 is [10, 20, 30, 40, 50]
        SourcePage sourcePage = SourcePage.create(new Page(
                createRepeatedValuesBlock(0L, 5),
                createRepeatedValuesBlock(0L, 5),
                createLongBlockPage(0, 1, 2, 3, 4).getBlock(0),
                createLongBlockPage(10, 20, 30, 40, 50).getBlock(0)));

        // filter1 reads source column 2 via InputChannels: values > 2 are at positions 3, 4
        SourcePage inputPage1 = filter1.getInputChannels().getInputChannels(sourcePage);
        SelectedPositions result1 = filter1.filter(SESSION, inputPage1);
        assertThat(result1.size()).isEqualTo(2);

        // filter2 reads source column 3 via InputChannels: all values > 2, so all 5 positions selected
        SourcePage inputPage2 = filter2.getInputChannels().getInputChannels(sourcePage);
        SelectedPositions result2 = filter2.filter(SESSION, inputPage2);
        assertThat(result2.size()).isEqualTo(5);
    }

    private Block project(PageProjection projection, Page page, SelectedPositions selectedPositions)
    {
        SourcePage sourcePage = SourcePage.create(page);
        SourcePage inputPage = projection.getInputChannels().getInputChannels(sourcePage);
        return projection.project(SESSION, inputPage, selectedPositions);
    }

    private static Page createLongBlockPage(long... values)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(values.length);
        for (long value : values) {
            BIGINT.writeLong(builder, value);
        }
        return new Page(builder.build());
    }

    private static Page createPageWithDataAtChannel2(long... values)
    {
        Page data = createLongBlockPage(values);
        return new Page(createRepeatedValuesBlock(0L, values.length), createRepeatedValuesBlock(0L, values.length), data.getBlock(0));
    }

    private static HiddenFunctions createHiddenFunctions()
    {
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("HiddenValue"),
                type(Object.class));
        FieldDefinition valueField = classDefinition.declareField(a(PUBLIC), "value", int.class);

        Parameter constructorValue = arg("value", int.class);
        MethodDefinition constructor = classDefinition.declareConstructor(a(PUBLIC), constructorValue);
        constructor.getBody()
                .append(constructor.getThis())
                .invokeConstructor(Object.class)
                .append(constructor.getThis().setField(valueField, constructorValue))
                .ret();

        Parameter identityValue = arg("value", classDefinition.getType());
        MethodDefinition identity = classDefinition.declareMethod(a(PUBLIC, STATIC), "identity", classDefinition.getType(), identityValue);
        identity.getBody()
                .append(identityValue.ret());

        Class<?> hiddenClass = defineClass(classDefinition, Object.class, new DynamicClassLoader(TestPageFunctionCompiler.class.getClassLoader()));
        return new HiddenFunctions(
                new HiddenType(hiddenClass),
                insertArguments(constructorMethodHandle(hiddenClass, int.class), 0, 42),
                methodHandle(hiddenClass, "identity", hiddenClass));
    }

    private record HiddenFunctions(Type type, MethodHandle constructor, MethodHandle identity) {}

    private static final class HiddenFunction
            extends SqlScalarFunction
    {
        private final MethodHandle methodHandle;
        private final List<InvocationArgumentConvention> argumentConventions;

        private HiddenFunction(String name, Type returnType, MethodHandle methodHandle, List<Type> argumentTypes)
        {
            super(functionMetadata(name, returnType, argumentTypes));
            this.methodHandle = requireNonNull(methodHandle, "methodHandle is null");
            this.argumentConventions = nCopies(argumentTypes.size(), NEVER_NULL);
        }

        @Override
        protected SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
        {
            return new ChoicesSpecializedSqlScalarFunction(boundSignature, FAIL_ON_NULL, argumentConventions, methodHandle);
        }

        private static FunctionMetadata functionMetadata(String name, Type returnType, List<Type> argumentTypes)
        {
            Signature.Builder signature = Signature.builder()
                    .returnType(returnType);
            argumentTypes.forEach(signature::argumentType);
            return FunctionMetadata.scalarBuilder(name)
                    .signature(signature.build())
                    .hidden()
                    .description("test hidden type function")
                    .build();
        }
    }

    private static final class HiddenType
            extends AbstractVariableWidthType
    {
        private static final TypeSignature TYPE_SIGNATURE = new TypeSignature("test_hidden");

        private final Field valueField;

        private HiddenType(Class<?> javaType)
        {
            super(TYPE_SIGNATURE, javaType);
            valueField = field(javaType, "value");
        }

        @Override
        public String getDisplayName()
        {
            return TYPE_SIGNATURE.toString();
        }

        @Override
        public Object getObjectValue(Block block, int position)
        {
            if (block.isNull(position)) {
                return null;
            }
            return getSlice(block, position).getInt(0);
        }

        @Override
        public Slice getSlice(Block block, int position)
        {
            VariableWidthBlock valueBlock = (VariableWidthBlock) block.getUnderlyingValueBlock();
            return valueBlock.getSlice(block.getUnderlyingValuePosition(position));
        }

        @Override
        public void writeSlice(BlockBuilder blockBuilder, Slice value)
        {
            writeSlice(blockBuilder, value, 0, value.length());
        }

        @Override
        public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
        {
            ((VariableWidthBlockBuilder) blockBuilder).writeEntry(value, offset, length);
        }

        @Override
        public void writeObject(BlockBuilder blockBuilder, Object value)
        {
            try {
                Slice slice = allocate(Integer.BYTES);
                slice.setInt(0, valueField.getInt(value));
                writeSlice(blockBuilder, slice);
            }
            catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
