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
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.InvocationConvention.InvocationArgumentConvention;
import io.trino.spi.function.Signature;
import io.trino.spi.type.AbstractVariableWidthType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.FunctionType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeDescriptor;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.In;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.planner.Symbol;
import io.trino.transaction.TransactionManager;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.STATIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.ClassGenerator.classGenerator;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.slice.Slices.allocate;
import static io.trino.block.BlockAssertions.createRepeatedValuesBlock;
import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.operator.scalar.ArrayTransformFunction.ARRAY_TRANSFORM_NAME;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.gen.RowConstructorCodeGenerator.MEGAMORPHIC_FIELD_COUNT;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.IrExpressions.call;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
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
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();
    private static final Map<Symbol, Integer> LAYOUT = ImmutableMap.of(new Symbol(BIGINT, "$col_0"), 2);
    private static final Call ADD_10_EXPRESSION = call(
            FUNCTION_RESOLUTION.resolveOperator(ADD, ImmutableList.of(BIGINT, BIGINT)),
            new Reference(BIGINT, "$col_0"),
            new Constant(BIGINT, 10L));

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
    public void testTransformWithPrivateJavaType()
    {
        HiddenFunctions hiddenFunctions = createHiddenFunctions();
        Type hiddenType = hiddenFunctions.type();
        ArrayType inputArrayType = new ArrayType(BIGINT);
        ArrayType outputArrayType = new ArrayType(hiddenType);
        TestingFunctionResolution functionResolution = createFunctionResolution(
                hiddenType,
                new HiddenFunction("test_hidden_constructor", hiddenType, hiddenFunctions.constructor(), ImmutableList.of()));

        ResolvedFunction constructor = functionResolution.resolveFunction("test_hidden_constructor", fromTypes());
        ResolvedFunction transform = functionResolution.resolveFunction(ARRAY_TRANSFORM_NAME, fromTypes(inputArrayType, new FunctionType(ImmutableList.of(BIGINT), hiddenType)));
        PageProjection projection = functionResolution.getPageFunctionCompiler()
                .compileProjection(
                        call(transform,
                                new Reference(inputArrayType, "$col_0"),
                                new Lambda(ImmutableList.of(new Symbol(BIGINT, "x")), call(constructor))),
                        ImmutableMap.of(new Symbol(inputArrayType, "$col_0"), 0),
                        Optional.empty())
                .get();

        Page page = createSingleArrayPage(inputArrayType, 1);
        Block result = project(projection, page, SelectedPositions.positionsRange(0, page.getPositionCount()));
        assertThat(outputArrayType.getObjectValue(result, 0)).isEqualTo(ImmutableList.of(42));
    }

    @Test
    public void testTransformValuesWithPrivateJavaType()
    {
        HiddenFunctions hiddenFunctions = createHiddenFunctions();
        Type hiddenType = hiddenFunctions.type();
        MapType inputMapType = new MapType(BIGINT, BIGINT, TYPE_OPERATORS);
        MapType outputMapType = new MapType(BIGINT, hiddenType, TYPE_OPERATORS);
        TestingFunctionResolution functionResolution = createFunctionResolution(
                hiddenType,
                new HiddenFunction("test_hidden_constructor", hiddenType, hiddenFunctions.constructor(), ImmutableList.of()));

        ResolvedFunction constructor = functionResolution.resolveFunction("test_hidden_constructor", fromTypes());
        ResolvedFunction transformValues = functionResolution.resolveFunction("transform_values", fromTypes(inputMapType, new FunctionType(ImmutableList.of(BIGINT, BIGINT), hiddenType)));
        PageProjection projection = functionResolution.getPageFunctionCompiler()
                .compileProjection(
                        call(transformValues,
                                new Reference(inputMapType, "$col_0"),
                                new Lambda(ImmutableList.of(new Symbol(BIGINT, "k"), new Symbol(BIGINT, "v")), call(constructor))),
                        ImmutableMap.of(new Symbol(inputMapType, "$col_0"), 0),
                        Optional.empty())
                .get();

        Page page = createSingleLongMapPage(inputMapType, 1, 11);
        Block result = project(projection, page, SelectedPositions.positionsRange(0, page.getPositionCount()));
        assertThat(outputMapType.getObjectValue(result, 0)).isEqualTo(ImmutableMap.of(1L, 42));
    }

    @Test
    public void testTransformKeysWithPrivateJavaType()
    {
        HiddenFunctions hiddenFunctions = createHiddenFunctions();
        Type hiddenType = hiddenFunctions.type();
        MapType mapType = new MapType(BIGINT, hiddenType, TYPE_OPERATORS);
        TestingFunctionResolution functionResolution = createFunctionResolution(hiddenType);

        ResolvedFunction transformKeys = functionResolution.resolveFunction("transform_keys", fromTypes(mapType, new FunctionType(ImmutableList.of(BIGINT, hiddenType), BIGINT)));
        PageProjection projection = functionResolution.getPageFunctionCompiler()
                .compileProjection(
                        call(transformKeys,
                                new Reference(mapType, "$col_0"),
                                new Lambda(ImmutableList.of(new Symbol(BIGINT, "k"), new Symbol(hiddenType, "v")), new Reference(BIGINT, "k"))),
                        ImmutableMap.of(new Symbol(mapType, "$col_0"), 0),
                        Optional.empty())
                .get();

        Page page = createSingleHiddenValueMapPage(mapType, hiddenType, 1, createHiddenValue(hiddenFunctions));
        Block result = project(projection, page, SelectedPositions.positionsRange(0, page.getPositionCount()));
        assertThat(mapType.getObjectValue(result, 0)).isEqualTo(ImmutableMap.of(1L, 42));
    }

    @Test
    public void testMapFilterWithPrivateJavaType()
    {
        HiddenFunctions hiddenFunctions = createHiddenFunctions();
        Type hiddenType = hiddenFunctions.type();
        MapType mapType = new MapType(BIGINT, hiddenType, TYPE_OPERATORS);
        TestingFunctionResolution functionResolution = createFunctionResolution(hiddenType);

        ResolvedFunction mapFilter = functionResolution.resolveFunction("map_filter", fromTypes(mapType, new FunctionType(ImmutableList.of(BIGINT, hiddenType), BOOLEAN)));
        PageProjection projection = functionResolution.getPageFunctionCompiler()
                .compileProjection(
                        call(mapFilter,
                                new Reference(mapType, "$col_0"),
                                new Lambda(ImmutableList.of(new Symbol(BIGINT, "k"), new Symbol(hiddenType, "v")), new Constant(BOOLEAN, true))),
                        ImmutableMap.of(new Symbol(mapType, "$col_0"), 0),
                        Optional.empty())
                .get();

        Page page = createSingleHiddenValueMapPage(mapType, hiddenType, 1, createHiddenValue(hiddenFunctions));
        Block result = project(projection, page, SelectedPositions.positionsRange(0, page.getPositionCount()));
        assertThat(mapType.getObjectValue(result, 0)).isEqualTo(ImmutableMap.of(1L, 42));
    }

    @Test
    public void testRowConstructorAndDereferenceWithPrivateJavaType()
    {
        HiddenFunctions hiddenFunctions = createHiddenFunctions();
        Type hiddenType = hiddenFunctions.type();
        RowType rowType = RowType.anonymous(ImmutableList.of(hiddenType));
        TestingFunctionResolution functionResolution = createFunctionResolution(
                hiddenType,
                new HiddenFunction("test_hidden_constructor", hiddenType, hiddenFunctions.constructor(), ImmutableList.of()));

        ResolvedFunction constructor = functionResolution.resolveFunction("test_hidden_constructor", fromTypes());
        Expression row = new Row(ImmutableList.of(call(constructor)), rowType);
        Expression dereference = new FieldReference(row, 0);
        PageProjection projection = functionResolution.getPageFunctionCompiler()
                .compileProjection(dereference, ImmutableMap.of(), Optional.empty())
                .get();

        Page page = createLongBlockPage(0);
        Block result = project(projection, page, SelectedPositions.positionsRange(0, page.getPositionCount()));
        assertThat(hiddenType.getObjectValue(result, 0)).isEqualTo(42);
    }

    @Test
    void testFilterWithLargeInputBackedRow()
    {
        int fieldCount = MEGAMORPHIC_FIELD_COUNT + 1;
        Reference input = new Reference(BIGINT, "$col_0");
        Row row = new Row(nCopies(fieldCount, input), RowType.anonymous(nCopies(fieldCount, BIGINT)));
        Expression filter = comparison(GREATER_THAN, new FieldReference(row, fieldCount - 1), new Constant(BIGINT, 2L));

        PageFilter compiled = FUNCTION_RESOLUTION.getPageFunctionCompiler()
                .compileFilter(filter, ImmutableMap.of(new Symbol(BIGINT, "$col_0"), 0), Optional.empty())
                .get();

        Page page = createLongBlockPage(0, 1, 2, 3, 4);
        SourcePage inputPage = compiled.getInputChannels().getInputChannels(SourcePage.create(page));
        assertThat(compiled.filter(SESSION, inputPage).size()).isEqualTo(2);
    }

    @Test
    public void testLargeRowConstructorWithBulkyFields()
    {
        // Regression test for https://github.com/trinodb/trino/issues/30311: partial row constructor
        // methods were packed by a fixed field count, so a batch of fields with bulky initialization
        // bytecode (e.g. nested rows over varchar values) exceeded the 64KB JVM method size limit
        int fieldCount = 120;
        RowType nestedRowType = RowType.anonymous(nCopies(8, VARCHAR));
        RowType rowType = RowType.anonymous(nCopies(fieldCount, nestedRowType));

        Expression nestedRow = new Row(nCopies(8, new Reference(VARCHAR, "$col_0")), nestedRowType);
        Expression row = new Row(nCopies(fieldCount, nestedRow), rowType);

        PageProjection projection = FUNCTION_RESOLUTION.getPageFunctionCompiler()
                .compileProjection(row, ImmutableMap.of(new Symbol(VARCHAR, "$col_0"), 0), Optional.empty())
                .get();

        Page page = new Page(createStringsBlock("abc", "xyz"));
        Block result = project(projection, page, SelectedPositions.positionsRange(0, page.getPositionCount()));
        assertThat(result.getPositionCount()).isEqualTo(2);
        assertThat(rowType.getObjectValue(result, 0)).isEqualTo(nCopies(fieldCount, nCopies(8, "abc")));
        assertThat(rowType.getObjectValue(result, 1)).isEqualTo(nCopies(fieldCount, nCopies(8, "xyz")));
    }

    @Test
    public void testCompiledClassesAreHidden()
            throws ReflectiveOperationException
    {
        PageFunctionCompiler compiler = FUNCTION_RESOLUTION.getPageFunctionCompiler();

        PageProjection projection = compiler.compileProjection(ADD_10_EXPRESSION, LAYOUT, Optional.empty()).get();
        Field workFactoryField = projection.getClass().getDeclaredField("pageProjectionWorkFactory");
        workFactoryField.setAccessible(true);
        Class<?> workClass = ((MethodHandle) workFactoryField.get(projection)).type().returnType();
        assertThat(workClass.isHidden()).isTrue();
        // names are stable unless class dumping is enabled; the JVM appends the unique suffix
        assertThat(workClass.getName()).matches("io\\.trino\\.\\$gen\\.PageProjectionWork/0x[0-9a-f]+");

        Expression filter = comparison(GREATER_THAN, new Reference(BIGINT, "$col_0"), new Constant(BIGINT, 2L));
        PageFilter pageFilter = compiler.compileFilter(filter, LAYOUT, Optional.empty()).get();
        assertThat(pageFilter.getClass().isHidden()).isTrue();
    }

    @Test
    public void testProjectionTemplateReuse()
    {
        PageFunctionCompiler compiler = FUNCTION_RESOLUTION.getPageFunctionCompiler(100);
        Page page = createPageWithDataAtChannel2(1);

        // same structure with different constants shares one compiled template
        Block first = project(compileAdd(compiler, 10), page, SelectedPositions.positionsRange(0, 1));
        Block second = project(compileAdd(compiler, 99), page, SelectedPositions.positionsRange(0, 1));
        assertThat(BIGINT.getLong(first, 0)).isEqualTo(11);
        assertThat(BIGINT.getLong(second, 0)).isEqualTo(100);

        // two lookups, one template stored on the first miss and hit by the second
        assertThat(compiler.getProjectionTemplateCache().getRequestCount()).isEqualTo(2);
        assertThat(compiler.getProjectionTemplateCache().size()).isEqualTo(1);
        assertThat(compiler.getProjectionTemplateCache().getHitRate()).isEqualTo(0.5);
    }

    @Test
    public void testTemplateAliasedLiterals()
    {
        PageFunctionCompiler compiler = FUNCTION_RESOLUTION.getPageFunctionCompiler(100);
        Page page = createPageWithDataAtChannel2(1);

        // equal literals deduplicate into one class data slot in the template
        Block aliased = project(compileAdd(compiler, 5, 5), page, SelectedPositions.positionsRange(0, 1));
        assertThat(BIGINT.getLong(aliased, 0)).isEqualTo(11);

        // the same structure with unequal literals cannot use the aliased template
        Block distinct = project(compileAdd(compiler, 5, 7), page, SelectedPositions.positionsRange(0, 1));
        assertThat(BIGINT.getLong(distinct, 0)).isEqualTo(13);

        // equal literals fit the template again
        Block aliasedAgain = project(compileAdd(compiler, 9, 9), page, SelectedPositions.positionsRange(0, 1));
        assertThat(BIGINT.getLong(aliasedAgain, 0)).isEqualTo(19);
        assertThat(compiler.getProjectionTemplateCache().size()).isEqualTo(1);
    }

    @Test
    public void testValueDependentFilterNotTemplated()
    {
        PageFunctionCompiler compiler = FUNCTION_RESOLUTION.getPageFunctionCompiler(100);
        Page page = createPageWithDataAtChannel2(1, 2, 3, 4, 5, 6);

        // IN filters derive switch labels and lookup sets from the values, so they must
        // not share templates; correctness across different value lists proves it
        assertThat(filterIn(compiler, page, 1L, 2L, 3L).size()).isEqualTo(3);
        assertThat(filterIn(compiler, page, 4L, 5L, 6L).size()).isEqualTo(3);
        assertThat(filterIn(compiler, page, 42L, 43L, 44L).size()).isEqualTo(0);
        assertThat(compiler.getFilterTemplateCache().size()).isEqualTo(0);
    }

    private static PageProjection compileAdd(PageFunctionCompiler compiler, long value)
    {
        return compiler.compileProjection(
                call(FUNCTION_RESOLUTION.resolveOperator(ADD, ImmutableList.of(BIGINT, BIGINT)),
                        new Reference(BIGINT, "$col_0"),
                        new Constant(BIGINT, value)),
                LAYOUT,
                Optional.empty()).get();
    }

    private static PageProjection compileAdd(PageFunctionCompiler compiler, long first, long second)
    {
        return compiler.compileProjection(
                call(FUNCTION_RESOLUTION.resolveOperator(ADD, ImmutableList.of(BIGINT, BIGINT)),
                        call(FUNCTION_RESOLUTION.resolveOperator(ADD, ImmutableList.of(BIGINT, BIGINT)),
                                new Reference(BIGINT, "$col_0"),
                                new Constant(BIGINT, first)),
                        new Constant(BIGINT, second)),
                LAYOUT,
                Optional.empty()).get();
    }

    private static SelectedPositions filterIn(PageFunctionCompiler compiler, Page page, Long... values)
    {
        Expression filter = new In(
                new Reference(BIGINT, "$col_0"),
                Arrays.stream(values)
                        .map(value -> (Expression) new Constant(BIGINT, value))
                        .collect(toImmutableList()));
        PageFilter compiled = compiler.compileFilter(filter, LAYOUT, Optional.empty()).get();
        SourcePage inputPage = compiled.getInputChannels().getInputChannels(SourcePage.create(page));
        return compiled.filter(SESSION, inputPage);
    }

    @Test
    public void testLambdaCapturingNothingIsHeldInAField()
            throws ReflectiveOperationException
    {
        MapType mapType = new MapType(BIGINT, BIGINT, TYPE_OPERATORS);
        ResolvedFunction mapFilter = FUNCTION_RESOLUTION.resolveFunction("map_filter", fromTypes(mapType, new FunctionType(ImmutableList.of(BIGINT, BIGINT), BOOLEAN)));
        // the lambda body reads neither the enclosing row nor any captured value
        Expression projection = call(
                mapFilter,
                new Reference(mapType, "$col_0"),
                new Lambda(ImmutableList.of(new Symbol(BIGINT, "k"), new Symbol(BIGINT, "v")), new Constant(BOOLEAN, true)));

        PageProjection compiled = FUNCTION_RESOLUTION.getPageFunctionCompiler()
                .compileProjection(projection, ImmutableMap.of(new Symbol(mapType, "$col_0"), 0), Optional.empty())
                .get();

        Field workFactoryField = compiled.getClass().getDeclaredField("pageProjectionWorkFactory");
        workFactoryField.setAccessible(true);
        Class<?> workClass = ((MethodHandle) workFactoryField.get(compiled)).type().returnType();

        // the lambda is built into a field of the class that evaluates it, rather than by
        // every evaluation, so a projection over a page builds one lambda and not one per row
        assertThat(workClass.getDeclaredFields())
                .anyMatch(field -> field.getName().endsWith("_instance"));
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
        Expression filter = comparison(GREATER_THAN, new Reference(BIGINT, "$col_0"), new Constant(BIGINT, 2L));
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
        Expression filter = comparison(GREATER_THAN, new Reference(BIGINT, "$col_0"), new Constant(BIGINT, 2L));

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

    private static Page createSingleArrayPage(ArrayType arrayType, long... values)
    {
        ArrayBlockBuilder builder = arrayType.createBlockBuilder(null, 1);
        builder.buildEntry(elementBuilder -> {
            for (long value : values) {
                BIGINT.writeLong(elementBuilder, value);
            }
        });
        return new Page(builder.build());
    }

    private static Page createSingleLongMapPage(MapType mapType, long key, long value)
    {
        MapBlockBuilder builder = mapType.createBlockBuilder(null, 1);
        builder.buildEntry((keyBuilder, valueBuilder) -> {
            BIGINT.writeLong(keyBuilder, key);
            BIGINT.writeLong(valueBuilder, value);
        });
        return new Page(builder.build());
    }

    private static Page createSingleHiddenValueMapPage(MapType mapType, Type hiddenType, long key, Object value)
    {
        MapBlockBuilder builder = mapType.createBlockBuilder(null, 1);
        builder.buildEntry((keyBuilder, valueBuilder) -> {
            BIGINT.writeLong(keyBuilder, key);
            hiddenType.writeObject(valueBuilder, value);
        });
        return new Page(builder.build());
    }

    private static TestingFunctionResolution createFunctionResolution(Type hiddenType, SqlScalarFunction... functions)
    {
        TransactionManager transactionManager = createTestTransactionManager();
        InternalFunctionBundle.InternalFunctionBundleBuilder functionBundle = InternalFunctionBundle.builder();
        for (SqlScalarFunction function : functions) {
            functionBundle.function(function);
        }
        PlannerContext plannerContext = plannerContextBuilder()
                .withTransactionManager(transactionManager)
                .addType(hiddenType)
                .addFunctions(functionBundle.build())
                .build();
        return new TestingFunctionResolution(transactionManager, plannerContext);
    }

    private static Object createHiddenValue(HiddenFunctions hiddenFunctions)
    {
        try {
            return hiddenFunctions.constructor().invoke();
        }
        catch (Throwable e) {
            throw new RuntimeException(e);
        }
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

        // defined in a separate class loader so the tests exercise the per accessed class
        // lookup anchors for types the engine loader cannot see
        Class<?> hiddenClass = classGenerator(new DynamicClassLoader(TestPageFunctionCompiler.class.getClassLoader()))
                .defineClass(classDefinition, Object.class);
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
        private static final TypeDescriptor TYPE_SIGNATURE = new TypeDescriptor("test_hidden");

        private final Constructor<?> constructor;
        private final Field valueField;

        private HiddenType(Class<?> javaType)
        {
            super(TYPE_SIGNATURE, javaType);
            try {
                constructor = javaType.getConstructor(int.class);
            }
            catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
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
        public Object getObject(Block block, int position)
        {
            try {
                return constructor.newInstance(getSlice(block, position).getInt(0));
            }
            catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
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
