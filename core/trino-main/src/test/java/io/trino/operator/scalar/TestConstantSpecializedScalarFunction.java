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
package io.trino.operator.scalar;

import io.trino.metadata.InternalFunctionBundle;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.project.PageProjection;
import io.trino.operator.project.SelectedPositions;
import io.trino.operator.scalar.annotations.ScalarFromAnnotationsParser;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.function.ConstantArgument;
import io.trino.spi.function.ConstantSpecialization;
import io.trino.spi.function.NonConstantImplementation;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.StandardTypes;
import io.trino.sql.gen.columnar.ColumnarFilter;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.query.QueryAssertions;
import io.trino.transaction.TransactionManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.type.StandardTypes.BIGINT;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestConstantSpecializedScalarFunction
{
    private QueryAssertions assertions;
    private TestingFunctionResolution functionResolution;

    @BeforeAll
    public void init()
    {
        InternalFunctionBundle functions = InternalFunctionBundle.builder()
                .scalar(ConstantAdd.class)
                .scalar(NullableConstantAdd.class)
                .scalar(FailingConstantAdd.class)
                .scalar(ConstantGreaterThan.class)
                .scalar(ConstantClamp.class)
                .build();
        assertions = new QueryAssertions();
        assertions.addFunctions(functions);

        TransactionManager transactionManager = createTestTransactionManager();
        functionResolution = new TestingFunctionResolution(
                transactionManager,
                plannerContextBuilder()
                        .withTransactionManager(transactionManager)
                        .addFunctions(functions)
                        .build());
    }

    @BeforeEach
    public void resetCounters()
    {
        ConstantAdd.reset();
        NullableConstantAdd.reset();
        FailingConstantAdd.reset();
        ConstantGreaterThan.reset();
        ConstantClamp.reset();
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testConstantSpecialization()
    {
        Block result = project("constant_specialized_add", new Constant(BigintType.BIGINT, 7L));
        assertThat(BigintType.BIGINT.getLong(result, 0)).isEqualTo(17);
        assertThat(BigintType.BIGINT.getLong(result, 1)).isEqualTo(27);

        // one probe instance when selecting the specialization, one instance for the evaluator
        assertThat(ConstantAdd.constructorInvocations).hasValue(2);
        assertThat(ConstantAdd.specializedInvocations).hasValue(2);
        assertThat(ConstantAdd.rowInvocations).hasValue(0);
    }

    @Test
    public void testFallsBackForRuntimeArgument()
    {
        assertThat(assertions.query("SELECT constant_specialized_add(value, increment) FROM (VALUES (BIGINT '10', BIGINT '7'), (BIGINT '20', BIGINT '8')) t(value, increment)"))
                .matches("VALUES BIGINT '17', BIGINT '28'");

        assertThat(ConstantAdd.constructorInvocations).hasValue(0);
        assertThat(ConstantAdd.specializedInvocations).hasValue(0);
        assertThat(ConstantAdd.rowInvocations).hasValue(2);
    }

    @Test
    public void testNullableConstant()
    {
        Block result = project("nullable_constant_specialized_add", new Constant(BigintType.BIGINT, null));
        assertThat(BigintType.BIGINT.getLong(result, 0)).isEqualTo(110);
        assertThat(BigintType.BIGINT.getLong(result, 1)).isEqualTo(120);

        assertThat(NullableConstantAdd.constructorInvocations).hasValue(2);
        assertThat(NullableConstantAdd.specializedInvocations).hasValue(2);
        assertThat(NullableConstantAdd.rowInvocations).hasValue(0);
    }

    @Test
    public void testNonNullableNullDoesNotConstructInstance()
    {
        Block result = project("constant_specialized_add", new Constant(BigintType.BIGINT, null));
        assertThat(result.isNull(0)).isTrue();
        assertThat(result.isNull(1)).isTrue();

        assertThat(ConstantAdd.constructorInvocations).hasValue(0);
        assertThat(ConstantAdd.specializedInvocations).hasValue(0);
        assertThat(ConstantAdd.rowInvocations).hasValue(0);
    }

    @Test
    public void testDifferentConstantsDoNotShareCompiledClass()
    {
        Block first = project("constant_specialized_add", new Constant(BigintType.BIGINT, 7L));
        Block second = project("constant_specialized_add", new Constant(BigintType.BIGINT, 8L));

        assertThat(BigintType.BIGINT.getLong(first, 0)).isEqualTo(17);
        assertThat(BigintType.BIGINT.getLong(second, 0)).isEqualTo(18);
        assertThat(BigintType.BIGINT.getLong(second, 1)).isEqualTo(28);
    }

    @Test
    public void testFailingConstructorFallsBackToNonConstantImplementation()
    {
        Block result = project("failing_constant_specialized_add", new Constant(BigintType.BIGINT, -1L));
        assertThat(BigintType.BIGINT.getLong(result, 0)).isEqualTo(9);
        assertThat(BigintType.BIGINT.getLong(result, 1)).isEqualTo(19);

        assertThat(FailingConstantAdd.specializedInvocations).hasValue(0);
        assertThat(FailingConstantAdd.rowInvocations).hasValue(2);

        FailingConstantAdd.reset();
        result = project("failing_constant_specialized_add", new Constant(BigintType.BIGINT, 1L));
        assertThat(BigintType.BIGINT.getLong(result, 0)).isEqualTo(11);
        assertThat(FailingConstantAdd.specializedInvocations).hasValue(2);
        assertThat(FailingConstantAdd.rowInvocations).hasValue(0);
    }

    @Test
    public void testNullRemainingArgumentSkipsSpecializedInvocation()
    {
        Block result = projectCall("constant_specialized_add", List.of(column(0), constant(7)), bigints(10L, null));
        assertThat(BigintType.BIGINT.getLong(result, 0)).isEqualTo(17);
        assertThat(result.isNull(1)).isTrue();

        assertThat(ConstantAdd.specializedInvocations).hasValue(1);
        assertThat(ConstantAdd.rowInvocations).hasValue(0);
    }

    @Test
    public void testFilter()
    {
        assertThat(filterCall("constant_specialized_greater_than", List.of(column(0), constant(7)), bigints(5L, 10L, 15L)))
                .containsExactly(1, 2);

        assertThat(ConstantGreaterThan.specializedInvocations).hasValue(3);
        assertThat(ConstantGreaterThan.rowInvocations).hasValue(0);
    }

    @Test
    public void testFilterWithRuntimeArgument()
    {
        assertThat(filterCall("constant_specialized_greater_than", List.of(column(0), column(1)), bigints(5L, 10L, 3L), bigints(7L, 7L, 4L)))
                .containsExactly(1);

        assertThat(ConstantGreaterThan.specializedInvocations).hasValue(0);
        assertThat(ConstantGreaterThan.rowInvocations).hasValue(3);
    }

    @Test
    public void testMostConstantArgumentsSpecializationIsSelected()
    {
        Block result = projectCall("constant_specialized_clamp", List.of(column(0), constant(1), constant(5)), bigints(10L, 0L, 3L));
        assertThat(longs(result)).containsExactly(5L, 1L, 3L);

        assertThat(ConstantClamp.bothInvocations).hasValue(3);
        assertThat(ConstantClamp.lowInvocations).hasValue(0);
        assertThat(ConstantClamp.rowInvocations).hasValue(0);
    }

    @Test
    public void testPartialConstantsUseMatchingSpecialization()
    {
        Block result = projectCall("constant_specialized_clamp", List.of(column(0), constant(1), column(1)), bigints(10L, 0L), bigints(5L, 5L));
        assertThat(longs(result)).containsExactly(5L, 1L);

        assertThat(ConstantClamp.lowInvocations).hasValue(2);
        assertThat(ConstantClamp.bothInvocations).hasValue(0);
        assertThat(ConstantClamp.rowInvocations).hasValue(0);
    }

    @Test
    public void testNoConstantsUseNonConstantImplementation()
    {
        Block result = projectCall("constant_specialized_clamp", List.of(column(0), column(1), column(2)), bigints(10L), bigints(1L), bigints(5L));
        assertThat(longs(result)).containsExactly(5L);

        assertThat(ConstantClamp.rowInvocations).hasValue(1);
        assertThat(ConstantClamp.lowInvocations).hasValue(0);
        assertThat(ConstantClamp.bothInvocations).hasValue(0);
    }

    @Test
    public void testAnnotationValidation()
    {
        assertThatThrownBy(() -> ScalarFromAnnotationsParser.parseFunctionDefinition(EmptyArguments.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must declare at least one @ConstantArgument");

        assertThatThrownBy(() -> ScalarFromAnnotationsParser.parseFunctionDefinition(RemainingSignatureMismatch.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must equal canonical signature");

        assertThatThrownBy(() -> ScalarFromAnnotationsParser.parseFunctionDefinition(DuplicateSpecializations.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("multiple @ConstantSpecialization classes");

        assertThatThrownBy(() -> ScalarFromAnnotationsParser.parseFunctionDefinition(OutOfRangePosition.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("is outside canonical signature");

        assertThatThrownBy(() -> ScalarFromAnnotationsParser.parseFunctionDefinition(DuplicatePosition.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("is declared more than once");
    }

    @Test
    public void testBoundFactoryMethodHandleShapeIsShared()
            throws Throwable
    {
        MethodHandle constructor = MethodHandles.lookup().findConstructor(BoundState.class, MethodType.methodType(void.class, long.class));
        List<MethodHandle> factories = new ArrayList<>();
        Set<Class<?>> handleClasses = new HashSet<>();
        for (long value = 0; value < 10_000; value++) {
            MethodHandle factory = MethodHandles.insertArguments(constructor, 0, value);
            factories.add(factory);
            handleClasses.add(factory.getClass());
        }

        assertThat(handleClasses).hasSize(1);
        for (int index = 0; index < factories.size(); index++) {
            assertThat(((BoundState) factories.get(index).invokeExact()).value()).isEqualTo(index);
        }
    }

    private Block project(String functionName, Constant constant)
    {
        return projectCall(functionName, List.of(column(0), constant), bigints(10L, 20L));
    }

    private static Reference column(int channel)
    {
        return new Reference(BigintType.BIGINT, "c" + channel);
    }

    private static Constant constant(long value)
    {
        return new Constant(BigintType.BIGINT, value);
    }

    private static Block bigints(Long... values)
    {
        BlockBuilder builder = BigintType.BIGINT.createFixedSizeBlockBuilder(values.length);
        for (Long value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                BigintType.BIGINT.writeLong(builder, value);
            }
        }
        return builder.build();
    }

    private static List<Long> longs(Block block)
    {
        return IntStream.range(0, block.getPositionCount())
                .mapToObj(position -> BigintType.BIGINT.getLong(block, position))
                .toList();
    }

    private static Map<Symbol, Integer> layout(Block[] columns)
    {
        return IntStream.range(0, columns.length)
                .boxed()
                .collect(toImmutableMap(channel -> new Symbol(BigintType.BIGINT, "c" + channel), channel -> channel));
    }

    private Block projectCall(String functionName, List<Expression> arguments, Block... columns)
    {
        ResolvedFunction function = functionResolution.resolveFunction(functionName, fromTypes(arguments.stream().map(Expression::type).toList()));
        PageProjection projection = functionResolution.getPageFunctionCompiler()
                .compileProjection(new Call(function, arguments), layout(columns), SQL_STANDARD, Optional.empty())
                .get();
        SourcePage inputPage = projection.getInputChannels().getInputChannels(SourcePage.create(new Page(columns)));
        return projection.project(SESSION, inputPage, SelectedPositions.positionsRange(0, columns[0].getPositionCount()));
    }

    private int[] filterCall(String functionName, List<Expression> arguments, Block... columns)
    {
        ResolvedFunction function = functionResolution.resolveFunction(functionName, fromTypes(arguments.stream().map(Expression::type).toList()));
        ColumnarFilter filter = functionResolution.getColumnarFilterCompiler()
                .generateFilter(SQL_STANDARD, new Call(function, arguments), layout(columns))
                .orElseThrow()
                .get();
        int positionCount = columns[0].getPositionCount();
        SourcePage inputPage = filter.getInputChannels().getInputChannels(SourcePage.create(new Page(columns)));
        int[] outputPositions = new int[positionCount];
        int selected = filter.filterPositionsRange(SESSION, outputPositions, 0, positionCount, inputPage);
        return Arrays.copyOf(outputPositions, selected);
    }

    private record BoundState(long value) {}

    @ScalarFunction("constant_specialized_add")
    public static final class ConstantAdd
    {
        private static final AtomicInteger constructorInvocations = new AtomicInteger();
        private static final AtomicInteger specializedInvocations = new AtomicInteger();
        private static final AtomicInteger rowInvocations = new AtomicInteger();

        private ConstantAdd() {}

        private static void reset()
        {
            constructorInvocations.set(0);
            specializedInvocations.set(0);
            rowInvocations.set(0);
        }

        @NonConstantImplementation
        public static final class Row
        {
            private Row() {}

            @SqlType(BIGINT)
            public static long add(@SqlType(BIGINT) long value, @SqlType(BIGINT) long increment)
            {
                rowInvocations.incrementAndGet();
                return value + increment;
            }
        }

        @ConstantSpecialization
        public static final class IncrementSpecialization
        {
            private final long increment;

            public IncrementSpecialization(@ConstantArgument(position = 1) long increment)
            {
                constructorInvocations.incrementAndGet();
                this.increment = increment;
            }

            @SqlType(BIGINT)
            public long add(@SqlType(BIGINT) long value)
            {
                specializedInvocations.incrementAndGet();
                return value + increment;
            }
        }
    }

    @ScalarFunction("nullable_constant_specialized_add")
    public static final class NullableConstantAdd
    {
        private static final AtomicInteger constructorInvocations = new AtomicInteger();
        private static final AtomicInteger specializedInvocations = new AtomicInteger();
        private static final AtomicInteger rowInvocations = new AtomicInteger();

        private NullableConstantAdd() {}

        private static void reset()
        {
            constructorInvocations.set(0);
            specializedInvocations.set(0);
            rowInvocations.set(0);
        }

        @NonConstantImplementation
        public static final class Row
        {
            private Row() {}

            @SqlType(BIGINT)
            public static long add(@SqlType(BIGINT) long value, @SqlNullable @SqlType(BIGINT) Long increment)
            {
                rowInvocations.incrementAndGet();
                return value + (increment == null ? 100 : increment);
            }
        }

        @ConstantSpecialization
        public static final class IncrementSpecialization
        {
            private final Long increment;

            public IncrementSpecialization(@ConstantArgument(position = 1) Long increment)
            {
                constructorInvocations.incrementAndGet();
                this.increment = increment;
            }

            @SqlType(BIGINT)
            public long add(@SqlType(BIGINT) long value)
            {
                specializedInvocations.incrementAndGet();
                return value + (increment == null ? 100 : increment);
            }
        }
    }

    @ScalarFunction("invalid_empty_arguments")
    public static final class EmptyArguments
    {
        @NonConstantImplementation
        public static final class Row
        {
            @SqlType(BIGINT)
            public static long apply(@SqlType(BIGINT) long value)
            {
                return value;
            }
        }

        @ConstantSpecialization
        public static final class Specialized
        {
            public Specialized() {}

            @SqlType(BIGINT)
            public long apply(@SqlType(BIGINT) long value)
            {
                return 0;
            }
        }
    }

    @ScalarFunction("invalid_remaining_signature")
    public static final class RemainingSignatureMismatch
    {
        @NonConstantImplementation
        public static final class Row
        {
            @SqlType(BIGINT)
            public static long apply(@SqlType(BIGINT) long value, @SqlType(BIGINT) long increment)
            {
                return value + increment;
            }
        }

        @ConstantSpecialization
        public static final class Specialized
        {
            public Specialized(@ConstantArgument(position = 1) long increment) {}

            @SqlType(BIGINT)
            public long apply(@SqlType(BIGINT) long value, @SqlType(BIGINT) long increment)
            {
                return value + increment;
            }
        }
    }

    @ScalarFunction("constant_specialized_greater_than")
    public static final class ConstantGreaterThan
    {
        private static final AtomicInteger specializedInvocations = new AtomicInteger();
        private static final AtomicInteger rowInvocations = new AtomicInteger();

        private ConstantGreaterThan() {}

        private static void reset()
        {
            specializedInvocations.set(0);
            rowInvocations.set(0);
        }

        @NonConstantImplementation
        public static final class Row
        {
            private Row() {}

            @SqlType(StandardTypes.BOOLEAN)
            public static boolean greaterThan(@SqlType(BIGINT) long value, @SqlType(BIGINT) long bound)
            {
                rowInvocations.incrementAndGet();
                return value > bound;
            }
        }

        @ConstantSpecialization
        public static final class BoundSpecialization
        {
            private final long bound;

            public BoundSpecialization(@ConstantArgument(position = 1) long bound)
            {
                this.bound = bound;
            }

            @SqlType(StandardTypes.BOOLEAN)
            public boolean greaterThan(@SqlType(BIGINT) long value)
            {
                specializedInvocations.incrementAndGet();
                return value > bound;
            }
        }
    }

    @ScalarFunction("constant_specialized_clamp")
    public static final class ConstantClamp
    {
        private static final AtomicInteger rowInvocations = new AtomicInteger();
        private static final AtomicInteger lowInvocations = new AtomicInteger();
        private static final AtomicInteger bothInvocations = new AtomicInteger();

        private ConstantClamp() {}

        private static void reset()
        {
            rowInvocations.set(0);
            lowInvocations.set(0);
            bothInvocations.set(0);
        }

        @NonConstantImplementation
        public static final class Row
        {
            private Row() {}

            @SqlType(BIGINT)
            public static long clamp(@SqlType(BIGINT) long value, @SqlType(BIGINT) long low, @SqlType(BIGINT) long high)
            {
                rowInvocations.incrementAndGet();
                return Math.min(Math.max(value, low), high);
            }
        }

        @ConstantSpecialization
        public static final class LowSpecialization
        {
            private final long low;

            public LowSpecialization(@ConstantArgument(position = 1) long low)
            {
                this.low = low;
            }

            @SqlType(BIGINT)
            public long clamp(@SqlType(BIGINT) long value, @SqlType(BIGINT) long high)
            {
                lowInvocations.incrementAndGet();
                return Math.min(Math.max(value, low), high);
            }
        }

        @ConstantSpecialization
        public static final class BothSpecialization
        {
            private final long low;
            private final long high;

            public BothSpecialization(@ConstantArgument(position = 1) long low, @ConstantArgument(position = 2) long high)
            {
                this.low = low;
                this.high = high;
            }

            @SqlType(BIGINT)
            public long clamp(@SqlType(BIGINT) long value)
            {
                bothInvocations.incrementAndGet();
                return Math.min(Math.max(value, low), high);
            }
        }
    }

    @ScalarFunction("failing_constant_specialized_add")
    public static final class FailingConstantAdd
    {
        private static final AtomicInteger specializedInvocations = new AtomicInteger();
        private static final AtomicInteger rowInvocations = new AtomicInteger();

        private FailingConstantAdd() {}

        private static void reset()
        {
            specializedInvocations.set(0);
            rowInvocations.set(0);
        }

        @NonConstantImplementation
        public static final class Row
        {
            private Row() {}

            @SqlType(BIGINT)
            public static long add(@SqlType(BIGINT) long value, @SqlType(BIGINT) long increment)
            {
                rowInvocations.incrementAndGet();
                return value + increment;
            }
        }

        @ConstantSpecialization
        public static final class IncrementSpecialization
        {
            private final long increment;

            public IncrementSpecialization(@ConstantArgument(position = 1) long increment)
            {
                if (increment < 0) {
                    throw new IllegalArgumentException("negative increment");
                }
                this.increment = increment;
            }

            @SqlType(BIGINT)
            public long add(@SqlType(BIGINT) long value)
            {
                specializedInvocations.incrementAndGet();
                return value + increment;
            }
        }
    }

    @ScalarFunction("invalid_duplicate_specializations")
    public static final class DuplicateSpecializations
    {
        @NonConstantImplementation
        public static final class Row
        {
            @SqlType(BIGINT)
            public static long apply(@SqlType(BIGINT) long value, @SqlType(BIGINT) long increment)
            {
                return value + increment;
            }
        }

        @ConstantSpecialization
        public static final class First
        {
            public First(@ConstantArgument(position = 1) long increment) {}

            @SqlType(BIGINT)
            public long apply(@SqlType(BIGINT) long value)
            {
                return value;
            }
        }

        @ConstantSpecialization
        public static final class Second
        {
            public Second(@ConstantArgument(position = 1) long increment) {}

            @SqlType(BIGINT)
            public long apply(@SqlType(BIGINT) long value)
            {
                return value;
            }
        }
    }

    @ScalarFunction("invalid_out_of_range_position")
    public static final class OutOfRangePosition
    {
        @NonConstantImplementation
        public static final class Row
        {
            @SqlType(BIGINT)
            public static long apply(@SqlType(BIGINT) long value)
            {
                return value;
            }
        }

        @ConstantSpecialization
        public static final class Specialized
        {
            public Specialized(@ConstantArgument(position = 5) long value) {}

            @SqlType(BIGINT)
            public long apply(@SqlType(BIGINT) long value)
            {
                return value;
            }
        }
    }

    @ScalarFunction("invalid_duplicate_position")
    public static final class DuplicatePosition
    {
        @NonConstantImplementation
        public static final class Row
        {
            @SqlType(BIGINT)
            public static long apply(@SqlType(BIGINT) long value, @SqlType(BIGINT) long increment)
            {
                return value + increment;
            }
        }

        @ConstantSpecialization
        public static final class Specialized
        {
            public Specialized(@ConstantArgument(position = 1) long first, @ConstantArgument(position = 1) long second) {}

            @SqlType(BIGINT)
            public long apply(@SqlType(BIGINT) long value)
            {
                return value;
            }
        }
    }
}
