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

import com.google.common.collect.ImmutableMap;
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
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.ScalarFunctionImplementationChoice;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.BigintType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
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
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.spi.type.StandardTypes.BIGINT;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
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

        assertThat(ConstantAdd.constructorInvocations).hasValue(1);
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

        assertThat(NullableConstantAdd.constructorInvocations).hasValue(1);
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
    public void testAnnotationValidation()
    {
        assertThatThrownBy(() -> ScalarFromAnnotationsParser.parseFunctionDefinition(EmptyArguments.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("@ConstantSpecialization arguments are empty");

        assertThatThrownBy(() -> ScalarFromAnnotationsParser.parseFunctionDefinition(ResidualSignatureMismatch.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must equal canonical signature");

        assertThatThrownBy(() -> ScalarFromAnnotationsParser.parseFunctionDefinition(ConstructorArgumentsMismatch.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must declare each consumed @ConstantArgument exactly once");
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
        ResolvedFunction function = functionResolution.resolveFunction(functionName, fromTypes(BigintType.BIGINT, BigintType.BIGINT));
        Symbol value = new Symbol(BigintType.BIGINT, "value");
        PageProjection projection = functionResolution.getPageFunctionCompiler()
                .compileProjection(
                        new Call(function, List.of(new Reference(BigintType.BIGINT, value.name()), constant)),
                        ImmutableMap.of(value, 0),
                        Optional.empty())
                .get();

        BlockBuilder builder = BigintType.BIGINT.createFixedSizeBlockBuilder(2);
        BigintType.BIGINT.writeLong(builder, 10);
        BigintType.BIGINT.writeLong(builder, 20);
        Page page = new Page(builder.build());
        SourcePage inputPage = projection.getInputChannels().getInputChannels(SourcePage.create(page));
        return projection.project(SESSION, inputPage, SelectedPositions.positionsRange(0, 2));
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

        @ScalarFunctionImplementationChoice
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

        @ConstantSpecialization(arguments = 1)
        public static final class IncrementSpecialization
        {
            private final long increment;

            public IncrementSpecialization(@ConstantArgument(1) long increment)
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

        @ScalarFunctionImplementationChoice
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

        @ConstantSpecialization(arguments = 1)
        public static final class IncrementSpecialization
        {
            private final Long increment;

            public IncrementSpecialization(@ConstantArgument(1) Long increment)
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
        @ScalarFunctionImplementationChoice
        public static final class Row
        {
            @SqlType(BIGINT)
            public static long apply(@SqlType(BIGINT) long value)
            {
                return value;
            }
        }

        @ConstantSpecialization(arguments = {})
        public static final class Specialized
        {
            public Specialized(@ConstantArgument(0) long value) {}

            @SqlType(BIGINT)
            public long apply()
            {
                return 0;
            }
        }
    }

    @ScalarFunction("invalid_residual_signature")
    public static final class ResidualSignatureMismatch
    {
        @ScalarFunctionImplementationChoice
        public static final class Row
        {
            @SqlType(BIGINT)
            public static long apply(@SqlType(BIGINT) long value, @SqlType(BIGINT) long increment)
            {
                return value + increment;
            }
        }

        @ConstantSpecialization(arguments = 1)
        public static final class Specialized
        {
            public Specialized(@ConstantArgument(1) long increment) {}

            @SqlType(BIGINT)
            public long apply(@SqlType(BIGINT) long value, @SqlType(BIGINT) long increment)
            {
                return value + increment;
            }
        }
    }

    @ScalarFunction("invalid_constructor_arguments")
    public static final class ConstructorArgumentsMismatch
    {
        @ScalarFunctionImplementationChoice
        public static final class Row
        {
            @SqlType(BIGINT)
            public static long apply(@SqlType(BIGINT) long value, @SqlType(BIGINT) long increment)
            {
                return value + increment;
            }
        }

        @ConstantSpecialization(arguments = 1)
        public static final class Specialized
        {
            public Specialized(@ConstantArgument(0) long value) {}

            @SqlType(BIGINT)
            public long apply(@SqlType(BIGINT) long value)
            {
                return value;
            }
        }
    }
}
