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
package io.trino.sql.routine;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Type;
import io.trino.sql.relational.InputReferenceExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.routine.ir.IrBlock;
import io.trino.sql.routine.ir.IrBreak;
import io.trino.sql.routine.ir.IrContinue;
import io.trino.sql.routine.ir.IrIf;
import io.trino.sql.routine.ir.IrLabel;
import io.trino.sql.routine.ir.IrLoop;
import io.trino.sql.routine.ir.IrRepeat;
import io.trino.sql.routine.ir.IrReturn;
import io.trino.sql.routine.ir.IrRoutine;
import io.trino.sql.routine.ir.IrSet;
import io.trino.sql.routine.ir.IrStatement;
import io.trino.sql.routine.ir.IrVariable;
import io.trino.sql.routine.ir.IrWhile;
import io.trino.util.Reflection;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.function.OperatorType.MULTIPLY;
import static io.trino.spi.function.OperatorType.SUBTRACT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.relational.Expressions.call;
import static io.trino.sql.relational.Expressions.constant;
import static io.trino.sql.relational.Expressions.constantNull;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.util.Reflection.constructorMethodHandle;
import static java.util.Arrays.stream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSqlRoutineCompiler
{
    private static final Session TEST_SESSION = testSessionBuilder().build();
    private final SqlRoutineCompiler compiler = new SqlRoutineCompiler(PLANNER_CONTEXT.getFunctionManager());

    @Test
    public void testSimpleExpression()
            throws Throwable
    {
        // CREATE FUNCTION test(a bigint)
        // RETURNS bigint
        // BEGIN
        //   DECLARE x bigint DEFAULT 99;
        //   RETURN x * a;
        // END

        IrVariable arg = new IrVariable(0, BIGINT, constantNull(BIGINT));
        IrVariable variable = new IrVariable(1, BIGINT, constant(99L, BIGINT));

        ResolvedFunction multiply = operator(MULTIPLY, BIGINT, BIGINT);

        IrRoutine routine = new IrRoutine(
                BIGINT,
                parameters(arg),
                new IrBlock(variables(variable), statements(
                        new IrSet(variable, call(multiply, reference(variable), reference(arg))),
                        new IrReturn(reference(variable)))));

        MethodHandle handle = compile(routine);

        assertThat(handle.invoke(0L)).isEqualTo(0L);
        assertThat(handle.invoke(1L)).isEqualTo(99L);
        assertThat(handle.invoke(42L)).isEqualTo(42L * 99);
        assertThat(handle.invoke(123L)).isEqualTo(123L * 99);
    }

    @Test
    public void testFibonacciWhileLoop()
            throws Throwable
    {
        // CREATE FUNCTION fib(n bigint)
        // RETURNS bigint
        // BEGIN
        //   DECLARE a bigint DEFAULT 1;
        //   DECLARE b bigint DEFAULT 1;
        //   DECLARE c bigint;
        //
        //   IF n <= 2 THEN
        //     RETURN 1;
        //   END IF;
        //
        //   WHILE n > 2 DO
        //     SET n = n - 1;
        //     SET c = a + b;
        //     SET a = b;
        //     SET b = c;
        //   END WHILE;
        //
        //   RETURN c;
        // END

        IrVariable n = new IrVariable(0, BIGINT, constantNull(BIGINT));
        IrVariable a = new IrVariable(1, BIGINT, constant(1L, BIGINT));
        IrVariable b = new IrVariable(2, BIGINT, constant(1L, BIGINT));
        IrVariable c = new IrVariable(3, BIGINT, constantNull(BIGINT));

        ResolvedFunction add = operator(ADD, BIGINT, BIGINT);
        ResolvedFunction subtract = operator(SUBTRACT, BIGINT, BIGINT);
        ResolvedFunction lessThan = operator(LESS_THAN, BIGINT, BIGINT);
        ResolvedFunction lessThanOrEqual = operator(LESS_THAN_OR_EQUAL, BIGINT, BIGINT);

        IrRoutine routine = new IrRoutine(
                BIGINT,
                parameters(n),
                new IrBlock(variables(a, b, c), statements(
                        new IrIf(
                                call(lessThanOrEqual, reference(n), constant(2L, BIGINT)),
                                new IrReturn(constant(1L, BIGINT)),
                                Optional.empty()),
                        new IrWhile(
                                Optional.empty(),
                                call(lessThan, constant(2L, BIGINT), reference(n)),
                                new IrBlock(
                                        variables(),
                                        statements(
                                                new IrSet(n, call(subtract, reference(n), constant(1L, BIGINT))),
                                                new IrSet(c, call(add, reference(a), reference(b))),
                                                new IrSet(a, reference(b)),
                                                new IrSet(b, reference(c))))),
                        new IrReturn(reference(c)))));

        MethodHandle handle = compile(routine);

        assertThat(handle.invoke(1L)).isEqualTo(1L);
        assertThat(handle.invoke(2L)).isEqualTo(1L);
        assertThat(handle.invoke(3L)).isEqualTo(2L);
        assertThat(handle.invoke(4L)).isEqualTo(3L);
        assertThat(handle.invoke(5L)).isEqualTo(5L);
        assertThat(handle.invoke(6L)).isEqualTo(8L);
        assertThat(handle.invoke(7L)).isEqualTo(13L);
        assertThat(handle.invoke(8L)).isEqualTo(21L);
    }

    @Test
    public void testBreakContinue()
            throws Throwable
    {
        // CREATE FUNCTION test()
        // RETURNS bigint
        // BEGIN
        //   DECLARE a bigint DEFAULT 0;
        //   DECLARE b bigint DEFAULT 0;
        //
        //   top: WHILE a < 10 DO
        //     SET a = a + 1;
        //     IF a < 3 THEN
        //       ITERATE top;
        //     END IF;
        //     SET b = b + 1;
        //     IF a > 6 THEN
        //       LEAVE top;
        //     END IF;
        //   END WHILE;
        //
        //   RETURN b;
        // END

        IrVariable a = new IrVariable(0, BIGINT, constant(0L, BIGINT));
        IrVariable b = new IrVariable(1, BIGINT, constant(0L, BIGINT));

        ResolvedFunction add = operator(ADD, BIGINT, BIGINT);
        ResolvedFunction lessThan = operator(LESS_THAN, BIGINT, BIGINT);

        IrLabel label = new IrLabel("test");

        IrRoutine routine = new IrRoutine(
                BIGINT,
                parameters(),
                new IrBlock(variables(a, b), statements(
                        new IrWhile(
                                Optional.of(label),
                                call(lessThan, reference(a), constant(10L, BIGINT)),
                                new IrBlock(variables(), statements(
                                        new IrSet(a, call(add, reference(a), constant(1L, BIGINT))),
                                        new IrIf(
                                                call(lessThan, reference(a), constant(3L, BIGINT)),
                                                new IrContinue(label),
                                                Optional.empty()),
                                        new IrSet(b, call(add, reference(b), constant(1L, BIGINT))),
                                        new IrIf(
                                                call(lessThan, constant(6L, BIGINT), reference(a)),
                                                new IrBreak(label),
                                                Optional.empty())))),
                        new IrReturn(reference(b)))));

        MethodHandle handle = compile(routine);

        assertThat(handle.invoke()).isEqualTo(5L);
    }

    @Test
    public void testInterruptionWhile()
            throws Throwable
    {
        assertRoutineInterruption(() -> new IrWhile(
                Optional.empty(),
                constant(true, BOOLEAN),
                new IrBlock(variables(), statements())));
    }

    @Test
    public void testInterruptionRepeat()
            throws Throwable
    {
        assertRoutineInterruption(() -> new IrRepeat(
                Optional.empty(),
                constant(false, BOOLEAN),
                new IrBlock(variables(), statements())));
    }

    @Test
    public void testInterruptionLoop()
            throws Throwable
    {
        assertRoutineInterruption(() -> new IrLoop(
                Optional.empty(),
                new IrBlock(variables(), statements())));
    }

    private void assertRoutineInterruption(Supplier<IrStatement> loopFactory)
            throws Throwable
    {
        IrRoutine routine = new IrRoutine(
                BIGINT,
                parameters(),
                new IrBlock(variables(), statements(
                        loopFactory.get(),
                        new IrReturn(constant(null, BIGINT)))));

        MethodHandle handle = compile(routine);

        AtomicBoolean interrupted = new AtomicBoolean();
        Thread thread = new Thread(() -> {
            assertThatThrownBy(handle::invoke)
                    .hasMessageContaining("Thread interrupted");
            interrupted.set(true);
        });
        thread.start();
        thread.interrupt();
        thread.join(TimeUnit.SECONDS.toMillis(10));
        assertThat(interrupted).isTrue();
    }

    private MethodHandle compile(IrRoutine routine)
            throws Throwable
    {
        Class<?> clazz = compiler.compileClass(routine);

        MethodHandle handle = stream(clazz.getMethods())
                .filter(method -> method.getName().equals("run"))
                .map(Reflection::methodHandle)
                .collect(onlyElement());

        Object instance = constructorMethodHandle(clazz).invoke();

        return handle.bindTo(instance).bindTo(TEST_SESSION.toConnectorSession());
    }

    private static List<IrVariable> parameters(IrVariable... variables)
    {
        return ImmutableList.copyOf(variables);
    }

    private static List<IrVariable> variables(IrVariable... variables)
    {
        return ImmutableList.copyOf(variables);
    }

    private static List<IrStatement> statements(IrStatement... statements)
    {
        return ImmutableList.copyOf(statements);
    }

    private static RowExpression reference(IrVariable variable)
    {
        return new InputReferenceExpression(variable.field(), variable.type());
    }

    private static ResolvedFunction operator(OperatorType operator, Type... argumentTypes)
    {
        return PLANNER_CONTEXT.getMetadata().resolveOperator(operator, ImmutableList.copyOf(argumentTypes));
    }
}
