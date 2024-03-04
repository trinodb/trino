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

import io.airlift.slice.Slice;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.operator.scalar.SpecializedSqlScalarFunction;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.ScalarFunctionImplementation;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.PlannerContext;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.routine.ir.IrRoutine;
import io.trino.sql.tree.FunctionSpecification;
import io.trino.transaction.TransactionManager;
import org.assertj.core.api.ThrowingConsumer;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandle;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.metadata.FunctionManager.createTestingFunctionManager;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.TransactionBuilder.transaction;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static io.trino.type.UnknownType.UNKNOWN;
import static java.lang.Math.floor;
import static org.assertj.core.api.Assertions.assertThat;

class TestSqlFunctions
{
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final TransactionManager TRANSACTION_MANAGER = createTestTransactionManager();
    private static final PlannerContext PLANNER_CONTEXT = plannerContextBuilder()
            .withTransactionManager(TRANSACTION_MANAGER)
            .build();
    private static final Session SESSION = testSessionBuilder().build();

    @Test
    void testConstantReturn()
    {
        @Language("SQL") String sql = """
                FUNCTION answer()
                RETURNS BIGINT
                RETURN 42
                """;
        assertFunction(sql, handle -> assertThat(handle.invoke()).isEqualTo(42L));
    }

    @Test
    void testSimpleReturn()
    {
        @Language("SQL") String sql = """
                FUNCTION hello(s VARCHAR)
                RETURNS VARCHAR
                RETURN 'Hello, ' || s || '!'
                """;
        assertFunction(sql, handle -> {
            assertThat(handle.invoke(utf8Slice("world"))).isEqualTo(utf8Slice("Hello, world!"));
            assertThat(handle.invoke(utf8Slice("WORLD"))).isEqualTo(utf8Slice("Hello, WORLD!"));
        });

        testSingleExpression(VARCHAR, utf8Slice("foo"), VARCHAR, "Hello, foo!", "'Hello, ' || p || '!'");
    }

    @Test
    void testSimpleExpression()
    {
        @Language("SQL") String sql = """
                FUNCTION test(a bigint)
                RETURNS bigint
                BEGIN
                  DECLARE x bigint DEFAULT CAST(99 AS bigint);
                  RETURN x * a;
                END
                """;
        assertFunction(sql, handle -> {
            assertThat(handle.invoke(0L)).isEqualTo(0L);
            assertThat(handle.invoke(1L)).isEqualTo(99L);
            assertThat(handle.invoke(42L)).isEqualTo(42L * 99);
            assertThat(handle.invoke(123L)).isEqualTo(123L * 99);
        });
    }

    @Test
    void testSimpleCase()
    {
        @Language("SQL") String sql = """
                FUNCTION simple_case(a bigint)
                RETURNS varchar
                BEGIN
                  CASE a
                    WHEN 0 THEN RETURN 'zero';
                    WHEN 1 THEN RETURN 'one';
                    WHEN DECIMAL '10.0' THEN RETURN 'ten';
                    WHEN 20.0E0 THEN RETURN 'twenty';
                    ELSE RETURN 'other';
                  END CASE;
                  RETURN NULL;
                END
                """;
        assertFunction(sql, handle -> {
            assertThat(handle.invoke(0L)).isEqualTo(utf8Slice("zero"));
            assertThat(handle.invoke(1L)).isEqualTo(utf8Slice("one"));
            assertThat(handle.invoke(10L)).isEqualTo(utf8Slice("ten"));
            assertThat(handle.invoke(20L)).isEqualTo(utf8Slice("twenty"));
            assertThat(handle.invoke(42L)).isEqualTo(utf8Slice("other"));
        });
    }

    @Test
    void testSingleIf()
    {
        @Language("SQL") String sql = """
                FUNCTION test_if(a bigint)
                  RETURNS varchar
                  BEGIN
                    IF a = 0 THEN
                      RETURN 'zero';
                    END IF;
                    RETURN 'other';
                  END
                  """;
        assertFunction(sql, handle -> {
            assertThat(handle.invoke(0L)).isEqualTo(utf8Slice("zero"));
            assertThat(handle.invoke(1L)).isEqualTo(utf8Slice("other"));
            assertThat(handle.invoke(10L)).isEqualTo(utf8Slice("other"));
        });
    }

    @Test
    void testSingleBranchIfElse()
    {
        @Language("SQL") String sql = """
                FUNCTION if_else(a bigint)
                  RETURNS varchar
                  BEGIN
                    IF a = 0 THEN
                      RETURN 'zero';
                    ELSE
                      RETURN 'other';
                    END IF;
                    RETURN NULL;
                  END
                  """;
        assertFunction(sql, handle -> {
            assertThat(handle.invoke(0L)).isEqualTo(utf8Slice("zero"));
            assertThat(handle.invoke(1L)).isEqualTo(utf8Slice("other"));
            assertThat(handle.invoke(10L)).isEqualTo(utf8Slice("other"));
        });
    }

    @Test
    void testMultiBranchIfElse()
    {
        @Language("SQL") String sql = """
                FUNCTION multi_if_else(a bigint)
                  RETURNS varchar
                  BEGIN
                    IF a = 0 THEN
                      RETURN 'zero';
                    ELSEIF a = 1 THEN
                      RETURN 'one';
                    ELSEIF a = 2 THEN
                      RETURN 'two';
                    ELSE
                      RETURN 'other';
                    END IF;
                    RETURN NULL;
                  END
                  """;
        assertFunction(sql, handle -> {
            assertThat(handle.invoke(0L)).isEqualTo(utf8Slice("zero"));
            assertThat(handle.invoke(1L)).isEqualTo(utf8Slice("one"));
            assertThat(handle.invoke(2L)).isEqualTo(utf8Slice("two"));
            assertThat(handle.invoke(10L)).isEqualTo(utf8Slice("other"));
        });
    }

    @Test
    void testSearchCase()
    {
        @Language("SQL") String sql = """
                FUNCTION search_case(a bigint, b bigint)
                RETURNS varchar
                BEGIN
                  CASE
                    WHEN a = 0 THEN RETURN 'zero';
                    WHEN b = 1 THEN RETURN 'one';
                    WHEN a = DECIMAL '10.0' THEN RETURN 'ten';
                    WHEN b = 20.0E0 THEN RETURN 'twenty';
                    ELSE RETURN 'other';
                  END CASE;
                  RETURN NULL;
                END
                """;
        assertFunction(sql, handle -> {
            assertThat(handle.invoke(0L, 42L)).isEqualTo(utf8Slice("zero"));
            assertThat(handle.invoke(42L, 1L)).isEqualTo(utf8Slice("one"));
            assertThat(handle.invoke(10L, 42L)).isEqualTo(utf8Slice("ten"));
            assertThat(handle.invoke(42L, 20L)).isEqualTo(utf8Slice("twenty"));
            assertThat(handle.invoke(42L, 42L)).isEqualTo(utf8Slice("other"));

            // verify ordering
            assertThat(handle.invoke(0L, 1L)).isEqualTo(utf8Slice("zero"));
            assertThat(handle.invoke(10L, 1L)).isEqualTo(utf8Slice("one"));
            assertThat(handle.invoke(10L, 20L)).isEqualTo(utf8Slice("ten"));
            assertThat(handle.invoke(42L, 20L)).isEqualTo(utf8Slice("twenty"));
        });
    }

    @Test
    void testFibonacciWhileLoop()
    {
        @Language("SQL") String sql = """
                FUNCTION fib(n bigint)
                RETURNS bigint
                BEGIN
                  DECLARE a, b bigint DEFAULT 1;
                  DECLARE c bigint;
                  IF n <= 2 THEN
                    RETURN 1;
                  END IF;
                  WHILE n > 2 DO
                    SET n = n - 1;
                    SET c = a + b;
                    SET a = b;
                    SET b = c;
                  END WHILE;
                  RETURN c;
                END
                """;
        assertFunction(sql, handle -> {
            assertThat(handle.invoke(1L)).isEqualTo(1L);
            assertThat(handle.invoke(2L)).isEqualTo(1L);
            assertThat(handle.invoke(3L)).isEqualTo(2L);
            assertThat(handle.invoke(4L)).isEqualTo(3L);
            assertThat(handle.invoke(5L)).isEqualTo(5L);
            assertThat(handle.invoke(6L)).isEqualTo(8L);
            assertThat(handle.invoke(7L)).isEqualTo(13L);
            assertThat(handle.invoke(8L)).isEqualTo(21L);
        });
    }

    @Test
    void testBreakContinue()
    {
        @Language("SQL") String sql = """
                FUNCTION test()
                RETURNS bigint
                BEGIN
                  DECLARE a, b int DEFAULT 0;
                  top: WHILE a < 10 DO
                    SET a = a + 1;
                    IF a < 3 THEN
                      ITERATE top;
                    END IF;
                    SET b = b + 1;
                    IF a > 6 THEN
                      LEAVE top;
                    END IF;
                  END WHILE;
                  RETURN b;
                END
                """;
        assertFunction(sql, handle -> assertThat(handle.invoke()).isEqualTo(5L));
    }

    @Test
    void testRepeat()
    {
        @Language("SQL") String sql = """
                FUNCTION test_repeat(a bigint)
                RETURNS bigint
                BEGIN
                  REPEAT
                    SET a = a + 1;
                  UNTIL a >= 10 END REPEAT;
                  RETURN a;
                END
                """;
        assertFunction(sql, handle -> {
            assertThat(handle.invoke(0L)).isEqualTo(10L);
            assertThat(handle.invoke(100L)).isEqualTo(101L);
        });
    }

    @Test
    void testRepeatContinue()
    {
        @Language("SQL") String sql = """
                FUNCTION test_repeat_continue()
                RETURNS bigint
                BEGIN
                  DECLARE a int DEFAULT 0;
                  DECLARE b int DEFAULT 0;
                  top: REPEAT
                    SET a = a + 1;
                    IF a <= 3 THEN
                      ITERATE top;
                    END IF;
                    SET b = b + 1;
                  UNTIL a >= 10 END REPEAT;
                  RETURN b;
                END
                """;
        assertFunction(sql, handle -> assertThat(handle.invoke()).isEqualTo(7L));
    }

    @Test
    void testReuseLabels()
    {
        @Language("SQL") String sql = """
                FUNCTION test()
                RETURNS int
                BEGIN
                  DECLARE r int DEFAULT 0;
                  abc: LOOP
                    SET r = r + 1;
                    LEAVE abc;
                  END LOOP;
                  abc: LOOP
                    SET r = r + 1;
                    LEAVE abc;
                  END LOOP;
                  RETURN r;
                END
                """;
        assertFunction(sql, handle -> assertThat(handle.invoke()).isEqualTo(2L));
    }

    @Test
    void testReuseVariables()
    {
        @Language("SQL") String sql = """
                FUNCTION test()
                RETURNS bigint
                BEGIN
                  DECLARE r bigint DEFAULT 0;
                  BEGIN
                    DECLARE x varchar DEFAULT 'hello';
                    SET r = r + length(x);
                  END;
                  BEGIN
                    DECLARE x array(int) DEFAULT array[1, 2, 3];
                    SET r = r + cardinality(x);
                  END;
                  RETURN r;
                END
                """;
        assertFunction(sql, handle -> assertThat(handle.invoke()).isEqualTo(8L));
    }

    @Test
    void testAssignParameter()
    {
        @Language("SQL") String sql = """
                FUNCTION test(x int)
                RETURNS int
                BEGIN
                  SET x = x * 3;
                  RETURN x;
                END
                """;
        assertFunction(sql, handle -> assertThat(handle.invoke(2L)).isEqualTo(6L));
    }

    @Test
    void testCall()
    {
        testSingleExpression(BIGINT, -123L, BIGINT, 123L, "abs(p)");
    }

    @Test
    void testCallNested()
    {
        testSingleExpression(BIGINT, -123L, BIGINT, 123L, "abs(ceiling(p))");
        testSingleExpression(BIGINT, 42L, DOUBLE, 42.0, "to_unixTime(from_unixtime(p))");
    }

    @Test
    void testArray()
    {
        testSingleExpression(BIGINT, 3L, BIGINT, 5L, "array[3,4,5,6,7][p]");
        testSingleExpression(BIGINT, 0L, BIGINT, 0L, "array_sort(array[3,2,4,5,1,p])[1]");
    }

    @Test
    void testRow()
    {
        testSingleExpression(BIGINT, 8L, BIGINT, 8L, "ROW(1, 'a', p)[3]");
    }

    @Test
    void testLambda()
    {
        testSingleExpression(BIGINT, 3L, BIGINT, 9L, "(transform(ARRAY [5, 6], x -> x + p)[2])", false);
    }

    @Test
    void testTry()
    {
        testSingleExpression(VARCHAR, utf8Slice("42"), BIGINT, 42L, "try(cast(p AS bigint))");
        testSingleExpression(VARCHAR, utf8Slice("abc"), BIGINT, null, "try(cast(p AS bigint))");
    }

    @Test
    void testTryCast()
    {
        testSingleExpression(VARCHAR, utf8Slice("42"), BIGINT, 42L, "try_cast(p AS bigint)");
        testSingleExpression(VARCHAR, utf8Slice("abc"), BIGINT, null, "try_cast(p AS bigint)");
    }

    @Test
    void testNonCanonical()
    {
        testSingleExpression(BIGINT, 100_000L, BIGINT, 1970L, "EXTRACT(YEAR FROM from_unixtime(p))");
    }

    @Test
    void testAtTimeZone()
    {
        testSingleExpression(UNKNOWN, null, VARCHAR, "2012-10-30 18:00:00 America/Los_Angeles", "CAST(TIMESTAMP '2012-10-31 01:00 UTC' AT TIME ZONE 'America/Los_Angeles' AS VARCHAR)");
    }

    @Test
    void testSession()
    {
        testSingleExpression(UNKNOWN, null, DOUBLE, floor(SESSION.getStart().toEpochMilli() / 1000.0), "floor(to_unixtime(localtimestamp))");
        testSingleExpression(UNKNOWN, null, VARCHAR, SESSION.getUser(), "current_user");
    }

    @Test
    void testSpecialType()
    {
        testSingleExpression(VARCHAR, utf8Slice("abc"), BOOLEAN, true, "(p LIKE '%bc')");
        testSingleExpression(VARCHAR, utf8Slice("xb"), BOOLEAN, false, "(p LIKE '%bc')");
        testSingleExpression(VARCHAR, utf8Slice("abc"), BOOLEAN, false, "regexp_like(p, '\\d')");
        testSingleExpression(VARCHAR, utf8Slice("123"), BOOLEAN, true, "regexp_like(p, '\\d')");
        testSingleExpression(VARCHAR, utf8Slice("[4,5,6]"), VARCHAR, "6", "json_extract_scalar(p, '$[2]')");
    }

    private final AtomicLong nextId = new AtomicLong();

    private void testSingleExpression(Type inputType, Object input, Type outputType, Object output, String expression)
    {
        testSingleExpression(inputType, input, outputType, output, expression, true);
    }

    private void testSingleExpression(Type inputType, Object input, Type outputType, Object output, String expression, boolean deterministic)
    {
        @Language("SQL") String sql = "FUNCTION %s(p %s)\nRETURNS %s\n%s\nRETURN %s".formatted(
                "test" + nextId.incrementAndGet(),
                inputType.getTypeSignature(),
                outputType.getTypeSignature(),
                deterministic ? "DETERMINISTIC" : "NOT DETERMINISTIC",
                expression);

        assertFunction(sql, handle -> {
            Object result = handle.invoke(input);

            if ((outputType instanceof VarcharType) && (result instanceof Slice slice)) {
                result = slice.toStringUtf8();
            }

            assertThat(result).isEqualTo(output);
        });
    }

    private static void assertFunction(@Language("SQL") String sql, ThrowingConsumer<MethodHandle> consumer)
    {
        transaction(TRANSACTION_MANAGER, PLANNER_CONTEXT.getMetadata(), new AllowAllAccessControl())
                .singleStatement()
                .execute(SESSION, session -> {
                    ScalarFunctionImplementation implementation = compileFunction(sql, session);
                    MethodHandle handle = implementation.getMethodHandle()
                            .bindTo(getInstance(implementation))
                            .bindTo(session.toConnectorSession());
                    consumer.accept(handle);
                });
    }

    private static Object getInstance(ScalarFunctionImplementation implementation)
    {
        try {
            return implementation.getInstanceFactory().orElseThrow().invoke();
        }
        catch (Throwable t) {
            throwIfUnchecked(t);
            throw new RuntimeException(t);
        }
    }

    private static ScalarFunctionImplementation compileFunction(@Language("SQL") String sql, Session session)
    {
        FunctionSpecification function = SQL_PARSER.createFunctionSpecification(sql);

        FunctionMetadata metadata = SqlRoutineAnalyzer.extractFunctionMetadata(new FunctionId("test"), function);

        SqlRoutineAnalyzer analyzer = new SqlRoutineAnalyzer(PLANNER_CONTEXT, WarningCollector.NOOP);
        SqlRoutineAnalysis analysis = analyzer.analyze(session, new AllowAllAccessControl(), function);

        SqlRoutinePlanner planner = new SqlRoutinePlanner(PLANNER_CONTEXT, WarningCollector.NOOP);
        IrRoutine routine = planner.planSqlFunction(session, function, analysis);

        SqlRoutineCompiler compiler = new SqlRoutineCompiler(createTestingFunctionManager());
        SpecializedSqlScalarFunction sqlScalarFunction = compiler.compile(routine);

        InvocationConvention invocationConvention = new InvocationConvention(
                metadata.getFunctionNullability().getArgumentNullable().stream()
                        .map(nullable -> nullable ? BOXED_NULLABLE : NEVER_NULL)
                        .toList(),
                metadata.getFunctionNullability().isReturnNullable() ? NULLABLE_RETURN : FAIL_ON_NULL,
                true,
                true);

        return sqlScalarFunction.getScalarFunctionImplementation(invocationConvention);
    }
}
