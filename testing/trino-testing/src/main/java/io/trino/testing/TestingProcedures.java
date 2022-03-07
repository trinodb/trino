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
package io.trino.testing;

import com.google.common.collect.ImmutableList;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.procedure.Procedure.Argument;
import io.trino.spi.type.ArrayType;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.util.Reflection.methodHandle;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public final class TestingProcedures
{
    private final ProcedureTester tester;

    public TestingProcedures(ProcedureTester tester)
    {
        this.tester = requireNonNull(tester, "tester is null");
    }

    @UsedByGeneratedCode
    public void simple()
    {
        tester.recordCalled("simple");
    }

    @UsedByGeneratedCode
    public void args(long x, double y, String z, boolean q)
    {
        tester.recordCalled("args", x, y, z, q);
    }

    @UsedByGeneratedCode
    public void nulls(Long x, String y)
    {
        tester.recordCalled("nulls", x, y);
    }

    @UsedByGeneratedCode
    public void arrays(List<Long> x, List<String> y)
    {
        tester.recordCalled("arrays", x, y);
    }

    @UsedByGeneratedCode
    public void nested(List<List<Long>> x)
    {
        tester.recordCalled("nested", x);
    }

    @UsedByGeneratedCode
    public void sessionFirst(ConnectorSession session, long x)
    {
        requireNonNull(session, "session is null");
        tester.recordCalled("session_first", x);
    }

    @UsedByGeneratedCode
    public void sessionLast(String x, ConnectorSession session)
    {
        requireNonNull(session, "session is null");
        tester.recordCalled("session_last", x);
    }

    @UsedByGeneratedCode
    public void exception()
    {
        tester.recordCalled("exception");
        throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "test exception from procedure");
    }

    @UsedByGeneratedCode
    public void optionals(ConnectorSession session, String x)
    {
        tester.recordCalled("optionals", x);
    }

    @UsedByGeneratedCode
    public void optionals2(ConnectorSession session, String x, String y)
    {
        tester.recordCalled("optionals2", x, y);
    }

    @UsedByGeneratedCode
    public void optionals3(ConnectorSession session, String x, String y, String z)
    {
        tester.recordCalled("optionals3", x, y, z);
    }

    @UsedByGeneratedCode
    public void optionals4(ConnectorSession session, String x, String y, String z, String v)
    {
        tester.recordCalled("optionals4", x, y, z, v);
    }

    @UsedByGeneratedCode
    public void error()
    {
        tester.recordCalled("error");
        throw new RuntimeException("test error from procedure");
    }

    @UsedByGeneratedCode
    public void names(ConnectorSession session, String x, String y, String z, String v)
    {
        tester.recordCalled("names", x, y, z, v);
    }

    public List<Procedure> getProcedures(String schema)
    {
        return ImmutableList.<Procedure>builder()
                .add(procedure(schema, "test_simple", "simple", ImmutableList.of()))
                .add(procedure(schema, "test_lowercase_name", "simple", ImmutableList.of()))
                .add(procedure(schema, "TEST_UPPERCASE_NAME", "simple", ImmutableList.of()))
                .add(procedure(schema, "test_args", "args", ImmutableList.of(
                        new Argument("X", BIGINT),
                        new Argument("Y", DOUBLE),
                        new Argument("Z", VARCHAR),
                        new Argument("Q", BOOLEAN))))
                .add(procedure(schema, "test_nulls", "nulls", ImmutableList.of(
                        new Argument("X", BIGINT),
                        new Argument("Y", VARCHAR))))
                .add(procedure(schema, "test_arrays", "arrays", ImmutableList.of(
                        new Argument("X", new ArrayType(BIGINT)),
                        new Argument("Y", new ArrayType(VARCHAR)))))
                .add(procedure(schema, "test_nested", "nested", ImmutableList.of(
                        new Argument("X", new ArrayType(new ArrayType(BIGINT))))))
                .add(procedure(schema, "test_session_first", "sessionFirst", ImmutableList.of(
                        new Argument("X", BIGINT))))
                .add(procedure(schema, "test_session_last", "sessionLast", ImmutableList.of(
                        new Argument("X", VARCHAR))))
                .add(procedure(schema, "test_optionals", "optionals", ImmutableList.of(
                        new Argument("X", VARCHAR, false, "hello"))))
                .add(procedure(schema, "test_optionals2", "optionals2", ImmutableList.of(
                        new Argument("X", VARCHAR),
                        new Argument("Y", VARCHAR, false, "world"))))
                .add(procedure(schema, "test_optionals3", "optionals3", ImmutableList.of(
                        new Argument("X", VARCHAR, false, "this"),
                        new Argument("Y", VARCHAR, false, "is"),
                        new Argument("Z", VARCHAR, false, "default"))))
                .add(procedure(schema, "test_optionals4", "optionals4", ImmutableList.of(
                        new Argument("X", VARCHAR),
                        new Argument("Y", VARCHAR),
                        new Argument("Z", VARCHAR, false, "z default"),
                        new Argument("V", VARCHAR, false, "v default"))))
                .add(procedure(schema, "test_exception", "exception", ImmutableList.of()))
                .add(procedure(schema, "test_error", "error", ImmutableList.of()))
                .add(procedure(schema, "test_argument_names", "names", ImmutableList.of(
                        new Argument("lower", true, VARCHAR, false, "a"),
                        new Argument("UPPER", true, VARCHAR, false, "b"),
                        new Argument("MixeD", true, VARCHAR, false, "c"),
                        new Argument("with space", true, VARCHAR, false, "d"))))
                .build();
    }

    private Procedure procedure(String schema, String name, String methodName, List<Argument> arguments)
    {
        return new Procedure(schema, name, arguments, handle(methodName));
    }

    private MethodHandle handle(String name)
    {
        List<Method> methods = Arrays.stream(getClass().getMethods())
                .filter(method -> method.getName().equals(name))
                .collect(toList());
        checkArgument(!methods.isEmpty(), "no matching methods: %s", name);
        checkArgument(methods.size() == 1, "multiple matching methods: %s", methods);
        return methodHandle(methods.get(0)).bindTo(this);
    }
}
