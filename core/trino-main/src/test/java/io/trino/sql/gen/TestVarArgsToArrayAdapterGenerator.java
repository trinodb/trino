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

import com.google.common.base.Joiner;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.metadata.SqlScalarFunction;
import io.trino.operator.scalar.ChoicesSpecializedSqlScalarFunction;
import io.trino.operator.scalar.SpecializedSqlScalarFunction;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.InvocationConvention.InvocationReturnConvention;
import io.trino.spi.function.Signature;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.lang.invoke.MethodHandle;
import java.util.Optional;
import java.util.stream.IntStream;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.gen.TestVarArgsToArrayAdapterGenerator.TestVarArgsSum.VAR_ARGS_SUM;
import static io.trino.sql.gen.VarArgsToArrayAdapterGenerator.generateVarArgsToArrayAdapter;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestVarArgsToArrayAdapterGenerator
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
        assertions.addFunctions(new InternalFunctionBundle(VAR_ARGS_SUM));
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testArrayElements()
    {
        assertThat(assertions.function("var_args_sum"))
                .isEqualTo(0);

        assertThat(assertions.function("var_args_sum", "1"))
                .isEqualTo(1);

        assertThat(assertions.function("var_args_sum", "1", "2"))
                .isEqualTo(3);

        assertThat(assertions.function("var_args_sum", "null"))
                .isNull(INTEGER);

        assertThat(assertions.function("var_args_sum", "1", "null", "2", "null", "3"))
                .isNull(INTEGER);

        assertThat(assertions.function("var_args_sum", "1", "2", "3"))
                .isEqualTo(6);

        // var_args_sum(1, 2, 3, ..., k)
        int k = 100;
        String expression = format("var_args_sum(%s)", Joiner.on(",").join(IntStream.rangeClosed(1, k).boxed().collect(toSet())));
        assertThat(assertions.expression(expression))
                .isEqualTo((1 + k) * k / 2);
    }

    public static class TestVarArgsSum
            extends SqlScalarFunction
    {
        public static final TestVarArgsSum VAR_ARGS_SUM = new TestVarArgsSum();

        private static final MethodHandle METHOD_HANDLE = methodHandle(TestVarArgsSum.class, "varArgsSum", Object.class, long[].class);
        private static final MethodHandle USER_STATE_FACTORY = methodHandle(TestVarArgsSum.class, "createState");

        private TestVarArgsSum()
        {
            super(FunctionMetadata.scalarBuilder()
                    .signature(Signature.builder()
                            .name("var_args_sum")
                            .returnType(INTEGER)
                            .argumentType(INTEGER)
                            .variableArity()
                            .build())
                    .nondeterministic()
                    .description("return sum of all the parameters")
                    .build());
        }

        @Override
        protected SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
        {
            VarArgsToArrayAdapterGenerator.MethodHandleAndConstructor methodHandleAndConstructor = generateVarArgsToArrayAdapter(
                    long.class,
                    long.class,
                    boundSignature.getArity(),
                    METHOD_HANDLE,
                    USER_STATE_FACTORY);
            return new ChoicesSpecializedSqlScalarFunction(
                    boundSignature,
                    InvocationReturnConvention.FAIL_ON_NULL,
                    nCopies(boundSignature.getArity(), NEVER_NULL),
                    methodHandleAndConstructor.getMethodHandle(),
                    Optional.of(methodHandleAndConstructor.getConstructor()));
        }

        @UsedByGeneratedCode
        public static Object createState()
        {
            return null;
        }

        @UsedByGeneratedCode
        public static long varArgsSum(Object state, long[] values)
        {
            long sum = 0;
            for (long value : values) {
                sum += value;
            }
            return sum;
        }
    }
}
