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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.block.Block;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.TypeDescriptor.arrayType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.util.Reflection.constructorMethodHandle;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.invoke.MethodHandles.insertArguments;

public final class MultiMatchAnyFunction
        extends SqlScalarFunction
{
    private static final MethodHandle METHOD_HANDLE = methodHandle(MultiMatchAnyFunction.class, "multiMatchAny", MultiPatternMatcher.class, Slice.class, Block.class);
    private static final MethodHandle CONSTRUCTOR = constructorMethodHandle(MultiPatternMatcher.class, int.class, int.class);

    private final int dfaStatesLimit;
    private final int dfaRetries;

    public MultiMatchAnyFunction(int dfaStatesLimit, int dfaRetries)
    {
        super(FunctionMetadata.scalarBuilder("multi_match_any")
                .signature(Signature.builder()
                        .returnType(BOOLEAN)
                        .argumentType(VARCHAR)
                        .argumentType(arrayType(VARCHAR.getTypeDescriptor()))
                        .build())
                .description("Tests whether any RE2J regular expression is contained in a string")
                .build());
        this.dfaStatesLimit = dfaStatesLimit;
        this.dfaRetries = dfaRetries;
    }

    @Override
    public SpecializedSqlScalarFunction specialize(BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                FAIL_ON_NULL,
                ImmutableList.of(NEVER_NULL, NEVER_NULL),
                METHOD_HANDLE,
                Optional.of(insertArguments(CONSTRUCTOR, 0, dfaStatesLimit, dfaRetries)));
    }

    @UsedByGeneratedCode
    public static boolean multiMatchAny(MultiPatternMatcher matcher, Slice source, Block patterns)
    {
        return matcher.matches(source, patterns);
    }
}
