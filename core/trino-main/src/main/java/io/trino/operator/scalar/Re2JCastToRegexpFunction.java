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
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.Type;
import io.trino.type.Re2JRegexp;

import java.lang.invoke.MethodHandle;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.type.Chars.padSpaces;
import static io.trino.sql.analyzer.TypeSignatureTranslator.parseTypeSignature;
import static io.trino.type.Re2JRegexpType.RE2J_REGEXP_SIGNATURE;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.invoke.MethodHandles.insertArguments;

public class Re2JCastToRegexpFunction
        extends SqlScalarFunction
{
    private static final MethodHandle METHOD_HANDLE = methodHandle(Re2JCastToRegexpFunction.class, "castToRegexp", int.class, int.class, boolean.class, long.class, Slice.class);

    private final int dfaStatesLimit;
    private final int dfaRetries;
    private final boolean padSpaces;

    public static SqlScalarFunction castVarcharToRe2JRegexp(int dfaStatesLimit, int dfaRetries)
    {
        return new Re2JCastToRegexpFunction("varchar(x)", dfaStatesLimit, dfaRetries, false);
    }

    public static SqlScalarFunction castCharToRe2JRegexp(int dfaStatesLimit, int dfaRetries)
    {
        return new Re2JCastToRegexpFunction("char(x)", dfaStatesLimit, dfaRetries, true);
    }

    private Re2JCastToRegexpFunction(String sourceType, int dfaStatesLimit, int dfaRetries, boolean padSpaces)
    {
        super(FunctionMetadata.scalarBuilder()
                .signature(Signature.builder()
                        .operatorType(CAST)
                        .returnType(RE2J_REGEXP_SIGNATURE)
                        .argumentType(parseTypeSignature(sourceType, ImmutableSet.of("x")))
                        .build())
                .build());
        this.dfaStatesLimit = dfaStatesLimit;
        this.dfaRetries = dfaRetries;
        this.padSpaces = padSpaces;
    }

    @Override
    protected SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
    {
        Type inputType = boundSignature.getArgumentType(0);
        Long typeLength = inputType.getTypeSignature().getParameters().get(0).getLongLiteral();
        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                FAIL_ON_NULL,
                ImmutableList.of(NEVER_NULL),
                insertArguments(METHOD_HANDLE, 0, dfaStatesLimit, dfaRetries, padSpaces, typeLength));
    }

    @UsedByGeneratedCode
    public static Re2JRegexp castToRegexp(int dfaStatesLimit, int dfaRetries, boolean padSpaces, long typeLength, Slice pattern)
    {
        try {
            if (padSpaces) {
                pattern = padSpaces(pattern, (int) typeLength);
            }
            return new Re2JRegexp(dfaStatesLimit, dfaRetries, pattern);
        }
        catch (Exception e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }
}
