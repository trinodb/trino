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
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.BoundSignature;
import io.trino.metadata.SqlOperator;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;

import static io.trino.metadata.Signature.typeVariable;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.type.UnknownType.UNKNOWN;
import static io.trino.util.Reflection.methodHandle;

public final class CastFromUnknownOperator
        extends SqlOperator
{
    public static final CastFromUnknownOperator CAST_FROM_UNKNOWN = new CastFromUnknownOperator();
    private static final MethodHandle METHOD_HANDLE_NON_NULL = methodHandle(CastFromUnknownOperator.class, "handleNonNull", boolean.class);

    public CastFromUnknownOperator()
    {
        super(CAST,
                ImmutableList.of(typeVariable("E")),
                ImmutableList.of(),
                new TypeSignature("E"),
                ImmutableList.of(UNKNOWN.getTypeSignature()),
                false);
    }

    @Override
    protected ScalarFunctionImplementation specialize(BoundSignature boundSignature)
    {
        Type toType = boundSignature.getReturnType();
        MethodHandle methodHandle = METHOD_HANDLE_NON_NULL.asType(METHOD_HANDLE_NON_NULL.type().changeReturnType(toType.getJavaType()));
        return new ChoicesScalarFunctionImplementation(
                boundSignature,
                FAIL_ON_NULL,
                ImmutableList.of(NEVER_NULL),
                methodHandle);
    }

    @UsedByGeneratedCode
    public static Object handleNonNull(boolean arg)
    {
        throw new IllegalArgumentException("value of unknown type should always be null");
    }
}
