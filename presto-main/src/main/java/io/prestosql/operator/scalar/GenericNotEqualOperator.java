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
package io.prestosql.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.prestosql.metadata.FunctionBinding;
import io.prestosql.metadata.FunctionInvoker;
import io.prestosql.metadata.SqlOperator;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeOperators;
import io.prestosql.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Optional;

import static io.prestosql.metadata.Signature.comparableTypeParameter;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

public class GenericNotEqualOperator
        extends SqlOperator
{
    private static final MethodHandle NOT_METHOD;

    static {
        try {
            NOT_METHOD = lookup().findStatic(GenericNotEqualOperator.class, "not", MethodType.methodType(Boolean.class, Boolean.class));
        }
        catch (NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private final TypeOperators typeOperators;

    public GenericNotEqualOperator(TypeOperators typeOperators)
    {
        super(OperatorType.NOT_EQUAL,
                ImmutableList.of(comparableTypeParameter("T")),
                ImmutableList.of(),
                BOOLEAN.getTypeSignature(),
                ImmutableList.of(new TypeSignature("T"), new TypeSignature("T")),
                true);
        this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
    }

    @Override
    protected ScalarFunctionImplementation specialize(FunctionBinding functionBinding)
    {
        Type type = functionBinding.getTypeVariable("T");
        return invocationConvention -> {
            MethodHandle methodHandle = typeOperators.getEqualOperator(type, invocationConvention);
            MethodHandle notMethodHandle = MethodHandles.filterReturnValue(methodHandle, NOT_METHOD);
            return new FunctionInvoker(notMethodHandle, Optional.empty());
        };
    }

    private static Boolean not(Boolean result)
    {
        if (result == null) {
            return null;
        }
        return !result;
    }
}
