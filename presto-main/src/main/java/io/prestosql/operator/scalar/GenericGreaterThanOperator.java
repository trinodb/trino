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
import com.google.common.primitives.Ints;
import io.prestosql.metadata.FunctionBinding;
import io.prestosql.metadata.FunctionInvoker;
import io.prestosql.metadata.SqlOperator;
import io.prestosql.spi.function.InvocationConvention;
import io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeOperators;
import io.prestosql.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.metadata.Signature.orderableTypeParameter;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Objects.requireNonNull;

public class GenericGreaterThanOperator
        extends SqlOperator
{
    private final TypeOperators typeOperators;

    public GenericGreaterThanOperator(TypeOperators typeOperators)
    {
        super(
                OperatorType.GREATER_THAN,
                ImmutableList.of(orderableTypeParameter("T")),
                ImmutableList.of(),
                BOOLEAN.getTypeSignature(),
                ImmutableList.of(new TypeSignature("T"), new TypeSignature("T")),
                false);
        this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
    }

    @Override
    protected ScalarFunctionImplementation specialize(FunctionBinding functionBinding)
    {
        Type type = functionBinding.getTypeVariable("T");
        return invocationConvention -> {
            MethodHandle methodHandle = reverseBinaryOperatorArgs(invocationConvention, convention -> typeOperators.getLessThanOperator(type, convention));
            return new FunctionInvoker(methodHandle, Optional.empty());
        };
    }

    static MethodHandle reverseBinaryOperatorArgs(InvocationConvention invocationConvention, Function<InvocationConvention, MethodHandle> operatorLoader)
    {
        checkArgument(invocationConvention.getArgumentConventions().size() == 2, "Expected binary convention");

        InvocationConvention reverseArgConvention = new InvocationConvention(
                List.of(invocationConvention.getArgumentConvention(1), invocationConvention.getArgumentConvention(0)),
                invocationConvention.getReturnConvention(),
                false,
                false);

        MethodHandle methodHandle = operatorLoader.apply(reverseArgConvention);

        // reverse the argument order
        InvocationArgumentConvention leftConvention = reverseArgConvention.getArgumentConvention(0);
        InvocationArgumentConvention rightConvention = reverseArgConvention.getArgumentConvention(1);

        // reorder index
        List<Integer> reorderIndexes = new ArrayList<>();
        for (int i = 0; i < leftConvention.getParameterCount(); i++) {
            reorderIndexes.add(rightConvention.getParameterCount() + i);
        }
        for (int i = 0; i < rightConvention.getParameterCount(); i++) {
            reorderIndexes.add(i);
        }

        // MethodHandles.permuteArguments(methodHandle, methodType(boolean.class, long.class, Block.class, int.class), 1, 2, 0)

        // build new signature
        List<Class<?>> reorderTypes = new ArrayList<>();
        for (int i = 0; i < rightConvention.getParameterCount(); i++) {
            reorderTypes.add(methodHandle.type().parameterType(leftConvention.getParameterCount() + i));
        }
        for (int i = 0; i < leftConvention.getParameterCount(); i++) {
            reorderTypes.add(methodHandle.type().parameterType(i));
        }

        return MethodHandles.permuteArguments(methodHandle, methodType(methodHandle.type().returnType(), reorderTypes), Ints.toArray(reorderIndexes));
    }
}
