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
import io.trino.metadata.FunctionBinding;
import io.trino.metadata.FunctionInvoker;
import io.trino.metadata.SqlOperator;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.trino.metadata.Signature.orderableTypeParameter;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.util.Objects.requireNonNull;

public class GenericComparisonUnorderedFirstOperator
        extends SqlOperator
{
    private final TypeOperators typeOperators;

    public GenericComparisonUnorderedFirstOperator(TypeOperators typeOperators)
    {
        super(OperatorType.COMPARISON_UNORDERED_FIRST,
                ImmutableList.of(orderableTypeParameter("T")),
                ImmutableList.of(),
                INTEGER.getTypeSignature(),
                ImmutableList.of(new TypeSignature("T"), new TypeSignature("T")),
                false);
        this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
    }

    @Override
    protected ScalarFunctionImplementation specialize(FunctionBinding functionBinding)
    {
        Type type = functionBinding.getTypeVariable("T");
        return invocationConvention -> {
            MethodHandle methodHandle = typeOperators.getComparisonUnorderedFirstOperator(type, invocationConvention);
            return new FunctionInvoker(methodHandle, Optional.empty());
        };
    }
}
