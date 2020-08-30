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
import java.util.Optional;

import static io.prestosql.metadata.Signature.comparableTypeParameter;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

public class GenericHashCodeOperator
        extends SqlOperator
{
    private final TypeOperators typeOperators;

    public GenericHashCodeOperator(TypeOperators typeOperators)
    {
        super(OperatorType.HASH_CODE,
                ImmutableList.of(comparableTypeParameter("T")),
                ImmutableList.of(),
                BIGINT.getTypeSignature(),
                ImmutableList.of(new TypeSignature("T")),
                false);
        this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
    }

    @Override
    protected ScalarFunctionImplementation specialize(FunctionBinding functionBinding)
    {
        Type type = functionBinding.getTypeVariable("T");
        return invocationConvention -> {
            MethodHandle methodHandle = typeOperators.getHashCodeOperator(type, invocationConvention);
            return new FunctionInvoker(methodHandle, Optional.empty());
        };
    }
}
