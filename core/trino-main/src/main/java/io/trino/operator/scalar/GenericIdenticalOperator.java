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

import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.ScalarFunctionImplementation;
import io.trino.spi.function.Signature;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;

import static io.trino.spi.function.OperatorType.IDENTICAL;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.util.Objects.requireNonNull;

public class GenericIdenticalOperator
        extends SqlScalarFunction
{
    private final TypeOperators typeOperators;

    public GenericIdenticalOperator(TypeOperators typeOperators)
    {
        super(FunctionMetadata.operatorBuilder(IDENTICAL)
                .signature(Signature.builder()
                        .comparableTypeParameter("T")
                        .returnType(BOOLEAN)
                        .argumentType(new TypeSignature("T"))
                        .argumentType(new TypeSignature("T"))
                        .build())
                .argumentNullability(true, true)
                .build());
        this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
    }

    @Override
    protected SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
    {
        Type type = boundSignature.getArgumentType(0);
        return invocationConvention -> {
            MethodHandle methodHandle = typeOperators.getIdenticalOperator(type, invocationConvention);
            return ScalarFunctionImplementation.builder()
                    .methodHandle(methodHandle)
                    .build();
        };
    }
}
