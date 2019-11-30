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
import com.google.common.primitives.Primitives;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionArgumentDefinition;
import io.prestosql.metadata.FunctionMetadata;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.ResolvedFunction;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlScalarFunction;
import io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static io.prestosql.metadata.FunctionKind.SCALAR;
import static io.prestosql.metadata.Signature.castableToTypeParameter;
import static io.prestosql.metadata.Signature.typeVariable;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.USE_BOXED_TYPE;
import static java.lang.invoke.MethodHandles.catchException;
import static java.lang.invoke.MethodHandles.constant;
import static java.lang.invoke.MethodHandles.dropArguments;
import static java.lang.invoke.MethodType.methodType;

public class TryCastFunction
        extends SqlScalarFunction
{
    public static final TryCastFunction TRY_CAST = new TryCastFunction();

    public TryCastFunction()
    {
        super(new FunctionMetadata(
                new Signature(
                        "TRY_CAST",
                        ImmutableList.of(castableToTypeParameter("F", new TypeSignature("T")), typeVariable("T")),
                        ImmutableList.of(),
                        new TypeSignature("T"),
                        ImmutableList.of(new TypeSignature("F")),
                        false),
                true,
                ImmutableList.of(new FunctionArgumentDefinition(false)),
                true,
                true,
                "",
                SCALAR));
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, Metadata metadata)
    {
        Type fromType = boundVariables.getTypeVariable("F");
        Type toType = boundVariables.getTypeVariable("T");

        Class<?> returnType = Primitives.wrap(toType.getJavaType());

        // the resulting method needs to return a boxed type
        ResolvedFunction resolvedFunction = metadata.getCoercion(fromType, toType);
        MethodHandle coercion = metadata.getScalarFunctionInvoker(resolvedFunction, Optional.empty()).getMethodHandle();
        coercion = coercion.asType(methodType(returnType, coercion.type()));

        MethodHandle exceptionHandler = dropArguments(constant(returnType, null), 0, RuntimeException.class);
        MethodHandle tryCastHandle = catchException(coercion, RuntimeException.class, exceptionHandler);

        boolean nullable = metadata.getFunctionMetadata(resolvedFunction).getArgumentDefinitions().get(0).isNullable();
        List<ArgumentProperty> argumentProperties = ImmutableList.of(nullable ? valueTypeArgumentProperty(USE_BOXED_TYPE) : valueTypeArgumentProperty(RETURN_NULL_ON_NULL));
        return new ScalarFunctionImplementation(true, argumentProperties, tryCastHandle);
    }
}
