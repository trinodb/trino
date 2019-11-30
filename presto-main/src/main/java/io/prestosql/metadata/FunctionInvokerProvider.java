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
package io.prestosql.metadata;

import io.prestosql.operator.scalar.ScalarFunctionImplementation;
import io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty;
import io.prestosql.operator.scalar.ScalarFunctionImplementation.ScalarImplementationChoice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.InvocationConvention;
import io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention;
import io.prestosql.spi.function.InvocationConvention.InvocationReturnConvention;
import io.prestosql.spi.type.Type;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.metadata.ScalarFunctionAdapter.NullAdaptationPolicy.UNSUPPORTED;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentType.FUNCTION_TYPE;
import static io.prestosql.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.FUNCTION;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.NULL_FLAG;
import static io.prestosql.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.prestosql.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class FunctionInvokerProvider
{
    private final ScalarFunctionAdapter functionAdapter = new ScalarFunctionAdapter(UNSUPPORTED);
    private final Metadata metadata;

    public FunctionInvokerProvider(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    public FunctionInvoker createFunctionInvoker(ResolvedFunction resolvedFunction, Optional<InvocationConvention> invocationConvention)
    {
        ScalarFunctionImplementation scalarFunctionImplementation = metadata.getScalarFunctionImplementation(resolvedFunction);

        InvocationConvention expectedConvention = invocationConvention.orElseGet(() -> getDefaultCallingConvention(scalarFunctionImplementation));

        for (ScalarImplementationChoice choice : scalarFunctionImplementation.getAllChoices()) {
            InvocationConvention callingConvention = toCallingConvention(choice);
            if (functionAdapter.canAdapt(callingConvention, expectedConvention)) {
                List<Type> actualTypes = resolvedFunction.getSignature().getArgumentTypes().stream()
                        .map(metadata::getType)
                        .collect(toImmutableList());
                MethodHandle methodHandle = functionAdapter.adapt(choice.getMethodHandle(), actualTypes, callingConvention, expectedConvention);
                return new FunctionInvoker(methodHandle, choice.getInstanceFactory());
            }
        }
        throw new PrestoException(FUNCTION_NOT_FOUND, format("Dependent function implementation (%s) with convention (%s) is not available", resolvedFunction, invocationConvention.toString()));
    }

    /**
     * Default calling convention is no nulls and null is never returned. Since the no nulls adaptation strategy is to fail, the scalar must have this
     * exact convention or convention must be specified.
     */
    private static InvocationConvention getDefaultCallingConvention(ScalarFunctionImplementation scalarFunctionImplementation)
    {
        List<InvocationArgumentConvention> argumentConventions = scalarFunctionImplementation.getArgumentProperties().stream()
                .map(ArgumentProperty::getArgumentType)
                .map(argumentProperty -> argumentProperty == FUNCTION_TYPE ? FUNCTION : NEVER_NULL)
                .collect(toImmutableList());
        InvocationReturnConvention returnConvention = scalarFunctionImplementation.isNullable() ? NULLABLE_RETURN : FAIL_ON_NULL;

        return new InvocationConvention(argumentConventions, returnConvention, true, false);
    }

    private static InvocationConvention toCallingConvention(ScalarImplementationChoice choice)
    {
        return new InvocationConvention(
                choice.getArgumentProperties().stream()
                        .map(FunctionInvokerProvider::toArgumentConvention)
                        .collect(toImmutableList()),
                choice.isNullable() ? NULLABLE_RETURN : FAIL_ON_NULL,
                choice.hasSession(),
                choice.getInstanceFactory().isPresent());
    }

    private static InvocationArgumentConvention toArgumentConvention(ArgumentProperty argumentProperty)
    {
        if (argumentProperty.getArgumentType() == FUNCTION_TYPE) {
            return FUNCTION;
        }
        switch (argumentProperty.getNullConvention()) {
            case RETURN_NULL_ON_NULL:
                return NEVER_NULL;
            case USE_BOXED_TYPE:
                return BOXED_NULLABLE;
            case USE_NULL_FLAG:
                return NULL_FLAG;
            case BLOCK_AND_POSITION:
                return BLOCK_POSITION;
            default:
                throw new IllegalArgumentException("Unsupported null convention: " + argumentProperty.getNullConvention());
        }
    }
}
