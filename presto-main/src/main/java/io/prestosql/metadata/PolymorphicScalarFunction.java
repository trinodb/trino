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

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;
import io.prestosql.metadata.PolymorphicScalarFunctionBuilder.MethodAndNativeContainerTypes;
import io.prestosql.metadata.PolymorphicScalarFunctionBuilder.MethodsGroup;
import io.prestosql.metadata.PolymorphicScalarFunctionBuilder.SpecializeContext;
import io.prestosql.operator.scalar.ScalarFunctionImplementation;
import io.prestosql.operator.scalar.ScalarFunctionImplementation.ScalarImplementationChoice;
import io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention;
import io.prestosql.spi.function.InvocationConvention.InvocationReturnConvention;
import io.prestosql.spi.type.Type;
import io.prestosql.util.Reflection;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.emptyList;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

class PolymorphicScalarFunction
        extends SqlScalarFunction
{
    private final List<PolymorphicScalarFunctionChoice> choices;

    PolymorphicScalarFunction(FunctionMetadata functionMetadata, List<PolymorphicScalarFunctionChoice> choices)
    {
        super(functionMetadata);
        this.choices = requireNonNull(choices, "choices is null");
    }

    @Override
    protected ScalarFunctionImplementation specialize(FunctionBinding functionBinding)
    {
        ImmutableList.Builder<ScalarImplementationChoice> implementationChoices = ImmutableList.builder();

        for (PolymorphicScalarFunctionChoice choice : choices) {
            implementationChoices.add(getScalarFunctionImplementationChoice(functionBinding, choice));
        }

        return new ScalarFunctionImplementation(implementationChoices.build());
    }

    private ScalarImplementationChoice getScalarFunctionImplementationChoice(
            FunctionBinding functionBinding,
            PolymorphicScalarFunctionChoice choice)
    {
        List<Type> argumentTypes = functionBinding.getBoundSignature().getArgumentTypes();
        Type returnType = functionBinding.getBoundSignature().getReturnType();
        SpecializeContext context = new SpecializeContext(functionBinding, argumentTypes, returnType);
        Optional<MethodAndNativeContainerTypes> matchingMethod = Optional.empty();

        Optional<MethodsGroup> matchingMethodsGroup = Optional.empty();
        for (MethodsGroup candidateMethodsGroup : choice.getMethodsGroups()) {
            for (MethodAndNativeContainerTypes candidateMethod : candidateMethodsGroup.getMethods()) {
                if (matchesParameterAndReturnTypes(candidateMethod, argumentTypes, returnType, choice.getArgumentConventions(), choice.getReturnConvention())) {
                    if (matchingMethod.isPresent()) {
                        throw new IllegalStateException("two matching methods (" + matchingMethod.get().getMethod().getName() + " and " + candidateMethod.getMethod().getName() + ") for parameter types " + argumentTypes);
                    }

                    matchingMethod = Optional.of(candidateMethod);
                    matchingMethodsGroup = Optional.of(candidateMethodsGroup);
                }
            }
        }
        checkState(matchingMethod.isPresent(), "no matching method for parameter types %s", argumentTypes);

        List<Object> extraParameters = computeExtraParameters(matchingMethodsGroup.get(), context);
        MethodHandle methodHandle = applyExtraParameters(matchingMethod.get().getMethod(), extraParameters, choice.getArgumentConventions());
        return new ScalarImplementationChoice(
                choice.getReturnConvention(),
                choice.getArgumentConventions(),
                nCopies(choice.getArgumentConventions().size(), Optional.empty()),
                methodHandle,
                Optional.empty());
    }

    private static boolean matchesParameterAndReturnTypes(
            MethodAndNativeContainerTypes methodAndNativeContainerTypes,
            List<Type> resolvedTypes,
            Type returnType,
            List<InvocationArgumentConvention> argumentConventions,
            InvocationReturnConvention returnConvention)
    {
        Method method = methodAndNativeContainerTypes.getMethod();
        checkState(method.getParameterCount() >= resolvedTypes.size(),
                "method %s has not enough arguments: %s (should have at least %s)", method.getName(), method.getParameterCount(), resolvedTypes.size());

        Class<?>[] methodParameterJavaTypes = method.getParameterTypes();
        int methodParameterIndex = 0;
        for (int i = 0; i < resolvedTypes.size(); i++) {
            Type resolvedType = resolvedTypes.get(i);
            InvocationArgumentConvention argumentConvention = argumentConventions.get(i);

            Class<?> expectedType = null;
            Class<?> actualType;
            switch (argumentConvention) {
                case NEVER_NULL:
                case NULL_FLAG:
                    expectedType = methodParameterJavaTypes[methodParameterIndex];
                    actualType = resolvedType.getJavaType();
                    break;
                case BOXED_NULLABLE:
                    expectedType = methodParameterJavaTypes[methodParameterIndex];
                    actualType = Primitives.wrap(resolvedType.getJavaType());
                    break;
                case BLOCK_POSITION:
                    Optional<Class<?>> explicitNativeContainerTypes = methodAndNativeContainerTypes.getExplicitNativeContainerTypes().get(i);
                    if (explicitNativeContainerTypes.isPresent()) {
                        expectedType = explicitNativeContainerTypes.get();
                    }
                    actualType = resolvedType.getJavaType();
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown argument convention: " + argumentConvention);
            }
            if (!actualType.equals(expectedType)) {
                return false;
            }
            methodParameterIndex += argumentConvention.getParameterCount();
        }
        return method.getReturnType().equals(getNullAwareContainerType(returnType.getJavaType(), returnConvention));
    }

    private static List<Object> computeExtraParameters(MethodsGroup methodsGroup, SpecializeContext context)
    {
        return methodsGroup.getExtraParametersFunction().map(function -> function.apply(context)).orElse(emptyList());
    }

    private MethodHandle applyExtraParameters(Method matchingMethod, List<Object> extraParameters, List<InvocationArgumentConvention> argumentConventions)
    {
        int expectedArgumentsCount = extraParameters.size() + argumentConventions.stream()
                .mapToInt(InvocationArgumentConvention::getParameterCount)
                .sum();
        int matchingMethodArgumentCount = matchingMethod.getParameterCount();
        checkState(matchingMethodArgumentCount == expectedArgumentsCount,
                "method %s has invalid number of arguments: %s (should have %s)", matchingMethod.getName(), matchingMethodArgumentCount, expectedArgumentsCount);

        MethodHandle matchingMethodHandle = Reflection.methodHandle(matchingMethod);
        matchingMethodHandle = MethodHandles.insertArguments(
                matchingMethodHandle,
                matchingMethodArgumentCount - extraParameters.size(),
                extraParameters.toArray());
        return matchingMethodHandle;
    }

    private static Class<?> getNullAwareContainerType(Class<?> clazz, InvocationReturnConvention returnConvention)
    {
        switch (returnConvention) {
            case NULLABLE_RETURN:
                return Primitives.wrap(clazz);
            case FAIL_ON_NULL:
                return clazz;
            default:
                throw new UnsupportedOperationException("Unknown return convention: " + returnConvention);
        }
    }

    static final class PolymorphicScalarFunctionChoice
    {
        private final InvocationReturnConvention returnConvention;
        private final List<InvocationArgumentConvention> argumentConventions;
        private final List<MethodsGroup> methodsGroups;

        PolymorphicScalarFunctionChoice(
                InvocationReturnConvention returnConvention,
                List<InvocationArgumentConvention> argumentConventions,
                List<MethodsGroup> methodsGroups)
        {
            this.returnConvention = requireNonNull(returnConvention, "returnConvention is null");
            this.argumentConventions = ImmutableList.copyOf(requireNonNull(argumentConventions, "argumentConventions is null"));
            this.methodsGroups = ImmutableList.copyOf(requireNonNull(methodsGroups, "methodsGroups is null"));
        }

        InvocationReturnConvention getReturnConvention()
        {
            return returnConvention;
        }

        List<MethodsGroup> getMethodsGroups()
        {
            return methodsGroups;
        }

        List<InvocationArgumentConvention> getArgumentConventions()
        {
            return argumentConventions;
        }
    }
}
