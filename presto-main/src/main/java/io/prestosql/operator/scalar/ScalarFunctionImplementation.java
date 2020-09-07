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
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.InvocationConvention;
import io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention;
import io.prestosql.spi.function.InvocationConvention.InvocationReturnConvention;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

public final class ScalarFunctionImplementation
{
    private final List<ScalarImplementationChoice> choices;

    public ScalarFunctionImplementation(
            InvocationReturnConvention returnConvention,
            List<InvocationArgumentConvention> argumentConventions,
            MethodHandle methodHandle)
    {
        this(returnConvention, argumentConventions, nCopies(argumentConventions.size(), Optional.empty()), methodHandle, Optional.empty());
    }

    public ScalarFunctionImplementation(
            InvocationReturnConvention returnConvention,
            List<InvocationArgumentConvention> argumentConventions,
            MethodHandle methodHandle,
            Optional<MethodHandle> instanceFactory)
    {
        this(returnConvention, argumentConventions, nCopies(argumentConventions.size(), Optional.empty()), methodHandle, instanceFactory);
    }

    public ScalarFunctionImplementation(
            InvocationReturnConvention returnConvention,
            List<InvocationArgumentConvention> argumentConventions,
            List<Optional<Class<?>>> lambdaInterfaces,
            MethodHandle methodHandle,
            Optional<MethodHandle> instanceFactory)
    {
        this(ImmutableList.of(new ScalarImplementationChoice(returnConvention, argumentConventions, lambdaInterfaces, methodHandle, instanceFactory)));
    }

    /**
     * Creates a ScalarFunctionImplementation consisting of one or more choices.
     * <p>
     * All choices must have the same SQL signature, and are equivalent in what they do.
     * The first choice is the default choice, which is the one used for legacy access methods.
     * The default choice must be usable under any context. (e.g. it must not use BLOCK_POSITION convention.)
     *
     * @param choices the list of choices, ordered from generic to specific
     */
    public ScalarFunctionImplementation(List<ScalarImplementationChoice> choices)
    {
        checkArgument(!choices.isEmpty(), "choices is an empty list");
        this.choices = ImmutableList.copyOf(choices);
    }

    public List<ScalarImplementationChoice> getChoices()
    {
        return choices;
    }

    public static class ScalarImplementationChoice
    {
        private final MethodHandle methodHandle;
        private final Optional<MethodHandle> instanceFactory;
        private final InvocationConvention invocationConvention;
        private final List<Optional<Class<?>>> lambdaInterfaces;

        public ScalarImplementationChoice(
                InvocationReturnConvention returnConvention,
                List<InvocationArgumentConvention> argumentConventions,
                List<Optional<Class<?>>> lambdaInterfaces,
                MethodHandle methodHandle,
                Optional<MethodHandle> instanceFactory)
        {
            this.methodHandle = requireNonNull(methodHandle, "methodHandle is null");
            this.instanceFactory = requireNonNull(instanceFactory, "instanceFactory is null");

            if (instanceFactory.isPresent()) {
                Class<?> instanceType = instanceFactory.get().type().returnType();
                checkArgument(instanceFactory.get().type().parameterList().isEmpty(), "instanceFactory should have no parameter");
                checkArgument(instanceType.equals(methodHandle.type().parameterType(0)), "methodHandle is not an instance method");
            }

            List<Class<?>> parameterList = methodHandle.type().parameterList();
            boolean hasSession = false;
            if (parameterList.contains(ConnectorSession.class)) {
                checkArgument(parameterList.stream().filter(ConnectorSession.class::equals).count() == 1, "function implementation should have exactly one ConnectorSession parameter");
                if (instanceFactory.isEmpty()) {
                    checkArgument(parameterList.get(0) == ConnectorSession.class, "ConnectorSession must be the first argument when instanceFactory is not present");
                }
                else {
                    checkArgument(parameterList.get(1) == ConnectorSession.class, "ConnectorSession must be the second argument when instanceFactory is present");
                }
                hasSession = true;
            }

            this.lambdaInterfaces = ImmutableList.copyOf(requireNonNull(lambdaInterfaces, "lambdaInterfaces is null"));
            invocationConvention = new InvocationConvention(
                    argumentConventions,
                    returnConvention,
                    hasSession,
                    instanceFactory.isPresent());
            checkArgument(lambdaInterfaces.size() == argumentConventions.size());
        }

        public MethodHandle getMethodHandle()
        {
            return methodHandle;
        }

        public Optional<MethodHandle> getInstanceFactory()
        {
            return instanceFactory;
        }

        public List<Optional<Class<?>>> getLambdaInterfaces()
        {
            return lambdaInterfaces;
        }

        public InvocationConvention getInvocationConvention()
        {
            return invocationConvention;
        }
    }
}
