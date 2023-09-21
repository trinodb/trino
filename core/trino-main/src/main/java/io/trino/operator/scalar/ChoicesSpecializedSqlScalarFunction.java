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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.InvocationConvention.InvocationArgumentConvention;
import io.trino.spi.function.InvocationConvention.InvocationReturnConvention;
import io.trino.spi.function.ScalarFunctionAdapter;
import io.trino.spi.function.ScalarFunctionImplementation;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static java.lang.String.format;
import static java.util.Comparator.comparingInt;
import static java.util.Objects.requireNonNull;

public final class ChoicesSpecializedSqlScalarFunction
        implements SpecializedSqlScalarFunction
{
    private final BoundSignature boundSignature;
    private final List<ScalarImplementationChoice> choices;

    public ChoicesSpecializedSqlScalarFunction(
            BoundSignature boundSignature,
            InvocationReturnConvention returnConvention,
            List<InvocationArgumentConvention> argumentConventions,
            MethodHandle methodHandle)
    {
        this(boundSignature, returnConvention, argumentConventions, ImmutableList.of(), methodHandle, Optional.empty());
    }

    public ChoicesSpecializedSqlScalarFunction(
            BoundSignature boundSignature,
            InvocationReturnConvention returnConvention,
            List<InvocationArgumentConvention> argumentConventions,
            MethodHandle methodHandle,
            Optional<MethodHandle> instanceFactory)
    {
        this(boundSignature, returnConvention, argumentConventions, ImmutableList.of(), methodHandle, instanceFactory);
    }

    public ChoicesSpecializedSqlScalarFunction(
            BoundSignature boundSignature,
            InvocationReturnConvention returnConvention,
            List<InvocationArgumentConvention> argumentConventions,
            List<Class<?>> lambdaInterfaces,
            MethodHandle methodHandle,
            Optional<MethodHandle> instanceFactory)
    {
        this(boundSignature, ImmutableList.of(new ScalarImplementationChoice(returnConvention, argumentConventions, lambdaInterfaces, methodHandle, instanceFactory)));
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
    public ChoicesSpecializedSqlScalarFunction(BoundSignature boundSignature, List<ScalarImplementationChoice> choices)
    {
        this.boundSignature = boundSignature;
        checkArgument(!choices.isEmpty(), "choices is an empty list");
        this.choices = ImmutableList.copyOf(choices);
    }

    @VisibleForTesting
    public List<ScalarImplementationChoice> getChoices()
    {
        return choices;
    }

    @Override
    public ScalarFunctionImplementation getScalarFunctionImplementation(InvocationConvention invocationConvention)
    {
        List<ScalarImplementationChoice> choices = new ArrayList<>();
        for (ScalarImplementationChoice choice : this.choices) {
            InvocationConvention callingConvention = choice.getInvocationConvention();
            if (ScalarFunctionAdapter.canAdapt(callingConvention, invocationConvention)) {
                choices.add(choice);
            }
        }
        if (choices.isEmpty()) {
            throw new TrinoException(FUNCTION_NOT_FOUND,
                    format("Function implementation for (%s) cannot be adapted to convention (%s)", boundSignature, invocationConvention));
        }

        ScalarImplementationChoice bestChoice = Collections.max(choices, comparingInt(ScalarImplementationChoice::getScore));
        MethodHandle methodHandle = ScalarFunctionAdapter.adapt(
                bestChoice.getMethodHandle(),
                boundSignature.getReturnType(),
                boundSignature.getArgumentTypes(),
                bestChoice.getInvocationConvention(),
                invocationConvention);
        ScalarFunctionImplementation.Builder builder = ScalarFunctionImplementation.builder()
                .methodHandle(methodHandle);
        bestChoice.getInstanceFactory().ifPresent(builder::instanceFactory);
        builder.lambdaInterfaces(bestChoice.getLambdaInterfaces());
        return builder.build();
    }

    public static class ScalarImplementationChoice
    {
        private final MethodHandle methodHandle;
        private final Optional<MethodHandle> instanceFactory;
        private final InvocationConvention invocationConvention;
        private final List<Class<?>> lambdaInterfaces;
        private final int score;

        public ScalarImplementationChoice(
                InvocationReturnConvention returnConvention,
                List<InvocationArgumentConvention> argumentConventions,
                List<Class<?>> lambdaInterfaces,
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
            checkArgument(lambdaInterfaces.size() <= argumentConventions.size());

            score = computeScore(invocationConvention);
        }

        public MethodHandle getMethodHandle()
        {
            return methodHandle;
        }

        public Optional<MethodHandle> getInstanceFactory()
        {
            return instanceFactory;
        }

        public List<Class<?>> getLambdaInterfaces()
        {
            return lambdaInterfaces;
        }

        public InvocationConvention getInvocationConvention()
        {
            return invocationConvention;
        }

        public int getScore()
        {
            return score;
        }

        private static int computeScore(InvocationConvention callingConvention)
        {
            int score = 0;
            for (InvocationArgumentConvention argument : callingConvention.getArgumentConventions()) {
                switch (argument) {
                    case NULL_FLAG:
                        score += 1;
                        break;
                    case BLOCK_POSITION_NOT_NULL:
                    case BLOCK_POSITION:
                        score += 1000;
                        break;
                    case IN_OUT:
                        score += 10_000;
                        break;
                    default:
                        break;
                }
            }
            return score;
        }
    }
}
