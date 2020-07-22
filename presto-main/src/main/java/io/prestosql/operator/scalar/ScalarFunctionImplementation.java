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

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentType.FUNCTION_TYPE;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentType.VALUE_TYPE;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.FUNCTION;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.NULL_FLAG;
import static io.prestosql.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.prestosql.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class ScalarFunctionImplementation
{
    private final List<ScalarImplementationChoice> choices;

    public ScalarFunctionImplementation(
            boolean nullable,
            List<ArgumentProperty> argumentProperties,
            MethodHandle methodHandle)
    {
        this(
                nullable,
                argumentProperties,
                methodHandle,
                Optional.empty());
    }

    public ScalarFunctionImplementation(
            boolean nullable,
            List<ArgumentProperty> argumentProperties,
            MethodHandle methodHandle,
            Optional<MethodHandle> instanceFactory)
    {
        this(ImmutableList.of(new ScalarImplementationChoice(nullable, argumentProperties, methodHandle, instanceFactory)));
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
                boolean nullable,
                List<ArgumentProperty> argumentProperties,
                MethodHandle methodHandle,
                Optional<MethodHandle> instanceFactory)
        {
            this.methodHandle = requireNonNull(methodHandle, "methodHandle is null");
            this.instanceFactory = requireNonNull(instanceFactory, "instanceFactory is null");

            if (instanceFactory.isPresent()) {
                Class<?> instanceType = instanceFactory.get().type().returnType();
                checkArgument(instanceFactory.get().type().parameterList().size() == 0, "instanceFactory should have no parameter");
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

            lambdaInterfaces = argumentProperties.stream()
                    .map(ArgumentProperty::getLambdaInterface)
                    .collect(toImmutableList());
            invocationConvention = new InvocationConvention(
                    argumentProperties.stream()
                            .map(ScalarImplementationChoice::toArgumentConvention)
                            .collect(toImmutableList()),
                    nullable ? NULLABLE_RETURN : FAIL_ON_NULL,
                    hasSession,
                    instanceFactory.isPresent());
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

        public MethodHandle getMethodHandle()
        {
            return methodHandle;
        }

        public Optional<MethodHandle> getInstanceFactory()
        {
            return instanceFactory;
        }

        public ImmutableList<Optional<Class<?>>> getLambdaInterfaces()
        {
            return lambdaInterfaces;
        }

        public InvocationConvention getInvocationConvention()
        {
            return invocationConvention;
        }
    }

    public static class ArgumentProperty
    {
        // TODO: Alternatively, we can store io.prestosql.spi.type.Type
        private final ArgumentType argumentType;
        private final Optional<NullConvention> nullConvention;
        private final Optional<Class<?>> lambdaInterface;

        public static ArgumentProperty valueTypeArgumentProperty(NullConvention nullConvention)
        {
            return new ArgumentProperty(VALUE_TYPE, Optional.of(nullConvention), Optional.empty());
        }

        public static ArgumentProperty functionTypeArgumentProperty(Class<?> lambdaInterface)
        {
            return new ArgumentProperty(FUNCTION_TYPE, Optional.empty(), Optional.of(lambdaInterface));
        }

        public ArgumentProperty(ArgumentType argumentType, Optional<NullConvention> nullConvention, Optional<Class<?>> lambdaInterface)
        {
            switch (argumentType) {
                case VALUE_TYPE:
                    checkArgument(nullConvention.isPresent(), "nullConvention must present for value type");
                    checkArgument(lambdaInterface.isEmpty(), "lambdaInterface must not present for value type");
                    break;
                case FUNCTION_TYPE:
                    checkArgument(nullConvention.isEmpty(), "nullConvention must not present for function type");
                    checkArgument(lambdaInterface.isPresent(), "lambdaInterface must present for function type");
                    checkArgument(lambdaInterface.get().isAnnotationPresent(FunctionalInterface.class), "lambdaInterface must be annotated with FunctionalInterface");
                    break;
                default:
                    throw new UnsupportedOperationException(format("Unsupported argument type: %s", argumentType));
            }

            this.argumentType = argumentType;
            this.nullConvention = nullConvention;
            this.lambdaInterface = lambdaInterface;
        }

        public ArgumentType getArgumentType()
        {
            return argumentType;
        }

        public NullConvention getNullConvention()
        {
            checkState(getArgumentType() == VALUE_TYPE, "nullConvention only applies to value type argument");
            return nullConvention.get();
        }

        public Optional<Class<?>> getLambdaInterface()
        {
            return lambdaInterface;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }

            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            ArgumentProperty other = (ArgumentProperty) obj;
            return this.argumentType == other.argumentType &&
                    this.nullConvention.equals(other.nullConvention) &&
                    this.lambdaInterface.equals(other.lambdaInterface);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(nullConvention, lambdaInterface);
        }
    }

    public enum NullConvention
    {
        RETURN_NULL_ON_NULL(1),
        USE_BOXED_TYPE(1),
        USE_NULL_FLAG(2),
        BLOCK_AND_POSITION(2),
        /**/;

        private final int parameterCount;

        NullConvention(int parameterCount)
        {
            this.parameterCount = parameterCount;
        }

        public int getParameterCount()
        {
            return parameterCount;
        }
    }

    public enum ArgumentType
    {
        VALUE_TYPE,
        FUNCTION_TYPE
    }
}
