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
package io.trino.spi.function;

import com.google.errorprone.annotations.CanIgnoreReturnValue;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ScalarFunctionImplementation
{
    private final MethodHandle methodHandle;
    private final Optional<MethodHandle> instanceFactory;
    private final List<Class<?>> lambdaInterfaces;

    private ScalarFunctionImplementation(MethodHandle methodHandle, Optional<MethodHandle> instanceFactory, List<Class<?>> lambdaInterfaces)
    {
        this.methodHandle = requireNonNull(methodHandle, "methodHandle is null");
        this.instanceFactory = requireNonNull(instanceFactory, "instanceFactory is null");
        this.lambdaInterfaces = List.copyOf(requireNonNull(lambdaInterfaces, "lambdaInterfaces is null"));
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

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private MethodHandle methodHandle;
        private Optional<MethodHandle> instanceFactory = Optional.empty();
        private List<Class<?>> lambdaInterfaces = List.of();

        private Builder() {}

        @CanIgnoreReturnValue
        public Builder methodHandle(MethodHandle methodHandle)
        {
            this.methodHandle = requireNonNull(methodHandle, "methodHandle is null");
            return this;
        }

        @CanIgnoreReturnValue
        public Builder instanceFactory(MethodHandle instanceFactory)
        {
            this.instanceFactory = Optional.of(requireNonNull(instanceFactory, "instanceFactory is null"));
            return this;
        }

        @CanIgnoreReturnValue
        public Builder instanceFactory(Optional<MethodHandle> instanceFactory)
        {
            this.instanceFactory = requireNonNull(instanceFactory, "instanceFactory is null");
            return this;
        }

        @CanIgnoreReturnValue
        public Builder lambdaInterfaces(List<Class<?>> lambdaInterfaces)
        {
            this.lambdaInterfaces = List.copyOf(requireNonNull(lambdaInterfaces, "lambdaInterfaces is null"));
            return this;
        }

        public ScalarFunctionImplementation build()
        {
            return new ScalarFunctionImplementation(methodHandle, instanceFactory, lambdaInterfaces);
        }
    }
}
