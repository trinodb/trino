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
package io.prestosql.plugin.hive.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.AbstractInvocationHandler;
import io.airlift.log.Logger;
import io.airlift.parameternames.ParameterNames;
import io.airlift.units.Duration;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.joining;

public class LoggingInvocationHandler
        extends AbstractInvocationHandler
{
    private final Object delegate;
    private final ParameterNamesProvider parameterNames;
    private final Consumer<String> logger;

    public LoggingInvocationHandler(Object delegate, ParameterNamesProvider parameterNames, Consumer<String> logger)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.parameterNames = requireNonNull(parameterNames, "parameterNames is null");
        this.logger = requireNonNull(logger, "logger is null");
    }

    @Override
    protected Object handleInvocation(Object proxy, Method method, Object[] args)
            throws Throwable
    {
        Object result;
        long startNanos = System.nanoTime();
        try {
            result = method.invoke(delegate, args);
        }
        catch (InvocationTargetException e) {
            Duration elapsed = Duration.nanosSince(startNanos);
            Throwable t = e.getCause();
            logger.accept(format("%s took %s and failed with %s", invocationDescription(method, args), elapsed, t));
            throw t;
        }
        Duration elapsed = Duration.nanosSince(startNanos);
        logger.accept(format("%s succeeded in %s", invocationDescription(method, args), elapsed));
        return result;
    }

    private String invocationDescription(Method method, Object[] args)
    {
        Optional<List<String>> parameterNames = this.parameterNames.getParameterNames(method);
        return "Invocation of " + method.getName() +
                IntStream.range(0, args.length)
                        .mapToObj(i -> {
                            if (parameterNames.isPresent()) {
                                return format("%s=%s", parameterNames.get().get(i), formatArgument(args[i]));
                            }
                            return formatArgument(args[i]);
                        })
                        .collect(joining(", ", "(", ")"));
    }

    private static String formatArgument(Object arg)
    {
        if (arg instanceof String) {
            return "'" + ((String) arg).replace("'", "''") + "'";
        }
        return String.valueOf(arg);
    }

    public interface ParameterNamesProvider
    {
        Optional<List<String>> getParameterNames(Method method);
    }

    public static class ReflectiveParameterNamesProvider
            implements ParameterNamesProvider
    {
        @Override
        public Optional<List<String>> getParameterNames(Method method)
        {
            Parameter[] parameters = method.getParameters();
            if (Arrays.stream(parameters).noneMatch(Parameter::isNamePresent)) {
                return Optional.empty();
            }
            return Arrays.stream(parameters)
                    .map(Parameter::getName)
                    .collect(collectingAndThen(toImmutableList(), Optional::of));
        }
    }

    public static class AirliftParameterNamesProvider
            implements ParameterNamesProvider
    {
        private static final Logger log = Logger.get(AirliftParameterNamesProvider.class);

        private final Map<Method, List<String>> parameterNames;

        public <I, C extends I> AirliftParameterNamesProvider(Class<I> interfaceClass, Class<C> implementationClass)
        {
            requireNonNull(interfaceClass, "interfaceClass is null");
            requireNonNull(implementationClass, "implementationClass is null");

            ImmutableMap.Builder<Method, List<String>> parameterNames = ImmutableMap.builder();
            for (Method interfaceMethod : interfaceClass.getMethods()) {
                tryGetParameterNamesForMethod(interfaceMethod, implementationClass)
                        .map(ImmutableList::copyOf)
                        .ifPresent(names -> parameterNames.put(interfaceMethod, names));
            }
            this.parameterNames = parameterNames.build();
        }

        private static Optional<List<String>> tryGetParameterNamesForMethod(Method interfaceMethod, Class<?> implementationClass)
        {
            Optional<List<String>> names = ParameterNames.tryGetParameterNames(interfaceMethod);
            if (names.isPresent()) {
                return names;
            }

            Method implementationMethod;
            try {
                implementationMethod = implementationClass.getMethod(interfaceMethod.getName(), interfaceMethod.getParameterTypes());
            }
            catch (NoSuchMethodException e) {
                log.debug(e, "Could not find implementation for %s", interfaceMethod);
                return Optional.empty();
            }
            return ParameterNames.tryGetParameterNames(implementationMethod);
        }

        @Override
        public Optional<List<String>> getParameterNames(Method method)
        {
            return Optional.ofNullable(parameterNames.get(method));
        }
    }
}
