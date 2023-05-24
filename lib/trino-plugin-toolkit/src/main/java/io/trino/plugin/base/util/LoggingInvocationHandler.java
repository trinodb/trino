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
package io.trino.plugin.base.util;

import com.google.common.reflect.AbstractInvocationHandler;
import io.airlift.units.Duration;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Arrays;
import java.util.List;
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
    private final Consumer<String> logger;
    private final boolean includeResult;

    public LoggingInvocationHandler(Object delegate, Consumer<String> logger)
    {
        this(delegate, logger, false);
    }

    public LoggingInvocationHandler(Object delegate, Consumer<String> logger, boolean includeResult)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.logger = requireNonNull(logger, "logger is null");
        this.includeResult = includeResult;
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
        if (includeResult) {
            logger.accept(format("%s succeeded in %s and returned %s", invocationDescription(method, args), elapsed, formatArgument(result)));
        }
        else {
            logger.accept(format("%s succeeded in %s", invocationDescription(method, args), elapsed));
        }
        return result;
    }

    private static String invocationDescription(Method method, Object[] args)
    {
        Optional<List<String>> parameterNames = getParameterNames(method);
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

    private static Optional<List<String>> getParameterNames(Method method)
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
