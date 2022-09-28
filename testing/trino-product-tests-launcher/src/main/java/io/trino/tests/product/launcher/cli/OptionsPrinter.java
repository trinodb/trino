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
package io.trino.tests.product.launcher.cli;

import com.google.common.base.Joiner;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.lang.reflect.Modifier.isPublic;
import static java.lang.reflect.Modifier.isStatic;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public final class OptionsPrinter
{
    private static final Joiner JOINER = Joiner.on(" \\\n")
            .skipNulls();

    private OptionsPrinter() {}

    public static String format(Object... objects)
    {
        List<String> arguments = stream(objects)
                .map(OptionsPrinter::extractArguments)
                .flatMap(Collection::stream)
                .collect(toList());

        return JOINER.join(arguments);
    }

    private static List<String> extractArguments(Object object)
    {
        return stream(object.getClass().getFields())
                .filter(field -> isPublic(field.getModifiers()) && !isStatic(field.getModifiers()))
                .map(field -> formatFieldValue(field, object))
                .collect(toList());
    }

    private static String formatFieldValue(Field field, Object object)
    {
        try {
            Object value = field.get(object);

            if (field.isAnnotationPresent(Option.class)) {
                return formatOption(value, field.getAnnotation(Option.class));
            }

            if (field.isAnnotationPresent(Parameters.class)) {
                return formatArguments(value);
            }
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        return null;
    }

    private static String formatOption(Object value, Option annotation)
    {
        if (annotation.hidden()) {
            return null;
        }

        if (value instanceof Boolean) {
            if ((boolean) value) {
                return annotation.names()[0].replaceFirst("--no-", "--");
            }
            if (annotation.negatable()) {
                return annotation.names()[0];
            }

            return null;
        }

        if (value instanceof String && ((String) value).isBlank()) {
            return null;
        }

        if (value == null) {
            return null;
        }

        if (value instanceof Optional) {
            if (((Optional<?>) value).isPresent()) {
                return formatOption(((Optional<?>) value).get(), annotation);
            }
            return null;
        }

        if (value instanceof Map) {
            if (((Map<?, ?>) value).isEmpty()) {
                return null;
            }
            return ((Map<?, ?>) value).keySet().stream()
                    .map(key -> String.format("%s %s=%s", annotation.names()[0], key, ((Map<?, ?>) value).get(key)))
                    .collect(joining(" "));
        }

        return String.format("%s %s", annotation.names()[0], value);
    }

    private static String formatArguments(Object value)
    {
        List<String> values = (List<String>) value;

        if (values.size() > 0) {
            return String.format("-- %s", Joiner.on(' ')
                    .skipNulls()
                    .join((List<String>) value));
        }

        return null;
    }
}
