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
package io.trino.plugin.jdbc.expression;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class MatchContext
{
    private final Map<String, Object> resolved = new HashMap<>();

    public void record(String name, Object value)
    {
        requireNonNull(name, "name is null");
        requireNonNull(value, "value is null");
        resolved.merge(name, value, checkEqual(name));
    }

    public Object get(String name)
    {
        Object value = resolved.get(name);
        if (value == null) {
            throw new IllegalStateException("No value recorded for: " + name);
        }
        return value;
    }

    public Optional<Object> getIfPresent(String name)
    {
        return Optional.ofNullable(resolved.get(name));
    }

    @VisibleForTesting
    Set<String> keys()
    {
        return ImmutableSet.copyOf(resolved.keySet());
    }

    private static BiFunction<Object, Object, Object> checkEqual(String name)
    {
        return (first, second) -> {
            if (first.equals(second)) {
                return first;
            }
            throw new IllegalStateException(format("%s is already mapped to %s, cannot remap to %s", name, first, second));
        };
    }
}
