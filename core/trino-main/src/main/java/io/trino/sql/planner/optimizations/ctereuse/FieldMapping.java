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
package io.trino.sql.planner.optimizations.ctereuse;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.type.Type;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableBiMap.toImmutableBiMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.sql.dialect.trino.TypeConstraint.IS_RELATION_ROW;
import static java.util.Objects.requireNonNull;

public record FieldMapping(BiMap<Integer, Integer> fieldIndexMapping)
{
    public static final FieldMapping EMPTY = new FieldMapping(ImmutableBiMap.of());

    public FieldMapping
    {
        requireNonNull(fieldIndexMapping, "fieldIndexMapping is null");
        fieldIndexMapping = ImmutableBiMap.copyOf(fieldIndexMapping);
    }

    public FieldMapping(Map<Integer, Integer> fieldIndexMapping)
    {
        this(ImmutableBiMap.copyOf(fieldIndexMapping));
    }

    public static FieldMapping identity(Type type)
    {
        checkArgument(IS_RELATION_ROW.test(type), "expected relation row type");

        return new FieldMapping(IntStream.range(0, type.getTypeParameters().size())
                .boxed()
                .collect(toImmutableBiMap(Function.identity(), Function.identity())));
    }

    public boolean isIdentity(Type type)
    {
        return identity(type).equals(this);
    }

    public boolean isReordering(Type type)
    {
        checkArgument(IS_RELATION_ROW.test(type), "expected relation row type");

        Set<Integer> indexes = IntStream.range(0, type.getTypeParameters().size())
                .boxed()
                .collect(toImmutableSet());

        return indexes.equals(fieldIndexMapping.keySet()) && indexes.equals(fieldIndexMapping.values());
    }

    public boolean isEmpty()
    {
        return EMPTY.equals(this);
    }

    public Integer get(int index)
    {
        return fieldIndexMapping.get(index);
    }

    public boolean containsKey(int index)
    {
        return fieldIndexMapping.containsKey(index);
    }

    public Set<Integer> keySet()
    {
        return fieldIndexMapping.keySet();
    }

    public FieldMapping inverse()
    {
        return new FieldMapping(fieldIndexMapping.inverse());
    }

    public FieldMapping composeWith(FieldMapping other)
    {
        ImmutableMap.Builder<Integer, Integer> composedIndexMapping = ImmutableMap.builder();

        fieldIndexMapping.entrySet().stream()
                .filter(entry -> other.containsKey(entry.getValue()))
                .forEach(entry -> composedIndexMapping.put(entry.getKey(), other.get(entry.getValue())));

        return new FieldMapping(composedIndexMapping.buildOrThrow());
    }
}
