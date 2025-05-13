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
package io.trino.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public final class MapLiteral
        extends Expression
{
    private final List<EntryLiteral> entries;

    public MapLiteral(NodeLocation location, List<EntryLiteral> entries)
    {
        super(location);
        this.entries = ImmutableList.copyOf(requireNonNull(entries, "entries is null"));
    }

    public List<EntryLiteral> getEntries()
    {
        return entries;
    }

    public List<Expression> getKeys()
    {
        return entries.stream()
                .map(EntryLiteral::key)
                .collect(toImmutableList());
    }

    public List<Expression> getValues()
    {
        return entries.stream()
                .map(EntryLiteral::value)
                .collect(toImmutableList());
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitMapLiteral(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return entries.stream()
                .flatMap(entry -> Stream.of(entry.key(), entry.value()))
                .collect(toImmutableList());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        return (o != null) && (getClass() == o.getClass());
    }

    @Override
    public int hashCode()
    {
        return MapLiteral.class.hashCode();
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        return sameClass(this, other);
    }

    public record EntryLiteral(Expression key, Expression value)
    {
        public EntryLiteral
        {
            requireNonNull(key, "key is null");
            requireNonNull(value, "value is null");
        }

        @Override
        public String toString()
        {
            return key + " => " + value;
        }
    }
}
