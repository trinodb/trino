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
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class Rollup
        extends GroupingElement
{
    private final List<List<Expression>> sets;

    public Rollup(List<List<Expression>> sets)
    {
        this(Optional.empty(), sets);
    }

    public Rollup(NodeLocation location, List<List<Expression>> sets)
    {
        this(Optional.of(location), sets);
    }

    private Rollup(Optional<NodeLocation> location, List<List<Expression>> sets)
    {
        super(location);
        this.sets = ImmutableList.copyOf(requireNonNull(sets, "sets is null"));
    }

    public List<List<Expression>> getSets()
    {
        return sets;
    }

    @Override
    public List<Expression> getExpressions()
    {
        return sets.stream()
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitRollup(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Rollup rollup = (Rollup) o;
        return Objects.equals(sets, rollup.sets);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(sets);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("sets", sets)
                .toString();
    }
}
