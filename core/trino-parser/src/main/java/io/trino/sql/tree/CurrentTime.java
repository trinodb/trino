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

import static java.util.Objects.requireNonNull;

public final class CurrentTime
        extends Expression
{
    private final Optional<Integer> precision;

    public CurrentTime(NodeLocation location, int precision)
    {
        this(location, Optional.of(precision));
    }

    public CurrentTime(NodeLocation location)
    {
        this(location, Optional.empty());
    }

    private CurrentTime(NodeLocation location, Optional<Integer> precision)
    {
        super(Optional.of(location));

        requireNonNull(precision, "precision is null");
        this.precision = precision;
    }

    public Optional<Integer> getPrecision()
    {
        return precision;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCurrentTime(this, context);
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
        if ((o == null) || (getClass() != o.getClass())) {
            return false;
        }
        CurrentTime that = (CurrentTime) o;
        return Objects.equals(precision, that.precision);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(precision);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        CurrentTime otherNode = (CurrentTime) other;
        return Objects.equals(precision, otherNode.precision);
    }
}
