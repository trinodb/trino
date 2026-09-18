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

/// SQL spec `<character overlay function>`:
/// `OVERLAY(value PLACING replacement FROM start [FOR length])`. Replaces the substring of
/// `value` of `length` characters starting at the 1-based position `start` with `replacement`.
/// When `FOR length` is omitted, the number of replaced characters defaults to the length of
/// `replacement`. Kept as a dedicated node and lowered to the internal `$overlay` function during
/// planning, mirroring [Trim].
public final class Overlay
        extends Expression
{
    private final Expression value;
    private final Expression replacement;
    private final Expression start;
    private final Optional<Expression> length;

    public Overlay(NodeLocation location, Expression value, Expression replacement, Expression start, Optional<Expression> length)
    {
        super(location);
        this.value = requireNonNull(value, "value is null");
        this.replacement = requireNonNull(replacement, "replacement is null");
        this.start = requireNonNull(start, "start is null");
        this.length = requireNonNull(length, "length is null");
    }

    public Expression getValue()
    {
        return value;
    }

    public Expression getReplacement()
    {
        return replacement;
    }

    public Expression getStart()
    {
        return start;
    }

    public Optional<Expression> getLength()
    {
        return length;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitOverlay(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        nodes.add(value);
        nodes.add(replacement);
        nodes.add(start);
        length.ifPresent(nodes::add);
        return nodes.build();
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

        Overlay that = (Overlay) o;
        return Objects.equals(value, that.value) &&
                Objects.equals(replacement, that.replacement) &&
                Objects.equals(start, that.start) &&
                Objects.equals(length, that.length);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value, replacement, start, length);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        return sameClass(this, other);
    }
}
