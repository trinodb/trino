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
package io.trino.sql.ir;

import com.google.common.collect.ImmutableList;
import io.trino.sql.tree.SkipTo.Position;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.tree.SkipTo.Position.LAST;
import static java.util.Objects.requireNonNull;

public class SkipTo
        extends Node
{
    private final Position position;
    private final Optional<Identifier> identifier;

    public static SkipTo skipPastLastRow()
    {
        return new SkipTo(Position.PAST_LAST, Optional.empty());
    }

    public static SkipTo skipToNextRow()
    {
        return new SkipTo(Position.NEXT, Optional.empty());
    }

    public static SkipTo skipToFirst(Identifier identifier)
    {
        return new SkipTo(Position.FIRST, Optional.of(identifier));
    }

    public static SkipTo skipToLast(Identifier identifier)
    {
        return new SkipTo(LAST, Optional.of(identifier));
    }

    public SkipTo(Position position, Optional<Identifier> identifier)
    {
        requireNonNull(position, "position is null");
        requireNonNull(identifier, "identifier is null");
        checkArgument(identifier.isPresent() || (position == Position.PAST_LAST || position == Position.NEXT), "missing identifier in SKIP TO " + position.name());
        checkArgument(!identifier.isPresent() || (position == Position.FIRST || position == LAST), "unexpected identifier in SKIP TO " + position.name());
        this.position = position;
        this.identifier = identifier;
    }

    public Position getPosition()
    {
        return position;
    }

    public Optional<Identifier> getIdentifier()
    {
        return identifier;
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitSkipTo(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return identifier.map(ImmutableList::of).orElse(ImmutableList.of());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("position", position)
                .add("identifier", identifier.orElse(null))
                .omitNullValues()
                .toString();
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

        SkipTo that = (SkipTo) o;
        return Objects.equals(position, that.position) &&
                Objects.equals(identifier, that.identifier);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(position, identifier);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        return position == ((SkipTo) other).position;
    }
}
