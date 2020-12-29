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
package io.prestosql.sql.tree;

import io.airlift.slice.Slice;

import java.util.Objects;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;

public class StringLiteral
        extends Literal
{
    private final Slice slice;

    public StringLiteral(String value)
    {
        this(Optional.empty(), value);
    }

    public StringLiteral(NodeLocation location, String value)
    {
        this(Optional.of(location), value);
    }

    private StringLiteral(Optional<NodeLocation> location, String value)
    {
        super(location);
        requireNonNull(value, "value is null");
        this.slice = utf8Slice(value);
    }

    public String getValue()
    {
        return slice.toStringUtf8();
    }

    public Slice getSlice()
    {
        return slice;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitStringLiteral(this, context);
    }

    @Override
    public boolean equals(Object that)
    {
        if (this == that) {
            return true;
        }
        if (that == null || getClass() != that.getClass()) {
            return false;
        }

        return Objects.equals(slice, ((StringLiteral) that).slice);
    }

    @Override
    public int hashCode()
    {
        return slice.hashCode();
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        return Objects.equals(slice, ((StringLiteral) other).slice);
    }
}
