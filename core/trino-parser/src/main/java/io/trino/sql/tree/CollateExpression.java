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

/// `value COLLATE collation`, which gives a character string expression an explicit collation.
public final class CollateExpression
        extends Expression
{
    private final Expression value;
    private final Identifier collation;

    public CollateExpression(NodeLocation location, Expression value, Identifier collation)
    {
        this(Optional.of(location), value, collation);
    }

    public CollateExpression(Optional<NodeLocation> location, Expression value, Identifier collation)
    {
        super(location);
        this.value = requireNonNull(value, "value is null");
        this.collation = requireNonNull(collation, "collation is null");
    }

    public Expression getValue()
    {
        return value;
    }

    public Identifier getCollation()
    {
        return collation;
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(value, collation);
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
        CollateExpression that = (CollateExpression) o;
        return value.equals(that.value) && collation.equals(that.collation);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value, collation);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        return sameClass(this, other);
    }
}
