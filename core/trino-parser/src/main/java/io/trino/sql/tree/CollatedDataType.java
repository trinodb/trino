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

/// A data type followed by a `COLLATE` clause, as in `varchar(10) COLLATE unicode_ci`.
///
/// The grammar accepts the clause after any type; analysis accepts it only for character
/// string types, as ISO/IEC 9075-2 6.1 allows a collate clause only for those.
public final class CollatedDataType
        extends DataType
{
    private final DataType type;
    private final Identifier collation;

    public CollatedDataType(NodeLocation location, DataType type, Identifier collation)
    {
        this(Optional.of(location), type, collation);
    }

    public CollatedDataType(Optional<NodeLocation> location, DataType type, Identifier collation)
    {
        super(location);
        this.type = requireNonNull(type, "type is null");
        this.collation = requireNonNull(collation, "collation is null");
    }

    public DataType getType()
    {
        return type;
    }

    public Identifier getCollation()
    {
        return collation;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of(type, collation);
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof CollatedDataType that)) {
            return false;
        }
        return type.equals(that.type) && collation.equals(that.collation);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, collation);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        return sameClass(this, other);
    }
}
