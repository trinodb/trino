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

/// A data type followed by a `COLLATE` clause, as in `varchar(10) COLLATE unicode_ci`, and an
/// optional pad attribute, as in `varchar(10) COLLATE unicode_ci PAD SPACE`.
///
/// The grammar accepts the clause after any type; analysis accepts it only for character
/// string types, as ISO/IEC 9075-2 6.1 allows a collate clause only for those.
public final class CollatedDataType
        extends DataType
{
    private final DataType type;
    private final Identifier collation;
    private final Optional<PadAttribute> padAttribute;

    public CollatedDataType(NodeLocation location, DataType type, Identifier collation, Optional<PadAttribute> padAttribute)
    {
        this(Optional.of(location), type, collation, padAttribute);
    }

    public CollatedDataType(Optional<NodeLocation> location, DataType type, Identifier collation, Optional<PadAttribute> padAttribute)
    {
        super(location);
        this.type = requireNonNull(type, "type is null");
        this.collation = requireNonNull(collation, "collation is null");
        this.padAttribute = requireNonNull(padAttribute, "padAttribute is null");
    }

    public DataType getType()
    {
        return type;
    }

    public Identifier getCollation()
    {
        return collation;
    }

    /// The pad attribute written after the collation name. An explicit `NO PAD` is distinct
    /// syntax from an omitted attribute, though both denote a `NO PAD` collation.
    public Optional<PadAttribute> getPadAttribute()
    {
        return padAttribute;
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
        return type.equals(that.type) && collation.equals(that.collation) && padAttribute.equals(that.padAttribute);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, collation, padAttribute);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        return sameClass(this, other) && padAttribute.equals(((CollatedDataType) other).padAttribute);
    }
}
