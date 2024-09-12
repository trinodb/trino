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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class ShowFunctions
        extends Statement
{
    private final Optional<QualifiedName> schema;
    private final Optional<String> likePattern;
    private final Optional<String> escape;

    public ShowFunctions(NodeLocation location, Optional<QualifiedName> schema, Optional<String> likePattern, Optional<String> escape)
    {
        super(Optional.of(location));
        this.schema = requireNonNull(schema, "schema is null");
        this.likePattern = requireNonNull(likePattern, "likePattern is null");
        this.escape = requireNonNull(escape, "escape is null");
    }

    public Optional<QualifiedName> getSchema()
    {
        return schema;
    }

    public Optional<String> getLikePattern()
    {
        return likePattern;
    }

    public Optional<String> getEscape()
    {
        return escape;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitShowFunctions(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schema, likePattern, escape);
    }

    @Override
    public boolean equals(Object obj)
    {
        return (obj instanceof ShowFunctions other) &&
                Objects.equals(schema, other.schema) &&
                Objects.equals(likePattern, other.likePattern) &&
                Objects.equals(escape, other.escape);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schema", schema.orElse(null))
                .add("likePattern", likePattern.orElse(null))
                .add("escape", escape.orElse(null))
                .omitNullValues()
                .toString();
    }
}
