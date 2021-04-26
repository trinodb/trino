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

public class ExcludedPattern
        extends RowPattern
{
    private final RowPattern pattern;

    public ExcludedPattern(NodeLocation location, RowPattern pattern)
    {
        this(Optional.of(location), pattern);
    }

    private ExcludedPattern(Optional<NodeLocation> location, RowPattern pattern)
    {
        super(location);
        this.pattern = requireNonNull(pattern, "pattern is null");
    }

    public RowPattern getPattern()
    {
        return pattern;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitExcludedPattern(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(pattern);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        ExcludedPattern o = (ExcludedPattern) obj;
        return Objects.equals(pattern, o.pattern);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(pattern);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("pattern", pattern)
                .toString();
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        return sameClass(this, other);
    }
}
