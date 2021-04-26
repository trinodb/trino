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
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PatternConcatenation
        extends RowPattern
{
    private final List<RowPattern> patterns;

    public PatternConcatenation(NodeLocation location, List<RowPattern> patterns)
    {
        this(Optional.of(location), patterns);
    }

    private PatternConcatenation(Optional<NodeLocation> location, List<RowPattern> patterns)
    {
        super(location);
        this.patterns = requireNonNull(patterns, "patterns is null");
        checkArgument(!patterns.isEmpty(), "patterns list is empty");
    }

    public List<RowPattern> getPatterns()
    {
        return patterns;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitPatternConcatenation(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.copyOf(patterns);
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
        PatternConcatenation o = (PatternConcatenation) obj;
        return Objects.equals(patterns, o.patterns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(patterns);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("patterns", patterns)
                .toString();
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        return sameClass(this, other);
    }
}
